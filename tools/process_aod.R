

bdir <- if (Sys.info()[["user"]] == "songlab") {
  "/mnt/hdd001/huimori/aerosol"
} else {
  if (Sys.info()[["user"]] == "isong") {
    "/media/isong/Korea/aerosol"
  } else {
    stop("Unknown user. Please set the base directory for data.")
  }
}

library(amadeus)
library(terra)
library(mirai)

# list hdf
aod_files <- list.files(
  bdir,
  pattern = ".hdf$",
  full.names = TRUE
)



modis_pattern <- "A20[0-9]{5,5}"
aod_dates <- stringi::stri_extract_first_regex(
  basename(aod_files), pattern = modis_pattern
)
aod_dates <- as.Date(aod_dates, format = "A%Y%j")
aod_dates <- unique(aod_dates)

# # process and export
# amadeus::process_modis_merge(
#   path = aod_files,
#   date = "2022-01-01",
#   subdataset = "^(Optical_Depth|AOD)",
#   fun_agg = "mean"
# )

library(stringr)

qa = rast(aod_files[1])

get_bits <- function(r, start, nbits = 1, filename = "", overwrite = FALSE) {
  mask <- bitwShiftL(2^nbits - 1L, start)
  app(
    r,
    fun = function(x) {
      x <- as.integer(x)
      out <- bitwShiftR(bitwAnd(x, mask), start)
      out[is.na(x)] <- NA_integer_
      out
    },
    filename = filename,
    overwrite = overwrite
  )
}


cloud_mask    <- get_bits(qa["AOD_QA"], start = 0, nbits = 3)   # bits 0-2
land_water    <- get_bits(qa, start = 3, nbits = 2)   # bits 3-4
adjacency     <- get_bits(qa, start = 5, nbits = 3)   # bits 5-7
aod_quality   <- get_bits(qa, start = 8, nbits = 4)   # bits 8-11
glint_mask    <- get_bits(qa, start = 12, nbits = 1)  # bit 12
aerosol_model <- get_bits(qa, start = 13, nbits = 2)  # bits 13-14

process_mcd19a2_day <- function(target_date, input_dir, output_dir, export = FALSE) {
    # 1. Locate the files for this specific date
    # Assuming your files have a naming convention like "MCD19A2_AOD_047_YYYYDDD.tif"

    aod_raw <-
        amadeus::process_modis_merge(
            path = aod_files,
            date = date,
            subdataset = "^(Optical_Depth)",
            fun_agg = "mean"
        )
    qa_c <-
        amadeus::process_modis_merge(
            path = aod_files,
            date = "2022-12-31",
            subdataset = "AOD_QC",
            fun_agg = "sum"
        )
    

    aod_file <- list.files(input_dir, pattern = paste0("Optical_Depth_047_", target_date, "\\.tif$"), full.names = TRUE)
    qa_file <- list.files(input_dir, pattern = paste0("AOD_QC_", target_date, "\\.tif$"), full.names = TRUE)
    unc_file <- list.files(input_dir, pattern = paste0("AOD_Uncertainty_", target_date, "\\.tif$"), full.names = TRUE)

    # Skip if missing any files for this date
    if (length(aod_file) == 0 || length(qa_file) == 0 || length(unc_file) == 0) {
        return(NA_character_)
    }

    # 2. Read and scale data inside the worker
    aod <- rast(aod_file) * 0.001
    qa_raw <- rast(qa_file)
    unc <- rast(unc_file) * 0.001

    # 3. Apply Bitwise Masks
    get_cloud_mask <- function(x) bitwAnd(x, 7) == 1
    get_aod_qa <- function(x) bitwAnd(bitwShiftR(x, 8), 15) == 0

    is_clear <- app(qa_raw, fun = get_cloud_mask)
    is_best_qa <- app(qa_raw, fun = get_aod_qa)
    valid_pixels_mask <- is_clear & is_best_qa

    aod_masked <- mask(aod, valid_pixels_mask, maskvalue = FALSE)
    unc_masked <- mask(unc, valid_pixels_mask, maskvalue = FALSE)

    # 4. Inverse-Variance Weighting
    weights <- 1 / (unc_masked^2 + 1e-6)
    weighted_aod <- aod_masked * weights

    sum_weighted_aod <- sum(weighted_aod, na.rm = TRUE)
    sum_weights <- sum(weights, na.rm = TRUE)

    daily_composite <- sum_weighted_aod / sum_weights
    daily_composite <- ifel(sum_weights == 0, NA, daily_composite)

    # 5. Write to disk immediately
    if (export) {
        out_path <- file.path(output_dir, paste0("MCD19A2_Daily_Composite_", target_date, ".tif"))

        writeRaster(daily_composite, out_path, overwrite = TRUE, gdal = c("COMPRESS=DEFLATE"))
        message("Processed and saved: ", out_path)
        # Return the path as a string
        return(out_path)
    } else {
        daily_composite
    }
}

best_mask <- cloud_mask == 1 & adjacency == 0 & aod_quality == 0

aod047_best <- mask(aod047, best_mask, maskvalues = FALSE)
aod055_best <- mask(aod055, best_mask, maskvalues = FALSE)

# apply scale factor 0.001
aod047_best <- aod047_best * 0.001
aod055_best <- aod055_best * 0.001
usable_mask <- cloud_mask %in% c(1, 2) &  # clear or possibly cloudy
               adjacency %in% c(0, 3) &   # clear or one cloudy neighbor
               aod_quality %in% c(0, 11)  # best or research quality

check <-
process_mcd19a2_day(
    target_date = "2022-01-01",
    input_dir = file.path(bdir),
    output_dir = file.path(bdir, "processed"),
    export = FALSE
)

# 
mirai::daemons(12L)
mirai::mirai_map(
    .x = aod_dates,
    .f = function(date) {
        prcd <-
            amadeus::process_modis_merge(
                path = aod_files,
                date = date,
                subdataset = "^(Optical_Depth|AOD_Uncertain)",
                fun_agg = "mean"
            )
        targ_file <- file.path(
            bdir, "aod", "processed",
            sprintf("aod_%s.tif", format(date, "%Y%m%d"))
        )
        terra::writeRaster(prcd, targ_file)
        out_file
    }
)


