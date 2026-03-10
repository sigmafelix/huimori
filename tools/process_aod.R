

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

process_mcd19a2_day <- function(target_date, input_dir, output_dir, export = FALSE) {
    # 1. Locate the files for this specific date
    # Assuming your files have a naming convention like "MCD19A2_AOD_047_YYYYDDD.tif"

    # Read only the 4-layer subdatasets you actually need
    aod_raw <- rast(hdf_file, subds = "Optical_Depth_047")
    unc_raw <- rast(hdf_file, subds = "AOD_Uncertainty")
    qa_raw  <- rast(hdf_file, subds = "AOD_QA")

    # 2. Read and scale data inside the worker
    aod <- rast(aod_raw) * 0.001
    qa_raw <- rast(qa_raw)
    unc <- rast(unc_raw) * 0.001

    # 3. Apply Bitwise Masks
    get_cloud_mask <- function(x) bitwAnd(x, 7) == 1
    get_aod_qa <- function(x) bitwAnd(bitwShiftR(x, 8), 15) == 0

    is_clear <- app(qa_raw, fun = get_cloud_mask)
    is_best_qa <- app(qa_raw, fun = get_aod_qa)
    valid_pixels_mask <- is_clear & is_best_qa

    aod_masked <- mask(aod, valid_pixels_mask, maskvalue = FALSE)
    unc_masked <- mask(unc, valid_pixels_mask, maskvalue = FALSE)

    # 4. Apply the QA masking across all 4 overpasses
    is_clear <- app(qa_raw, fun = get_cloud_mask)
    is_best_qa <- app(qa_raw, fun = get_aod_qa)

    valid_pixels_mask <- is_clear & is_best_qa

    aod_masked <- mask(aod, valid_pixels_mask, maskvalue = FALSE)
    unc_masked <- mask(unc, valid_pixels_mask, maskvalue = FALSE)

    # 5. Inverse-Variance Weighting & Daily Composite
    weights <- 1 / (unc_masked^2 + 1e-6)
    weighted_aod <- aod_masked * weights

    sum_weighted_aod <- sum(weighted_aod, na.rm = TRUE)
    sum_weights <- sum(weights, na.rm = TRUE)

    daily_aod_composite <- sum_weighted_aod / sum_weights

    # Clean up areas with zero valid overpasses
    daily_aod_composite <- ifel(sum_weights == 0, NA, daily_aod_composite)

    # 5. Write to disk immediately
    if (export) {
        out_path <- file.path(output_dir, paste0("MCD19A2_Daily_Composite_", target_date, ".tif"))

        writeRaster(daily_aod_composite, out_path, overwrite = TRUE, gdal = c("COMPRESS=DEFLATE"))
        message("Processed and saved: ", out_path)
        # Return the path as a string
        return(out_path)
    } else {
        daily_composite
    }
}


process_daily_tiles <- function(target_date, hdf_dir, output_dir, export = FALSE) {
  # 1. Find all HDF files for the target date across all tiles
  # Assuming standard MODIS naming: MCD19A2.A2010001.h27v05...hdf
  search_pattern <- paste0("MCD19A2\\.A", target_date, "\\..*\\.hdf$")
  daily_files <- list.files(hdf_dir, pattern = search_pattern, full.names = TRUE)

  if (length(daily_files) == 0) {
    warning(paste("No files found for date:", target_date))
    return(NULL)
  }

  # List to store the processed, single-layer composite for each tile
  processed_tiles <- list()

  # 2. Process each tile individually
  for (i in seq_along(daily_files)) {
    hdf_file <- daily_files[i]

    # Read subdatasets for the current tile
    aod_raw <- rast(hdf_file, subds = "Optical_Depth_047")
    unc_raw <- rast(hdf_file, subds = "AOD_Uncertainty")
    qa_raw <- rast(hdf_file, subds = "AOD_QA")

    # Scale AOD and Uncertainty
    aod <- aod_raw * 0.001
    unc <- unc_raw * 0.001

    # Apply QA and Cloud masking
    is_clear <- app(qa_raw, fun = function(x) bitwAnd(x, 7) == 1)
    is_best_qa <- app(qa_raw, fun = function(x) bitwAnd(bitwShiftR(x, 8), 15) == 0)
    valid_pixels <- is_clear & is_best_qa

    aod_m <- mask(aod, valid_pixels, maskvalue = FALSE)
    unc_m <- mask(unc, valid_pixels, maskvalue = FALSE)

    # Inverse-Variance Weighting
    weights <- 1 / (unc_m^2 + 1e-6)
    w_aod <- aod_m * weights

    sum_w_aod <- sum(w_aod, na.rm = TRUE)
    sum_w <- sum(weights, na.rm = TRUE)

    tile_composite <- sum_w_aod / sum_w
    tile_composite <- ifel(sum_w == 0, NA, tile_composite)
    names(tile_composite) <- paste0("AOD_047_", target_date)

    # Store the processed tile
    processed_tiles[[i]] <- tile_composite
  }

  # 3. Merge all processed tiles into a single continuous raster
  # sprc() creates a SpatRasterCollection, which is required by merge() or mosaic()
  tile_collection <- sprc(processed_tiles)

  # merge() is highly efficient for MODIS sinusoidal grids because the
  # tiles abut each other perfectly without overlapping pixel values.
  daily_mosaic <- merge(tile_collection)

  # 5. Write to disk immediately
  if (export) {
    out_path <- file.path(output_dir, paste0("MCD19A2_Daily_Composite_", target_date, ".tif"))

    writeRaster(daily_mosaic, out_path, overwrite = TRUE, gdal = c("COMPRESS=DEFLATE"))
    message("Processed and saved: ", out_path)
    # Return the path as a string
    return(out_path)
  } else {
    daily_mosaic
  }
}

# create range vector for all dates between 2010-2024
aod_dates <- seq(as.Date("2010-01-01"), as.Date("2024-12-31"), by = "day")
aod_dates <- format(aod_dates, "%Y%j")


library(futurize)
library(future.mirai)
plan(mirai_multisession, workers = 10L)
futurize(
  lapply(
    aod_dates,
    function(d) {
      process_daily_tiles(
        d,
        hdf_dir = bdir,
        output_dir = file.path(bdir, "processed"),
        export = TRUE
      )
      message("Completed processing for date: ", d)
    }
  )
)


# Example execution for a single day:
# target_date <- "2010001"
# hdf_directory <- "/mnt/hdd001/huimori/aerosol/"
# regional_aod <- process_daily_tiles(target_date, hdf_directory)

check <-
process_mcd19a2_day(
    target_date = "2022-01-01",
    input_dir = file.path(bdir),
    output_dir = file.path(bdir, "processed"),
    export = FALSE
)

