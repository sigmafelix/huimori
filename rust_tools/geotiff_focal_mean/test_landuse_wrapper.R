#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(terra)
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || identical(x, "") || all(is.na(x))) y else x
}

find_test_extent <- function(r, window_cells = 96L, search_step = 128L) {
  ncols <- terra::ncol(r)
  nrows <- terra::nrow(r)
  nodata <- terra::NAflag(r)
  res_xy <- terra::res(r)

  cell_window_extent <- function(row_start, col_start) {
    row_end <- row_start + window_cells - 1L
    col_end <- col_start + window_cells - 1L

    xmin <- terra::xFromCol(r, col_start) - (res_xy[1] / 2)
    xmax <- terra::xFromCol(r, col_end) + (res_xy[1] / 2)
    ymax <- terra::yFromRow(r, row_start) + (res_xy[2] / 2)
    ymin <- terra::yFromRow(r, row_end) - (res_xy[2] / 2)

    terra::ext(xmin, xmax, ymin, ymax)
  }

  col_starts <- unique(pmax(1L, pmin(ncols - window_cells + 1L, c(
    floor((ncols - window_cells) / 2) + 1L,
    seq.int(1L, max(1L, ncols - window_cells + 1L), by = search_step)
  ))))
  row_starts <- unique(pmax(1L, pmin(nrows - window_cells + 1L, c(
    floor((nrows - window_cells) / 2) + 1L,
    seq.int(1L, max(1L, nrows - window_cells + 1L), by = search_step)
  ))))

  best_ext <- NULL
  best_nclass <- -1L

  for (row_start in row_starts) {
    for (col_start in col_starts) {
      ext_try <- cell_window_extent(row_start, col_start)
      vals <- terra::values(terra::crop(r, ext_try), mat = FALSE)
      vals <- vals[!is.na(vals)]
      if (!is.na(nodata)) {
        vals <- vals[vals != nodata]
      }
      nclass <- length(unique(vals))
      if (nclass > best_nclass) {
        best_nclass <- nclass
        best_ext <- ext_try
      }
      if (nclass >= 2L) {
        return(ext_try)
      }
    }
  }

  if (is.null(best_ext) || best_nclass < 1L) {
    stop("Could not find a non-empty subset with valid landuse classes.", call. = FALSE)
  }

  best_ext
}

get_band_descriptions <- function(output_file) {
  gdalinfo_bin <- Sys.which("gdalinfo")
  if (!nzchar(gdalinfo_bin)) {
    return(NULL)
  }

  info_json <- system2(gdalinfo_bin, c("-json", output_file), stdout = TRUE, stderr = TRUE)
  json_text <- paste(info_json, collapse = "\n")
  jsonlite::fromJSON(json_text, simplifyVector = TRUE)
}

input_file <- args[1] %||% "/mnt/hdd001/Korea/landuse/glc_fcs30d/lc_glc_fcs30d_30m_2022.tif"
work_dir <- normalizePath(args[2] %||% file.path(tempdir(), "geotiff_focal_mean_test"), mustWork = FALSE)
dir.create(work_dir, recursive = TRUE, showWarnings = FALSE)

subset_file <- file.path(work_dir, "landuse_subset_2022.tif")
output_file <- file.path(work_dir, "landuse_subset_freq_2022.tif")

script_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
script_path <- sub("^--file=", "", script_arg[1] %||% "rust_tools/geotiff_focal_mean/test_landuse_wrapper.R")
repo_root <- normalizePath(file.path(dirname(script_path), "..", ".."), mustWork = TRUE)
tool_dir <- file.path(repo_root, "rust_tools", "geotiff_focal_mean")
tool_bin <- "cargo"

message("Reading source raster: ", input_file)
src <- terra::rast(input_file)
test_ext <- find_test_extent(src, window_cells = 1024L, search_step = 128L)
subset_ras <- terra::crop(src, test_ext)
terra::writeRaster(subset_ras, subset_file, overwrite = TRUE, gdal = c("COMPRESS=DEFLATE"))

cmd <- if (basename(tool_bin) == "cargo") {
  c("run", "--quiet", "--", "--input", subset_file, "--output", output_file,
    "--radius", "33", "--radius-in-cells", "--chunksize", "256", "--threads", "2", "--verbose")
} else {
  c("--input", subset_file, "--output", output_file,
    "--radius", "33", "--radius-in-cells", "--chunksize", "256", "--threads", "2", "--verbose")
}
cmd_bin <- if (basename(tool_bin) == "cargo") "cargo" else tool_bin

message("Running tool: ", cmd_bin, " ", paste(shQuote(cmd), collapse = " "))
orig_wd <- getwd()
if (basename(tool_bin) == "cargo") {
  setwd(tool_dir)
}
on.exit(setwd(orig_wd), add = TRUE)
status <- system2(cmd_bin, cmd)
if (!identical(status, 0L)) {
  stop("Rust focal-mean tool failed with exit status ", status, call. = FALSE)
}

out <- terra::rast(output_file)
band_names <- names(out)
if (length(band_names) != terra::nlyr(out)) {
  stop("Output band names are incomplete.", call. = FALSE)
}
if (!all(grepl("^landuse_class_[0-9]+$", band_names))) {
  stop("Unexpected output band names: ", paste(band_names, collapse = ", "), call. = FALSE)
}

gd_info <- get_band_descriptions(output_file)
if (!is.null(gd_info) && !is.null(gd_info$bands)) {
  desc <- vapply(gd_info$bands$description, identity, character(1))
  meta_class <- vapply(
    gd_info$bands$metadata,
    function(x) {
      if (is.null(x) || is.null(x$`""`) || is.null(x$`""`$LANDUSE_CLASS)) {
        NA_character_
      } else {
        x$`""`$LANDUSE_CLASS
      }
    },
    character(1)
  )

  if (!identical(unname(desc), unname(band_names))) {
    stop("GDAL band descriptions do not match terra layer names.", call. = FALSE)
  }

  expected_from_name <- sub("^landuse_class_", "", band_names)
  if (!all(meta_class == expected_from_name)) {
    stop(
      "LANDUSE_CLASS metadata does not match band names. Metadata: ",
      paste(meta_class, collapse = ", "),
      "; names: ",
      paste(band_names, collapse = ", "),
      call. = FALSE
    )
  }
}

summary_df <- data.frame(
  band = seq_along(band_names),
  name = band_names,
  min = as.numeric(terra::global(out, "min", na.rm = TRUE)[, 1]),
  max = as.numeric(terra::global(out, "max", na.rm = TRUE)[, 1])
)

message("Smoke test passed.")
print(summary_df, row.names = FALSE)
message("Subset file: ", subset_file)
message("Output file: ", output_file)
