assert_daily_raster_geometry <- function(raster, expected) {
  same <- terra::compareGeom(raster, expected, stopOnError = FALSE)
  if (!isTRUE(same) || any(abs(terra::res(raster) - terra::res(expected)) > 1e-8) ||
      any(abs(as.vector(terra::ext(raster)) - as.vector(terra::ext(expected))) > 1e-6)) {
    stop("Raster geometry differs from explicit reference.")
  }
  invisible(TRUE)
}

daily_prediction_crop <- function(predictions, reference) {
  assert_daily_grid_identity(predictions, reference)
  res <- terra::res(reference)
  extent <- terra::ext(min(predictions$x) - res[1] / 2, max(predictions$x) + res[1] / 2,
                       min(predictions$y) - res[2] / 2, max(predictions$y) + res[2] / 2)
  crop <- terra::crop(terra::rast(reference), extent, snap = "near")
  if (terra::ncell(crop) > 100000L) stop("Smoke crop exceeds the small-raster boundary.")
  crop
}

daily_assert_prediction_slice <- function(predictions, pollutant, date, reference, expected_chunks) {
  if (length(pollutant) != 1L || !pollutant %in% c("PM10", "PM25")) stop("Invalid pollutant.")
  if (!inherits(date, "Date") || length(date) != 1L || is.na(date)) stop("Expected one Date.")
  daily_assert_columns(predictions, pollutant)
  assert_daily_grid_identity(predictions, reference)
  if (any(predictions$date != date)) stop("Prediction date mismatch.")
  if (any(!is.finite(predictions[[pollutant]]))) stop("Nonfinite smoke predictions.")
  if (is.null(names(expected_chunks)) || anyDuplicated(names(expected_chunks)) ||
      any(!nzchar(names(expected_chunks))) || anyNA(expected_chunks) || any(expected_chunks < 1L)) {
    stop("Invalid expected chunk counts.")
  }
  if (!setequal(unique(predictions$chunk_id), names(expected_chunks))) stop("Missing/unexpected chunk identity.")
  counts <- table(factor(predictions$chunk_id, levels = names(expected_chunks)))
  if (any(as.numeric(counts) != expected_chunks)) stop("Prediction chunk row count mismatch.")
  if (nrow(predictions) > 10000L) stop("Prediction slice exceeds small-smoke scope.")
  invisible(TRUE)
}

assemble_daily_prediction_raster <- function(predictions, pollutant, date, reference, expected_chunks) {
  daily_assert_prediction_slice(predictions, pollutant, date, reference, expected_chunks)
  raster <- daily_prediction_crop(predictions, reference)
  cells <- daily_global_cell_id(predictions, raster)
  values <- rep(NA_real_, terra::ncell(raster))
  values[cells] <- predictions[[pollutant]]
  terra::values(raster) <- values
  names(raster) <- pollutant
  raster
}

daily_cog_info <- function(path) {
  executable <- Sys.which("gdalinfo")
  if (!nzchar(executable)) stop("gdalinfo is required to validate COG output.")
  info <- system2(executable, c("-json", shQuote(path)), stdout = TRUE, stderr = TRUE)
  if (!is.null(attr(info, "status")) && attr(info, "status") != 0L) stop("gdalinfo failed.")
  parsed <- jsonlite::fromJSON(paste(info, collapse = "\n"), simplifyVector = FALSE)
  if (!identical(parsed$driverShortName, "GTiff") ||
      !identical(parsed$metadata$IMAGE_STRUCTURE$LAYOUT, "COG")) stop("Output is not a valid COG GeoTIFF.")
  parsed
}

write_daily_smoke_raster <- function(predictions, pollutant, date, reference, expected_chunks,
                                     output_root, model_metadata = list(nonproduction = TRUE)) {
  raster <- assemble_daily_prediction_raster(predictions, pollutant, date, reference, expected_chunks)
  if (!dir.exists(output_root)) stop("Exclusive output directory must already exist.")
  stem <- paste0(format(date), "_", pollutant)
  paths <- c(raster = file.path(output_root, paste0(stem, ".tif")),
             metadata = file.path(output_root, paste0(stem, ".json")))
  staging <- stats::setNames(paste0(paths, ".partial"), names(paths))
  if (any(file.exists(c(paths, staging)))) stop("Smoke output already exists.")
  source_tif <- tempfile("daily_smoke_source_", fileext = ".tif")
  terra::writeRaster(raster, source_tif, filetype = "GTiff", datatype = "FLT4S",
                     overwrite = FALSE, gdal = "NUM_THREADS=1")
  sf::gdal_utils("translate", source = source_tif, destination = staging[["raster"]],
    options = c("-of", "COG", "-co", "COMPRESS=DEFLATE", "-co", "LEVEL=6",
                "-co", "PREDICTOR=3", "-co", "BLOCKSIZE=512", "-co", "OVERVIEWS=NONE",
                "-co", "NUM_THREADS=1"), quiet = TRUE)
  info <- daily_cog_info(staging[["raster"]])
  metadata <- list(date = format(date), pollutant = pollutant, target_year = 2023L,
    units = "ug/m3", nonproduction = TRUE, prediction_rows = nrow(predictions),
    expected_chunks = as.list(expected_chunks), grid_template_id = daily_template_id(reference),
    ncell = terra::ncell(raster), nrow = terra::nrow(raster), ncol = terra::ncol(raster),
    resolution = terra::res(raster), extent = as.vector(terra::ext(raster)),
    crs = terra::crs(raster), non_na_cells = sum(!is.na(terra::values(raster))),
    cog = TRUE, compression = info$metadata$IMAGE_STRUCTURE$COMPRESSION,
    gdal_threads = 1L, model = model_metadata)
  jsonlite::write_json(metadata, staging[["metadata"]], auto_unbox = TRUE, pretty = TRUE, na = "null", digits = NA)
  if (!file.rename(staging[["raster"]], paths[["raster"]]) ||
      !file.rename(staging[["metadata"]], paths[["metadata"]])) stop("Could not publish smoke bundle.")
  paths
}

validate_daily_smoke_raster <- function(bundle, predictions, pollutant, date, reference, expected_chunks) {
  if (!all(file.exists(bundle)) || any(file.info(bundle)$size <= 0)) stop("Missing/empty smoke bundle.")
  daily_assert_prediction_slice(predictions, pollutant, date, reference, expected_chunks)
  raster <- terra::rast(bundle[["raster"]])
  expected <- daily_prediction_crop(predictions, reference)
  assert_daily_raster_geometry(raster, expected)
  daily_cog_info(bundle[["raster"]])
  values <- as.numeric(terra::values(raster))
  cells <- daily_global_cell_id(predictions, raster)
  if (sum(!is.na(values)) != nrow(predictions) || any(is.infinite(values)) ||
      !isTRUE(all.equal(values[cells], predictions[[pollutant]], tolerance = 1e-6))) {
    stop("Raster prediction/cell accounting differs.")
  }
  metadata <- jsonlite::read_json(bundle[["metadata"]], simplifyVector = TRUE)
  if (!identical(metadata$date, format(date)) || !identical(metadata$pollutant, pollutant) ||
      !isTRUE(metadata$nonproduction) || metadata$prediction_rows != nrow(predictions) ||
      !identical(metadata$grid_template_id, daily_template_id(reference)) ||
      !isTRUE(metadata$cog) || metadata$non_na_cells != sum(!is.na(values))) {
    stop("Smoke metadata mismatch.")
  }
  list(date = format(date), pollutant = pollutant, path = bundle[["raster"]],
       cells = terra::ncell(raster), non_na_cells = sum(!is.na(values)),
       crs = terra::crs(raster), resolution = terra::res(raster),
       extent = as.vector(terra::ext(raster)), alignment = TRUE, cog = TRUE, metadata = TRUE)
}
