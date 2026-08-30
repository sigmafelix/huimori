aod_v2_schema_version <- function() {
  "aod-processed-v2.0.0"
}

aod_v2_limit_nested_threads <- function() {
  values <- c(
    OMP_NUM_THREADS = "1",
    OPENBLAS_NUM_THREADS = "1",
    MKL_NUM_THREADS = "1",
    VECLIB_MAXIMUM_THREADS = "1",
    NUMEXPR_NUM_THREADS = "1",
    GDAL_NUM_THREADS = "1"
  )
  do.call(Sys.setenv, as.list(values))
  invisible(values)
}

aod_v2_assert_output_root <- function(output_dir) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  resolved <- normalizePath(output_dir, winslash = "/", mustWork = TRUE)
  allowed <- normalizePath(file.path("daehoon", "data"), winslash = "/", mustWork = TRUE)
  if (!startsWith(paste0(resolved, "/"), paste0(allowed, "/"))) {
    stop("AOD v2 output must resolve below daehoon/data: ", resolved)
  }
  resolved
}

aod_v2_dates <- function(start = "2015-01-01", end = "2023-12-31") {
  format(seq(as.Date(start), as.Date(end), by = "day"), "%Y%j")
}

aod_v2_source_files <- function(target_date, input_dir) {
  pattern <- paste0("^MCD19A2\\.A", target_date, "\\.h(27|28)v05\\.061\\..*\\.hdf$")
  sort(list.files(input_dir, pattern = pattern, full.names = TRUE))
}

aod_v2_output_paths <- function(target_date, output_dir) {
  stem <- paste0("MCD19A2_Daily_Composite_", target_date)
  c(
    raster = file.path(output_dir, paste0(stem, ".tif")),
    provenance = file.path(output_dir, paste0(stem, ".json"))
  )
}

aod_v2_metadata_value <- function(raster, key) {
  tags <- terra::metags(raster)
  value <- tags$value[tags$name == key]
  if (length(value) < 1L) {
    stop("MCD19A2 subdataset is missing metadata key: ", key)
  }
  as.character(value[[1L]])
}

aod_v2_open_tile <- function(path) {
  aod_v2_limit_nested_threads()
  aod <- terra::rast(path, subds = "Optical_Depth_047")
  uncertainty <- terra::rast(path, subds = "AOD_Uncertainty")
  qa <- terra::rast(path, subds = "AOD_QA")
  n_overpasses <- terra::nlyr(aod)
  if (n_overpasses < 1L ||
      terra::nlyr(uncertainty) != n_overpasses ||
      terra::nlyr(qa) != n_overpasses ||
      !isTRUE(terra::compareGeom(aod, uncertainty, stopOnError = FALSE)) ||
      !isTRUE(terra::compareGeom(aod, qa, stopOnError = FALSE))) {
    stop("MCD19A2 tile subdatasets must have matching non-empty layer geometry: ", path)
  }
  aod_scale <- as.numeric(aod_v2_metadata_value(aod, "scale_factor"))
  uncertainty_scale <- as.numeric(aod_v2_metadata_value(uncertainty, "scale_factor"))
  aod_offset <- as.numeric(aod_v2_metadata_value(aod, "add_offset"))
  uncertainty_offset <- as.numeric(aod_v2_metadata_value(uncertainty, "add_offset"))
  if (!isTRUE(all.equal(aod_scale, 0.001, tolerance = 0)) ||
      !isTRUE(all.equal(uncertainty_scale, 0.0001, tolerance = 0)) ||
      aod_offset != 0 ||
      uncertainty_offset != 0) {
    stop(
      "Unexpected MCD19A2 scale/offset metadata in ", path,
      ": AOD=", aod_scale, "/", aod_offset,
      ", uncertainty=", uncertainty_scale, "/", uncertainty_offset
    )
  }
  list(
    aod = aod,
    uncertainty = uncertainty,
    qa = qa,
    metadata = list(
      aod_scale_factor = aod_scale,
      aod_add_offset = aod_offset,
      aod_valid_range_stored = aod_v2_metadata_value(aod, "valid_range"),
      aod_fill_value_stored = aod_v2_metadata_value(aod, "_FillValue"),
      uncertainty_scale_factor = uncertainty_scale,
      uncertainty_add_offset = uncertainty_offset,
      uncertainty_valid_range_stored = aod_v2_metadata_value(uncertainty, "valid_range"),
      uncertainty_fill_value_stored = aod_v2_metadata_value(uncertainty, "_FillValue"),
      qa_fill_value = aod_v2_metadata_value(qa, "_FillValue"),
      overpass_layers = n_overpasses
    )
  )
}

aod_v2_template <- function(input_dir) {
  all_files <- sort(list.files(
    input_dir,
    pattern = "^MCD19A2\\.A[0-9]{7}\\.h(27|28)v05\\.061\\..*\\.hdf$",
    full.names = TRUE
  ))
  h27 <- all_files[grepl("\\.h27v05\\.", all_files)]
  h28 <- all_files[grepl("\\.h28v05\\.", all_files)]
  if (length(h27) < 1L || length(h28) < 1L) {
    stop("AOD v2 requires h27v05 and h28v05 native tiles.")
  }
  left <- terra::rast(h27[[1L]], subds = "Optical_Depth_047")[[1L]]
  right <- terra::rast(h28[[1L]], subds = "Optical_Depth_047")[[1L]]
  if (!isTRUE(terra::same.crs(left, right)) ||
      !isTRUE(all.equal(terra::res(left), terra::res(right), tolerance = 1e-10))) {
    stop("MCD19A2 h27/h28 native CRS or resolution differs.")
  }
  extent <- c(
    min(terra::xmin(left), terra::xmin(right)),
    max(terra::xmax(left), terra::xmax(right)),
    min(terra::ymin(left), terra::ymin(right)),
    max(terra::ymax(left), terra::ymax(right))
  )
  resolution <- terra::res(left)
  terra::rast(
    nrows = as.integer(round((extent[[4L]] - extent[[3L]]) / resolution[[2L]])),
    ncols = as.integer(round((extent[[2L]] - extent[[1L]]) / resolution[[1L]])),
    xmin = extent[[1L]],
    xmax = extent[[2L]],
    ymin = extent[[3L]],
    ymax = extent[[4L]],
    crs = terra::crs(left)
  )
}

aod_v2_qa_mask <- function(qa) {
  terra::app(
    qa,
    fun = function(x) {
      x <- as.integer(x)
      as.integer(bitwAnd(x, 7L) == 1L & bitwAnd(bitwShiftR(x, 8L), 15L) == 0L)
    }
  )
}

aod_v2_process_tile <- function(path, target_date) {
  source <- aod_v2_open_tile(path)
  qa_valid <- aod_v2_qa_mask(source$qa)
  valid <- qa_valid == 1L &
    !is.na(source$aod) &
    !is.na(source$uncertainty) &
    source$aod >= -0.1 &
    source$aod <= 6 &
    source$uncertainty >= 0 &
    source$uncertainty <= 3

  # terra/GDAL has already applied the HDF scale and offset metadata.
  aod_physical <- terra::ifel(valid, source$aod, NA)
  uncertainty_physical <- terra::ifel(valid, source$uncertainty, NA)
  weights <- 1 / (uncertainty_physical^2 + 1e-6)
  sum_weights <- sum(weights, na.rm = TRUE)
  composite <- sum(aod_physical * weights, na.rm = TRUE) / sum_weights
  composite <- terra::ifel(sum_weights > 0, composite, NA)
  names(composite) <- paste0("AOD_047_", target_date)
  list(raster = composite, metadata = source$metadata)
}

aod_v2_align_to_template <- function(raster, template, target_date) {
  if (isTRUE(terra::compareGeom(template, raster, stopOnError = FALSE))) {
    return(raster)
  }
  same_crs <- isTRUE(terra::same.crs(template, raster))
  same_res <- isTRUE(all.equal(terra::res(template), terra::res(raster), tolerance = 1e-8))
  inside <- terra::xmin(raster) >= terra::xmin(template) - 1e-4 &&
    terra::xmax(raster) <= terra::xmax(template) + 1e-4 &&
    terra::ymin(raster) >= terra::ymin(template) - 1e-4 &&
    terra::ymax(raster) <= terra::ymax(template) + 1e-4
  if (!same_crs || !same_res || !inside) {
    stop("AOD daily geometry is incompatible with the v2 template: ", target_date)
  }
  out <- terra::extend(raster, template)
  if (!isTRUE(terra::compareGeom(template, out, stopOnError = FALSE))) {
    stop("AOD daily raster could not be aligned to the v2 template: ", target_date)
  }
  out
}

aod_v2_source_provenance <- function(paths) {
  info <- file.info(paths)
  lapply(seq_along(paths), function(i) {
    list(
      path = normalizePath(paths[[i]], winslash = "/", mustWork = TRUE),
      bytes = unname(info$size[[i]]),
      mtime = format(info$mtime[[i]], "%Y-%m-%dT%H:%M:%S%z")
    )
  })
}

aod_v2_raster_summary <- function(raster) {
  stats <- terra::global(raster, fun = c("min", "mean", "max"), na.rm = TRUE)
  valid <- terra::global(!is.na(raster), fun = "sum", na.rm = TRUE)[1L, 1L]
  list(
    valid_cells = unname(valid),
    min = unname(stats[1L, "min"]),
    mean = unname(stats[1L, "mean"]),
    max = unname(stats[1L, "max"])
  )
}

aod_v2_provenance <- function(target_date, source_files, source_metadata, raster, output_file) {
  extent <- as.vector(terra::ext(raster))
  list(
    schema_version = aod_v2_schema_version(),
    date_yyyyddd = target_date,
    date = format(as.Date(target_date, "%Y%j"), "%Y-%m-%d"),
    output_file = normalizePath(output_file, winslash = "/", mustWork = FALSE),
    source_hdf = aod_v2_source_provenance(source_files),
    source_product = "MCD19A2.061",
    variable = "Optical_Depth_047",
    unit = "1",
    scale_metadata = source_metadata,
    scale_handling = paste(
      "terra/GDAL applies HDF scale_factor and add_offset on read;",
      "no additional AOD or uncertainty scaling is performed"
    ),
    qa = list(
      cloud_mask_bits_0_2 = "001 (clear)",
      aod_qa_bits_8_11 = "0000 (best quality)",
      expression = "bitwAnd(qa,7)==1 && bitwAnd(bitwShiftR(qa,8),15)==0"
    ),
    physical_valid_range = list(aod = c(-0.1, 6), uncertainty = c(0, 3)),
    weighting = list(
      method = "inverse_variance",
      formula = "sum(aod / (uncertainty^2 + 1e-6)) / sum(1 / (uncertainty^2 + 1e-6))",
      epsilon = 1e-6
    ),
    aggregation = "QA-filtered uncertainty-weighted daily composite",
    geometry = list(
      crs_wkt = terra::crs(raster),
      proj = terra::crs(raster, proj = TRUE),
      nrow = terra::nrow(raster),
      ncol = terra::ncol(raster),
      resolution = unname(terra::res(raster)),
      extent = unname(extent)
    ),
    summary = aod_v2_raster_summary(raster),
    created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )
}

aod_v2_write_atomic <- function(raster, provenance, paths) {
  dir.create(dirname(paths[["raster"]]), recursive = TRUE, showWarnings = FALSE)
  raster_temp <- tempfile(
    pattern = paste0(".", basename(paths[["raster"]]), "_"),
    tmpdir = dirname(paths[["raster"]]),
    fileext = ".tif"
  )
  json_temp <- tempfile(
    pattern = paste0(".", basename(paths[["provenance"]]), "_"),
    tmpdir = dirname(paths[["provenance"]]),
    fileext = ".json"
  )
  on.exit(unlink(c(raster_temp, json_temp), force = TRUE), add = TRUE)
  terra::writeRaster(
    raster,
    raster_temp,
    overwrite = TRUE,
    datatype = "FLT4S",
    NAflag = -9999,
    gdal = c("COMPRESS=ZSTD", "PREDICTOR=3", "TILED=YES", "BIGTIFF=IF_SAFER", "NUM_THREADS=1")
  )
  provenance$output_file <- normalizePath(paths[["raster"]], winslash = "/", mustWork = FALSE)
  jsonlite::write_json(
    provenance,
    json_temp,
    auto_unbox = TRUE,
    pretty = TRUE,
    digits = 16,
    na = "null"
  )
  if (!file.rename(raster_temp, paths[["raster"]])) {
    stop("Could not atomically install AOD v2 raster: ", paths[["raster"]])
  }
  if (!file.rename(json_temp, paths[["provenance"]])) {
    unlink(paths[["raster"]], force = TRUE)
    stop("Could not atomically install AOD v2 provenance: ", paths[["provenance"]])
  }
  unname(paths)
}

aod_v2_validate_output <- function(paths, template, target_date) {
  if (!all(file.exists(paths)) || any(file.info(paths)$size <= 0)) {
    stop("AOD v2 output bundle is missing or empty: ", target_date)
  }
  raster <- terra::rast(paths[["raster"]])
  if (terra::nlyr(raster) != 1L ||
      !isTRUE(terra::compareGeom(template, raster, stopOnError = FALSE))) {
    stop("AOD v2 output geometry is invalid: ", target_date)
  }
  provenance <- jsonlite::read_json(paths[["provenance"]], simplifyVector = TRUE)
  source_paths <- unlist(provenance$source_hdf$path, use.names = FALSE)
  if (!identical(provenance$schema_version, aod_v2_schema_version()) ||
      !identical(provenance$date_yyyyddd, target_date) ||
      !identical(provenance$unit, "1") ||
      length(source_paths) < 1L ||
      !all(file.exists(source_paths))) {
    stop("AOD v2 provenance is invalid: ", target_date)
  }
  invisible(list(raster = raster, provenance = provenance))
}

process_mcd19a2_day_v2 <- function(
  target_date,
  input_dir,
  output_dir,
  template = NULL,
  source_files = NULL,
  overwrite = FALSE
) {
  aod_v2_limit_nested_threads()
  output_dir <- aod_v2_assert_output_root(output_dir)
  worker_temp <- file.path(output_dir, ".tmp", paste0("worker_", Sys.getpid()))
  dir.create(worker_temp, recursive = TRUE, showWarnings = FALSE)
  terra::terraOptions(tempdir = worker_temp, threads = 1)
  on.exit(unlink(worker_temp, recursive = TRUE, force = TRUE), add = TRUE)

  source_files <- if (is.null(source_files)) {
    aod_v2_source_files(target_date, input_dir)
  } else {
    sort(source_files)
  }
  if (length(source_files) < 1L) {
    return(data.frame(
      target_date = target_date,
      status = "missing_source",
      raster = NA_character_,
      provenance = NA_character_,
      seconds = 0,
      valid_cells = NA_real_,
      min = NA_real_,
      mean = NA_real_,
      max = NA_real_,
      stringsAsFactors = FALSE
    ))
  }
  if (length(source_files) > 2L) {
    stop("More than two MCD19A2 h27/h28 source files for ", target_date)
  }
  template <- if (is.null(template)) {
    aod_v2_template(input_dir)
  } else if (inherits(template, "PackedSpatRaster")) {
    terra::unwrap(template)
  } else {
    template
  }
  paths <- aod_v2_output_paths(target_date, output_dir)
  if (!overwrite && all(file.exists(paths))) {
    valid_existing <- try(aod_v2_validate_output(paths, template, target_date), silent = TRUE)
    if (!inherits(valid_existing, "try-error")) {
      summary <- aod_v2_raster_summary(valid_existing$raster)
      return(data.frame(
        target_date = target_date,
        status = "skipped_valid",
        raster = paths[["raster"]],
        provenance = paths[["provenance"]],
        seconds = 0,
        valid_cells = summary$valid_cells,
        min = summary$min,
        mean = summary$mean,
        max = summary$max,
        stringsAsFactors = FALSE
      ))
    }
  }
  unlink(paths[file.exists(paths)], force = TRUE)
  started <- proc.time()[["elapsed"]]
  tiles <- lapply(source_files, aod_v2_process_tile, target_date = target_date)
  metadata <- lapply(tiles, function(x) x[["metadata"]])
  rasters <- lapply(tiles, function(x) x[["raster"]])
  mosaic <- if (length(rasters) == 1L) rasters[[1L]] else terra::merge(terra::sprc(rasters))
  mosaic <- aod_v2_align_to_template(mosaic, template, target_date)
  names(mosaic) <- paste0("AOD_047_", target_date)
  provenance <- aod_v2_provenance(
    target_date = target_date,
    source_files = source_files,
    source_metadata = metadata,
    raster = mosaic,
    output_file = paths[["raster"]]
  )
  aod_v2_write_atomic(mosaic, provenance, paths)
  validated <- aod_v2_validate_output(paths, template, target_date)
  summary <- aod_v2_raster_summary(validated$raster)
  data.frame(
    target_date = target_date,
    status = "created",
    raster = paths[["raster"]],
    provenance = paths[["provenance"]],
    seconds = proc.time()[["elapsed"]] - started,
    valid_cells = summary$valid_cells,
    min = summary$min,
    mean = summary$mean,
    max = summary$max,
    stringsAsFactors = FALSE
  )
}

aod_v2_direct_pixel_values <- function(target_date, input_dir, x, y) {
  source_files <- aod_v2_source_files(target_date, input_dir)
  for (path in source_files) {
    source <- aod_v2_open_tile(path)
    extent <- terra::ext(source$aod)
    if (x >= terra::xmin(extent) && x <= terra::xmax(extent) &&
        y >= terra::ymin(extent) && y <= terra::ymax(extent)) {
      xy <- matrix(c(x, y), nrow = 1L)
      aod <- as.numeric(terra::extract(source$aod, xy)[1L, ])
      uncertainty <- as.numeric(terra::extract(source$uncertainty, xy)[1L, ])
      qa <- as.integer(terra::extract(source$qa, xy)[1L, ])
      valid <- !is.na(aod) &
        !is.na(uncertainty) &
        !is.na(qa) &
        bitwAnd(qa, 7L) == 1L &
        bitwAnd(bitwShiftR(qa, 8L), 15L) == 0L &
        aod >= -0.1 &
        aod <= 6 &
        uncertainty >= 0 &
        uncertainty <= 3
      if (!any(valid)) {
        return(c(expected = NA_real_, n_overpasses = 0))
      }
      weights <- 1 / (uncertainty[valid]^2 + 1e-6)
      return(c(
        expected = sum(aod[valid] * weights) / sum(weights),
        n_overpasses = sum(valid)
      ))
    }
  }
  c(expected = NA_real_, n_overpasses = 0)
}

aod_v2_reproduce_pixels <- function(
  target_date,
  input_dir,
  output_file,
  sample_size = 200L,
  seed = 1L
) {
  raster <- terra::rast(output_file)
  set.seed(seed)
  sample <- terra::spatSample(
    raster,
    size = sample_size,
    method = "random",
    na.rm = TRUE,
    xy = TRUE,
    values = TRUE,
    as.df = TRUE
  )
  direct <- t(mapply(
    aod_v2_direct_pixel_values,
    x = sample$x,
    y = sample$y,
    MoreArgs = list(target_date = target_date, input_dir = input_dir)
  ))
  value_col <- setdiff(names(sample), c("x", "y"))[[1L]]
  out <- data.frame(
    x = sample$x,
    y = sample$y,
    output = sample[[value_col]],
    expected = direct[, "expected"],
    n_overpasses = as.integer(direct[, "n_overpasses"])
  )
  out$absolute_error <- abs(out$output - out$expected)
  out
}

aod_v2_write_manifest <- function(x, output_dir) {
  path <- file.path(output_dir, "aod_processed_v2_manifest.csv")
  temp <- tempfile(pattern = ".manifest_", tmpdir = output_dir, fileext = ".csv")
  on.exit(unlink(temp, force = TRUE), add = TRUE)
  utils::write.csv(x, temp, row.names = FALSE, na = "")
  if (!file.rename(temp, path)) {
    stop("Could not atomically write AOD v2 run manifest.")
  }
  path
}

run_aod_v2 <- function(
  input_dir,
  output_dir,
  start = "2015-01-01",
  end = "2023-12-31",
  workers = 20L,
  batch_size = 100L
) {
  aod_v2_limit_nested_threads()
  output_dir <- aod_v2_assert_output_root(output_dir)
  workers <- min(as.integer(workers), 20L)
  if (workers < 1L) {
    stop("workers must be between 1 and 20.")
  }
  expected <- aod_v2_dates(start, end)
  source_catalog <- sort(list.files(
    input_dir,
    pattern = "^MCD19A2\\.A[0-9]{7}\\.h(27|28)v05\\.061\\..*\\.hdf$",
    full.names = TRUE
  ))
  source_date <- sub(
    "^MCD19A2\\.A([0-9]{7})\\..*$",
    "\\1",
    basename(source_catalog)
  )
  source_index <- split(source_catalog, source_date)
  dates <- expected[expected %in% names(source_index)]
  template <- terra::wrap(aod_v2_template(input_dir))
  batches <- split(dates, ceiling(seq_along(dates) / as.integer(batch_size)))
  all_results <- list()
  future::plan(future::multisession, workers = workers)
  on.exit(future::plan(future::sequential), add = TRUE)
  started <- Sys.time()
  for (i in seq_along(batches)) {
    result <- future.apply::future_lapply(
      batches[[i]],
      function(target_date) {
        process_mcd19a2_day_v2(
          target_date = target_date,
          input_dir = input_dir,
          output_dir = output_dir,
          template = template,
          source_files = source_index[[target_date]],
          overwrite = FALSE
        )
      },
      future.packages = c("terra", "jsonlite"),
      future.seed = TRUE,
      future.stdout = FALSE,
      future.scheduling = 2
    )
    all_results[[i]] <- do.call(rbind, result)
    manifest <- do.call(rbind, all_results)
    aod_v2_write_manifest(manifest, output_dir)
    cat(sprintf(
      "%s batch %d/%d complete: %d/%d dates, elapsed=%.1f min\n",
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      i,
      length(batches),
      nrow(manifest),
      length(dates),
      as.numeric(difftime(Sys.time(), started, units = "mins"))
    ))
    flush.console()
  }
  manifest <- do.call(rbind, all_results)
  attr(manifest, "expected_dates") <- expected
  attr(manifest, "source_dates") <- dates
  manifest
}

aod_v2_cli_value <- function(args, key, default = NULL) {
  prefix <- paste0("--", key, "=")
  hit <- args[startsWith(args, prefix)]
  if (length(hit) < 1L) default else sub(prefix, "", hit[[1L]], fixed = TRUE)
}

if (sys.nframe() == 0L) {
  suppressPackageStartupMessages({
    library(terra)
    library(jsonlite)
    library(future)
    library(future.apply)
  })
  args <- commandArgs(trailingOnly = TRUE)
  input_dir <- aod_v2_cli_value(args, "input", "/mnt/hdd001/Korea/climate/aerosol")
  output_dir <- aod_v2_cli_value(args, "output", file.path("daehoon", "data", "aod_processed_v2"))
  start <- aod_v2_cli_value(args, "start", "2015-01-01")
  end <- aod_v2_cli_value(args, "end", "2023-12-31")
  workers <- as.integer(aod_v2_cli_value(args, "workers", "20"))
  batch_size <- as.integer(aod_v2_cli_value(args, "batch-size", "100"))
  result <- run_aod_v2(
    input_dir = input_dir,
    output_dir = output_dir,
    start = start,
    end = end,
    workers = workers,
    batch_size = batch_size
  )
  cat(
    "AOD v2 run complete: ", nrow(result),
    " source dates, output=", normalizePath(output_dir), "\n",
    sep = ""
  )
}
