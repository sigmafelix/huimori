canonical_data_artifact_path <- function(path, label, must_exist = TRUE) {
  logical_root <- file.path("daehoon", "data")
  if (!dir.exists(logical_root)) {
    stop("Canonical data root does not exist: ", logical_root)
  }
  if (!must_exist) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
  if (!file.exists(path)) {
    stop(label, " does not exist: ", path)
  }
  root_resolved <- normalizePath(logical_root, winslash = "/", mustWork = TRUE)
  path_resolved <- normalizePath(path, winslash = "/", mustWork = TRUE)
  if (!startsWith(paste0(path_resolved, "/"), paste0(root_resolved, "/"))) {
    stop(label, " must resolve below ", logical_root, ": ", path_resolved)
  }
  path
}

canonical_aod_processed_dir <- function(
  path = file.path("daehoon", "data", "aod_processed_v2")
) {
  canonical_data_artifact_path(path, "Canonical processed AOD directory")
}

canonical_aod_inventory <- function(aod_dir, dates, validate_provenance = TRUE) {
  aod_dir <- canonical_aod_processed_dir(aod_dir)
  dates <- as.Date(dates)
  if (length(dates) < 1L || anyNA(dates) || anyDuplicated(dates)) {
    stop("Canonical AOD inventory requires unique, non-missing dates.")
  }
  date_code <- format(dates, "%Y%j")
  stem <- paste0("MCD19A2_Daily_Composite_", date_code)
  out <- data.frame(
    date = dates,
    date_code = date_code,
    raster = file.path(aod_dir, paste0(stem, ".tif")),
    provenance = file.path(aod_dir, paste0(stem, ".json")),
    stringsAsFactors = FALSE
  )
  out$available <- file.exists(out$raster)
  if (any(out$available & !file.exists(out$provenance))) {
    bad <- out$date[out$available & !file.exists(out$provenance)]
    stop("Canonical AOD raster is missing v2 provenance for date(s): ", paste(bad, collapse = ", "))
  }
  if (validate_provenance && any(out$available)) {
    valid_rows <- which(out$available)
    valid <- vapply(valid_rows, function(i) {
      metadata <- jsonlite::read_json(out$provenance[[i]], simplifyVector = TRUE)
      identical(metadata$schema_version, "aod-processed-v2.0.0") &&
        identical(metadata$date_yyyyddd, out$date_code[[i]]) &&
        identical(metadata$variable, "Optical_Depth_047") &&
        identical(metadata$unit, "1") &&
        grepl(
          "no additional AOD or uncertainty scaling is performed",
          metadata$scale_handling,
          fixed = TRUE
        )
    }, logical(1))
    if (!all(valid)) {
      stop(
        "Canonical AOD v2 provenance validation failed for date(s): ",
        paste(out$date[valid_rows[!valid]], collapse = ", ")
      )
    }
  }
  out
}

assert_same_native_geometry <- function(rasters, label) {
  if (length(rasters) < 1L) {
    stop(label, " requires at least one raster.")
  }
  reference <- rasters[[1L]][[1L]]
  valid <- vapply(rasters, function(raster) {
    isTRUE(terra::compareGeom(reference, raster[[1L]], stopOnError = FALSE))
  }, logical(1))
  if (!all(valid)) {
    stop(label, " source rasters do not share one native geometry.")
  }
  invisible(reference)
}

canonical_raster_median <- function(files, label) {
  files <- unname(files[file.exists(files)])
  if (length(files) < 1L) {
    stop(label, " has no available source rasters.")
  }
  rasters <- lapply(files, terra::rast)
  assert_same_native_geometry(rasters, label)
  stack <- terra::rast(rasters)
  terra::app(
    stack,
    fun = function(x) {
      if (all(is.na(x))) NA_real_ else stats::median(x, na.rm = TRUE)
    }
  )
}

write_canonical_raster <- function(raster, path, provenance) {
  out_dir <- canonical_data_artifact_path(dirname(path), "Canonical raster output directory", must_exist = FALSE)
  path <- file.path(out_dir, basename(path))
  json_path <- sub("\\.tif$", ".json", path)
  raster_temp <- tempfile(pattern = ".canonical_", tmpdir = out_dir, fileext = ".tif")
  json_temp <- tempfile(pattern = ".canonical_", tmpdir = out_dir, fileext = ".json")
  on.exit(unlink(c(raster_temp, json_temp), force = TRUE), add = TRUE)
  terra::writeRaster(
    raster,
    raster_temp,
    overwrite = TRUE,
    datatype = "FLT4S",
    NAflag = -9999,
    gdal = c("COMPRESS=ZSTD", "PREDICTOR=3", "TILED=YES", "NUM_THREADS=1")
  )
  provenance$output_file <- path
  jsonlite::write_json(
    provenance,
    json_temp,
    auto_unbox = TRUE,
    pretty = TRUE,
    digits = 16,
    na = "null"
  )
  if (!file.rename(raster_temp, path)) {
    stop("Could not install canonical raster: ", path)
  }
  if (!file.rename(json_temp, json_path)) {
    unlink(path, force = TRUE)
    stop("Could not install canonical raster provenance: ", json_path)
  }
  path
}

daily_cube_year_branches <- function(cubes, source, year) {
  year <- unique(as.integer(year))
  if (length(year) != 1L || is.na(year)) {
    stop("Annual ", source, " cube aggregation requires exactly one year.")
  }
  branches <- as_daily_cube_branches(cubes, paste0(source, " daily cubes"))
  metadata <- lapply(branches, function(bundle) {
    manifest <- validate_daily_cube(bundle, source = source)
    data.frame(
      month = unique(manifest$month),
      bundle = I(list(bundle)),
      stringsAsFactors = FALSE
    )
  }) |>
    dplyr::bind_rows() |>
    dplyr::filter(substr(month, 1L, 4L) == as.character(year)) |>
    dplyr::arrange(month)
  expected <- sprintf("%04d-%02d", year, 1:12)
  if (!identical(metadata$month, expected)) {
    stop(
      "Annual ", source, " requires exactly 12 ordered monthly cubes for ", year,
      "; found: ", paste(metadata$month, collapse = ", ")
    )
  }
  unname(metadata$bundle)
}

build_yearly_raster_from_daily_cubes <- function(
  cubes,
  source,
  year,
  output_root,
  output_stem
) {
  branches <- daily_cube_year_branches(cubes, source, year)
  rasters <- lapply(branches, function(bundle) terra::rast(daily_cube_file(bundle)))
  assert_same_native_geometry(rasters, paste0("Annual ", source, " daily cubes"))
  stack <- do.call(c, rasters)
  raster <- terra::app(
    stack,
    fun = function(x) {
      if (all(is.na(x))) NA_real_ else stats::median(x, na.rm = TRUE)
    }
  )
  names(raster) <- paste0(output_stem, "_", year)
  path <- file.path(output_root, paste0(output_stem, "_", year, ".tif"))
  provenance <- list(
    schema_version = paste0("canonical-", source, "-annual-from-daily-cubes-v1"),
    source = source,
    year = year,
    temporal_aggregation = "median across canonical KST/source-daily cube layers",
    source_cube_files = unlist(branches, use.names = FALSE),
    source_months = sprintf("%04d-%02d", year, 1:12),
    spatial_extraction = "downstream 100/500/2000/5000 m buffer mean",
    created_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z")
  )
  write_canonical_raster(raster, path, provenance)
}

build_aod_yearly_from_daily_cubes <- function(
  cubes,
  year,
  output_root = file.path("daehoon", "data", "aod_processed_v2", "annual")
) {
  build_yearly_raster_from_daily_cubes(
    cubes = cubes,
    source = "aod",
    year = year,
    output_root = output_root,
    output_stem = "aod_yearly"
  )
}

era5_actual_valid_time <- function(raster, label) {
  stamp_text <- sub(".*valid_time=", "", names(raster))
  stamp <- suppressWarnings(as.numeric(stamp_text))
  if (length(stamp) != terra::nlyr(raster) || anyNA(stamp) || any(stamp %% 3600 != 0)) {
    stop(label, " does not expose an hourly actual valid_time for every layer.")
  }
  time_utc <- as.POSIXct(stamp, origin = "1970-01-01", tz = "UTC")
  if (anyDuplicated(time_utc)) {
    stop(label, " contains duplicated actual valid_time timestamps.")
  }
  time_utc
}

era5_required_utc_months <- function(target_dates) {
  target_dates <- sort(unique(as.Date(target_dates)))
  if (length(target_dates) < 1L || anyNA(target_dates)) {
    stop("ERA5 KST aggregation requires non-missing target dates.")
  }
  first_month <- as.Date(format(min(target_dates) - 1, "%Y-%m-01"))
  last_month <- as.Date(format(max(target_dates), "%Y-%m-01"))
  format(seq(first_month, last_month, by = "month"), "%Y-%m")
}

canonical_blh_hourly_files <- function(era5_dir, target_dates) {
  months <- era5_required_utc_months(target_dates)
  paths <- file.path(era5_dir, paste0("ERA5_BLH_", sub("-", "_", months), ".nc"))
  missing <- paths[!file.exists(paths)]
  if (length(missing) > 0L) {
    stop("Missing canonical ERA5-BLH hourly file(s): ", paste(missing, collapse = ", "))
  }
  paths
}

era5_hourly_variable_parts <- function(rasters, variable) {
  parts <- lapply(seq_along(rasters), function(i) {
    idx <- grep(paste0("^", variable, "_"), names(rasters[[i]]))
    if (length(idx) < 1L) {
      stop("ERA5 input ", i, " is missing variable ", variable, ".")
    }
    raster <- rasters[[i]][[idx]]
    list(
      raster = raster,
      time = era5_actual_valid_time(raster, paste0("ERA5 ", variable, " input ", i))
    )
  })
  assert_same_native_geometry(lapply(parts, `[[`, "raster"), paste0("ERA5 ", variable))
  parts
}

era5_land_accumulated_variables <- function() {
  c("ssr", "tp")
}

era5_land_negative_increment_tolerance <- function(variable) {
  switch(
    variable,
    ssr = 10,
    tp = 1e-7,
    0
  )
}

deaccumulate_era5_land_hourly <- function(
  raster,
  time_utc,
  variable,
  required_time_utc,
  negative_tolerance = era5_land_negative_increment_tolerance(variable)
) {
  if (!variable %in% era5_land_accumulated_variables()) {
    stop("ERA5-Land de-accumulation is only defined for ssr and tp.")
  }
  if (terra::nlyr(raster) != length(time_utc)) {
    stop("ERA5-Land ", variable, " raster/time length mismatch before de-accumulation.")
  }
  time_utc <- as.POSIXct(as.numeric(time_utc), origin = "1970-01-01", tz = "UTC")
  required_time_utc <- as.POSIXct(
    as.numeric(required_time_utc),
    origin = "1970-01-01",
    tz = "UTC"
  )
  if (anyNA(time_utc) || anyNA(required_time_utc)) {
    stop("ERA5-Land ", variable, " de-accumulation received missing UTC timestamps.")
  }
  order_idx <- order(time_utc)
  raster <- raster[[order_idx]]
  time_utc <- time_utc[order_idx]
  if (anyDuplicated(time_utc)) {
    stop("ERA5-Land ", variable, " contains duplicated UTC timestamps before de-accumulation.")
  }
  if (anyDuplicated(required_time_utc)) {
    stop("ERA5-Land ", variable, " de-accumulation received duplicated required timestamps.")
  }

  available <- as.numeric(time_utc)
  required <- as.numeric(required_time_utc)
  required_idx <- match(required, available)
  if (anyNA(required_idx)) {
    missing <- as.POSIXct(required[is.na(required_idx)], origin = "1970-01-01", tz = "UTC")
    stop("ERA5-Land ", variable, " lacks required UTC timestamp(s): ", paste(missing, collapse = ", "))
  }

  hour_utc <- as.integer(format(required_time_utc, tz = "UTC", format = "%H"))
  forecast_reset <- hour_utc == 1L
  previous <- required - 3600
  previous_idx <- match(previous, available)
  needs_previous <- !forecast_reset
  if (anyNA(previous_idx[needs_previous])) {
    missing <- as.POSIXct(
      previous[needs_previous][is.na(previous_idx[needs_previous])],
      origin = "1970-01-01",
      tz = "UTC"
    )
    stop(
      "ERA5-Land ", variable,
      " cannot de-accumulate required timestamp(s); missing previous UTC hour(s): ",
      paste(missing, collapse = ", ")
    )
  }

  increments <- vector("list", length(required))
  for (i in seq_along(required)) {
    increment <- if (forecast_reset[[i]]) {
      raster[[required_idx[[i]]]]
    } else {
      raster[[required_idx[[i]]]] - raster[[previous_idx[[i]]]]
    }
    min_value <- suppressWarnings(as.numeric(terra::global(
      increment,
      fun = "min",
      na.rm = TRUE
    )[[1L, 1L]]))
    if (is.finite(min_value) && min_value < -negative_tolerance) {
      stop(
        "ERA5-Land ", variable, " de-accumulation produced a negative hourly increment ",
        "below tolerance at UTC ", required_time_utc[[i]], ": ", min_value
      )
    }
    if (is.finite(min_value) && min_value < 0) {
      increment <- terra::ifel(increment < 0, 0, increment)
    }
    increments[[i]] <- increment
  }

  out <- do.call(c, increments)
  names(out) <- paste0(variable, "_", format(required_time_utc, tz = "UTC", "%Y%m%d%H"))
  list(raster = out, time = required_time_utc)
}

aggregate_era5_land_accumulated_kst_daily <- function(
  rasters,
  variable,
  month = NULL,
  target_dates = NULL
) {
  if (!variable %in% era5_land_accumulated_variables()) {
    stop("ERA5-Land accumulated daily aggregation is only defined for ssr and tp.")
  }
  if (is.null(target_dates)) {
    if (is.null(month)) {
      stop("ERA5-Land accumulated daily aggregation requires month or target_dates.")
    }
    target_dates <- daily_month_dates(month)
  }
  target_dates <- sort(unique(as.Date(target_dates)))
  if (length(target_dates) < 1L || anyNA(target_dates)) {
    stop("ERA5-Land accumulated daily aggregation target dates are invalid.")
  }

  parts <- era5_hourly_variable_parts(rasters, variable)
  raster <- do.call(c, lapply(parts, `[[`, "raster"))
  time_utc <- do.call(c, lapply(parts, `[[`, "time"))
  order_idx <- order(time_utc)
  raster <- raster[[order_idx]]
  time_utc <- time_utc[order_idx]
  if (anyDuplicated(time_utc)) {
    stop("ERA5-Land ", variable, " contains duplicated UTC timestamps across source files.")
  }

  expected_time <- unlist(lapply(target_dates, function(date) {
    midnight_kst <- as.POSIXct(paste(date, "00:00:00"), tz = "Asia/Seoul")
    as.numeric(midnight_kst) + 0:23 * 3600
  }), use.names = FALSE)
  deaccumulated <- deaccumulate_era5_land_hourly(
    raster = raster,
    time_utc = time_utc,
    variable = variable,
    required_time_utc = expected_time
  )
  raster <- deaccumulated$raster
  time_utc <- deaccumulated$time
  date_kst <- as.Date(time_utc, tz = "Asia/Seoul")

  actual_time <- as.numeric(time_utc)
  if (!setequal(actual_time, expected_time) || length(actual_time) != length(expected_time)) {
    missing <- as.POSIXct(setdiff(expected_time, actual_time), origin = "1970-01-01", tz = "UTC")
    extra <- as.POSIXct(setdiff(actual_time, expected_time), origin = "1970-01-01", tz = "UTC")
    stop(
      "ERA5-Land ", variable, " de-accumulated valid_time does not cover exact KST days; missing UTC=",
      paste(missing, collapse = ", "), "; extra UTC=", paste(extra, collapse = ", ")
    )
  }
  coverage <- data.frame(date = target_dates) |>
    dplyr::left_join(
      tibble::tibble(date = date_kst, time_utc = time_utc) |>
        dplyr::group_by(date) |>
        dplyr::summarise(n_hours = dplyr::n_distinct(time_utc), .groups = "drop"),
      by = "date"
    )
  if (anyNA(coverage$n_hours) || any(coverage$n_hours != 24L)) {
    bad <- coverage$date[is.na(coverage$n_hours) | coverage$n_hours != 24L]
    stop("ERA5-Land ", variable, " lacks 24 de-accumulated valid_time hours for KST date(s): ", paste(bad, collapse = ", "))
  }
  index <- match(date_kst, target_dates)
  valid_count <- terra::tapp(!is.na(raster), index = index, fun = "sum", na.rm = TRUE)
  daily <- terra::tapp(raster, index = index, fun = "sum", na.rm = TRUE)
  daily <- terra::ifel(valid_count == 24L, daily, NA)
  names(daily) <- format(target_dates, "%Y-%m-%d")
  coverage$utc_start <- as.POSIXct(
    vapply(target_dates, function(date) {
      as.numeric(as.POSIXct(paste(date, "00:00:00"), tz = "Asia/Seoul"))
    }, numeric(1)),
    origin = "1970-01-01",
    tz = "UTC"
  )
  coverage$utc_end <- coverage$utc_start + 23 * 3600
  attr(daily, "hourly_coverage") <- coverage
  attr(daily, "time_contract") <- "actual_valid_time UTC de-accumulated by ERA5-Land forecast cycle -> Asia/Seoul; exactly 24 hours and 24 valid cell values"
  daily
}

aggregate_era5_kst_daily <- function(
  rasters,
  variable,
  month = NULL,
  fun,
  target_dates = NULL
) {
  if (!fun %in% c("mean", "sum")) {
    stop("ERA5 daily aggregation must be mean or sum.")
  }
  if (is.null(target_dates)) {
    if (is.null(month)) {
      stop("ERA5 daily aggregation requires month or target_dates.")
    }
    target_dates <- daily_month_dates(month)
  }
  target_dates <- sort(unique(as.Date(target_dates)))
  if (length(target_dates) < 1L || anyNA(target_dates)) {
    stop("ERA5 daily aggregation target dates are invalid.")
  }
  parts <- era5_hourly_variable_parts(rasters, variable)
  raster <- do.call(c, lapply(parts, `[[`, "raster"))
  time_utc <- do.call(c, lapply(parts, `[[`, "time"))
  order_idx <- order(time_utc)
  raster <- raster[[order_idx]]
  time_utc <- time_utc[order_idx]
  if (anyDuplicated(time_utc)) {
    stop("ERA5 ", variable, " contains duplicated UTC timestamps across source files.")
  }
  date_kst <- as.Date(time_utc, tz = "Asia/Seoul")
  keep <- date_kst %in% target_dates
  raster <- raster[[which(keep)]]
  time_utc <- time_utc[keep]
  date_kst <- date_kst[keep]

  expected_time <- unlist(lapply(target_dates, function(date) {
    midnight_kst <- as.POSIXct(paste(date, "00:00:00"), tz = "Asia/Seoul")
    as.numeric(midnight_kst) + 0:23 * 3600
  }), use.names = FALSE)
  actual_time <- as.numeric(time_utc)
  if (!setequal(actual_time, expected_time) || length(actual_time) != length(expected_time)) {
    missing <- as.POSIXct(setdiff(expected_time, actual_time), origin = "1970-01-01", tz = "UTC")
    extra <- as.POSIXct(setdiff(actual_time, expected_time), origin = "1970-01-01", tz = "UTC")
    stop(
      "ERA5 ", variable, " actual valid_time does not cover exact KST days; missing UTC=",
      paste(missing, collapse = ", "), "; extra UTC=", paste(extra, collapse = ", ")
    )
  }
  coverage <- data.frame(date = target_dates) |>
    dplyr::left_join(
      tibble::tibble(date = date_kst, time_utc = time_utc) |>
        dplyr::group_by(date) |>
        dplyr::summarise(n_hours = dplyr::n_distinct(time_utc), .groups = "drop"),
      by = "date"
    )
  if (anyNA(coverage$n_hours) || any(coverage$n_hours != 24L)) {
    bad <- coverage$date[is.na(coverage$n_hours) | coverage$n_hours != 24L]
    stop("ERA5 ", variable, " lacks 24 actual valid_time hours for KST date(s): ", paste(bad, collapse = ", "))
  }
  index <- match(date_kst, target_dates)
  valid_count <- terra::tapp(!is.na(raster), index = index, fun = "sum", na.rm = TRUE)
  daily <- terra::tapp(raster, index = index, fun = fun, na.rm = TRUE)
  daily <- terra::ifel(valid_count == 24L, daily, NA)
  names(daily) <- format(target_dates, "%Y-%m-%d")
  coverage$utc_start <- as.POSIXct(
    vapply(target_dates, function(date) {
      as.numeric(as.POSIXct(paste(date, "00:00:00"), tz = "Asia/Seoul"))
    }, numeric(1)),
    origin = "1970-01-01",
    tz = "UTC"
  )
  coverage$utc_end <- coverage$utc_start + 23 * 3600
  attr(daily, "hourly_coverage") <- coverage
  attr(daily, "time_contract") <- "actual_valid_time UTC -> Asia/Seoul; exactly 24 hours and 24 valid cell values"
  daily
}

build_canonical_blh_daily_raster <- function(target_dates, era5_dir, source_spec = NULL) {
  target_dates <- sort(unique(as.Date(target_dates)))
  files <- canonical_blh_hourly_files(era5_dir, target_dates)
  rasters <- lapply(files, terra::rast)
  if (!is.null(source_spec) && !isTRUE(terra::compareGeom(
    daily_raster_from_spec(source_spec),
    rasters[[1L]][[1L]],
    stopOnError = FALSE
  ))) {
    stop("BLH native geometry differs from the canonical source spec.")
  }
  daily <- aggregate_era5_kst_daily(
    rasters = rasters,
    variable = "blh",
    fun = "mean",
    target_dates = target_dates
  )
  list(
    raster = daily,
    files = files,
    coverage = attr(daily, "hourly_coverage"),
    time_contract = attr(daily, "time_contract")
  )
}

build_blh_yearly_from_daily_cubes <- function(
  cubes,
  year,
  output_root = file.path("daehoon", "data", "era5_blh_processed_kst")
) {
  build_yearly_raster_from_daily_cubes(
    cubes = cubes,
    source = "blh",
    year = year,
    output_root = output_root,
    output_stem = "era5_blh_yearly"
  )
}
