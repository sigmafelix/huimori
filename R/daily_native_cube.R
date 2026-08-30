daily_native_schema_version <- function() {
  "daily-native-v1"
}

daily_dynamic_schema <- function() {
  data.frame(
    variable = c("aod", "blh", "t2m", "u10", "v10", "sp", "ssr", "tp"),
    source = c(
      "aod", "blh", rep("era5_land", 6L)
    ),
    unit = c(
      "1", "m", "degC", "m s-1", "m s-1", "Pa", "J m-2 day-1", "m day-1"
    ),
    aggregation = c(
      "source_daily", "kst_24h_mean", rep("kst_24h_mean", 4L),
      "kst_24h_sum", "kst_24h_sum"
    ),
    storage_type = rep("float32", 8L),
    stringsAsFactors = FALSE
  )
}

daily_predictor_terms <- function() {
  daily_dynamic_schema()$variable
}

daily_aod_terms <- function() {
  "aod"
}

daily_blh_terms <- function() {
  "blh"
}

daily_era5_land_terms <- function() {
  c("t2m", "u10", "v10", "sp", "ssr", "tp")
}

daily_source_terms <- function(source) {
  switch(
    source,
    aod = daily_aod_terms(),
    blh = daily_blh_terms(),
    era5_land = daily_era5_land_terms(),
    stop("Unknown daily source: ", source)
  )
}

daily_native_artifact_root <- function(
  root = file.path("daehoon", "data", "daily_native")
) {
  dir.create(root, recursive = TRUE, showWarnings = FALSE)
  resolved <- normalizePath(root, winslash = "/", mustWork = TRUE)
  allowed <- normalizePath(file.path("daehoon", "data"), winslash = "/", mustWork = TRUE)
  if (!startsWith(paste0(resolved, "/"), paste0(allowed, "/"))) {
    stop("Daily native artifacts must resolve below daehoon/data: ", resolved)
  }
  root
}

daily_limit_nested_threads <- function() {
  vars <- c(
    OMP_NUM_THREADS = "1",
    OPENBLAS_NUM_THREADS = "1",
    MKL_NUM_THREADS = "1",
    VECLIB_MAXIMUM_THREADS = "1",
    NUMEXPR_NUM_THREADS = "1",
    GDAL_NUM_THREADS = "1"
  )
  do.call(Sys.setenv, as.list(vars))
  invisible(vars)
}

daily_cube_paths <- function(root, source, month) {
  if (!source %in% c("aod", "era5_land", "blh")) {
    stop("Unsupported daily cube source: ", source)
  }
  daily_month_dates(month)
  out_dir <- file.path(
    daily_native_artifact_root(root),
    "cubes",
    source,
    paste0("year=", substr(month, 1L, 4L)),
    paste0("month=", substr(month, 6L, 7L))
  )
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  stem <- paste0(source, "_", gsub("-", "", month), "_daily_native")
  c(
    raster = file.path(out_dir, paste0(stem, ".tif")),
    manifest = file.path(out_dir, paste0(stem, "_manifest.csv"))
  )
}

daily_cube_file <- function(cube_files) {
  files <- unname(unlist(cube_files, use.names = FALSE))
  path <- files[grepl("\\.tif$", files, ignore.case = TRUE)]
  if (length(path) != 1L || !file.exists(path)) {
    stop("Daily cube bundle must contain exactly one existing .tif file.")
  }
  path
}

daily_cube_manifest_file <- function(cube_files) {
  files <- unname(unlist(cube_files, use.names = FALSE))
  path <- files[grepl("_manifest\\.csv$", files, ignore.case = TRUE)]
  if (length(path) != 1L || !file.exists(path)) {
    stop("Daily cube bundle must contain exactly one existing manifest CSV.")
  }
  path
}

atomic_write_daily_raster <- function(raster, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  temp <- tempfile(
    pattern = paste0(".", basename(path), "_"),
    tmpdir = dirname(path),
    fileext = ".tif"
  )
  on.exit(unlink(temp, force = TRUE), add = TRUE)
  terra::writeRaster(
    raster,
    temp,
    overwrite = TRUE,
    datatype = "FLT4S",
    NAflag = -9999,
    gdal = c(
      "COMPRESS=ZSTD", "PREDICTOR=3", "TILED=YES", "BIGTIFF=IF_SAFER",
      "NUM_THREADS=1"
    )
  )
  if (!file.rename(temp, path)) {
    stop("Could not atomically install daily cube: ", path)
  }
  path
}

atomic_write_daily_csv <- function(x, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  temp <- tempfile(
    pattern = paste0(".", basename(path), "_"),
    tmpdir = dirname(path),
    fileext = ".csv"
  )
  on.exit(unlink(temp, force = TRUE), add = TRUE)
  utils::write.csv(x, temp, row.names = FALSE, na = "")
  if (!file.rename(temp, path)) {
    stop("Could not atomically install daily cube manifest: ", path)
  }
  path
}

daily_source_provenance <- function(paths) {
  paths <- normalizePath(paths, winslash = "/", mustWork = TRUE)
  info <- file.info(paths)
  paste(
    paste0(
      paths,
      "|bytes=", info$size,
      "|mtime=", format(info$mtime, "%Y-%m-%dT%H:%M:%S%z")
    ),
    collapse = ";"
  )
}

daily_raster_grid_signature <- function(x) {
  spec <- if (inherits(x, "SpatRaster")) daily_raster_spec(x) else x
  raster <- daily_raster_from_spec(spec)
  fields <- c(
    as.character(spec$nrow),
    as.character(spec$ncol),
    format(c(spec$xmin, spec$xmax, spec$ymin, spec$ymax), digits = 16L),
    terra::crs(raster, proj = TRUE)
  )
  paste(fields, collapse = "|")
}

daily_cube_manifest <- function(
  raster,
  source,
  month,
  variables,
  dates,
  source_files,
  source_available = TRUE,
  source_geometry_padded = FALSE,
  n_hours = NA_integer_
) {
  schema <- daily_dynamic_schema()
  idx <- match(variables, schema$variable)
  if (anyNA(idx)) {
    stop("Cube manifest has variables outside the canonical schema.")
  }
  if (length(variables) != terra::nlyr(raster) || length(dates) != terra::nlyr(raster)) {
    stop("Cube manifest layer metadata length mismatch.")
  }
  source_available <- rep(as.logical(source_available), length.out = length(variables))
  source_geometry_padded <- rep(
    as.logical(source_geometry_padded),
    length.out = length(variables)
  )
  n_hours <- rep(as.integer(n_hours), length.out = length(variables))
  source_files <- rep(as.character(source_files), length.out = length(variables))
  kst_start <- as.POSIXct(
    paste(as.Date(dates), "00:00:00"),
    tz = "Asia/Seoul"
  )
  utc_start <- if (source == "aod") {
    rep(NA_character_, length(dates))
  } else {
    format(kst_start, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ")
  }
  utc_end <- if (source == "aod") {
    rep(NA_character_, length(dates))
  } else {
    format(kst_start + 23L * 3600L, tz = "UTC", format = "%Y-%m-%dT%H:%M:%SZ")
  }
  resolution <- terra::res(raster)
  extent <- as.vector(terra::ext(raster))
  data.frame(
    schema_version = daily_native_schema_version(),
    source = source,
    month = month,
    layer = seq_len(terra::nlyr(raster)),
    layer_name = names(raster),
    date = format(as.Date(dates), "%Y-%m-%d"),
    variable = variables,
    unit = schema$unit[idx],
    aggregation = schema$aggregation[idx],
    storage_type = schema$storage_type[idx],
    timezone = "Asia/Seoul",
    source_available = source_available,
    source_geometry_padded = source_geometry_padded,
    n_source_hours = n_hours,
    required_valid_hours = ifelse(source == "aod", NA_integer_, 24L),
    utc_start = utc_start,
    utc_end = utc_end,
    source_provenance = source_files,
    grid_signature = daily_raster_grid_signature(raster),
    crs_wkt = terra::crs(raster),
    nrow = terra::nrow(raster),
    ncol = terra::ncol(raster),
    res_x = resolution[[1L]],
    res_y = resolution[[2L]],
    xmin = extent[[1L]],
    xmax = extent[[2L]],
    ymin = extent[[3L]],
    ymax = extent[[4L]],
    stringsAsFactors = FALSE
  )
}

write_daily_cube_bundle <- function(raster, manifest, paths) {
  atomic_write_daily_raster(raster, paths[["raster"]])
  atomic_write_daily_csv(manifest, paths[["manifest"]])
  out <- unname(paths)
  validate_daily_cube(out)
  out
}

read_daily_cube_manifest <- function(cube_files) {
  manifest <- utils::read.csv(
    daily_cube_manifest_file(cube_files),
    stringsAsFactors = FALSE,
    check.names = FALSE,
    colClasses = "character"
  )
  manifest$date <- as.Date(manifest$date)
  manifest$layer <- as.integer(manifest$layer)
  manifest$source_available <- as.logical(manifest$source_available)
  manifest$source_geometry_padded <- as.logical(manifest$source_geometry_padded)
  manifest$n_source_hours <- as.integer(manifest$n_source_hours)
  manifest$required_valid_hours <- as.integer(manifest$required_valid_hours)
  manifest$nrow <- as.integer(manifest$nrow)
  manifest$ncol <- as.integer(manifest$ncol)
  for (column in c("res_x", "res_y", "xmin", "xmax", "ymin", "ymax")) {
    manifest[[column]] <- as.numeric(manifest[[column]])
  }
  manifest
}

validate_daily_cube <- function(cube_files, source = NULL, month = NULL) {
  raster <- terra::rast(daily_cube_file(cube_files))
  manifest <- read_daily_cube_manifest(cube_files)
  required <- c(
    "schema_version", "source", "month", "layer", "layer_name", "date",
    "variable", "unit", "aggregation", "storage_type", "timezone",
    "source_available", "source_geometry_padded", "n_source_hours", "required_valid_hours",
    "utc_start", "utc_end",
    "source_provenance", "grid_signature", "crs_wkt", "nrow", "ncol", "res_x", "res_y",
    "xmin", "xmax", "ymin", "ymax"
  )
  missing <- setdiff(required, names(manifest))
  if (length(missing) > 0L) {
    stop("Daily cube manifest is missing columns: ", paste(missing, collapse = ", "))
  }
  if (nrow(manifest) != terra::nlyr(raster) ||
      !identical(manifest$layer, seq_len(nrow(manifest))) ||
      !identical(manifest$layer_name, names(raster))) {
    stop("Daily cube raster layers do not match the manifest.")
  }
  source <- if (is.null(source)) unique(manifest$source) else source
  month <- if (is.null(month)) unique(manifest$month) else month
  if (length(source) != 1L || length(month) != 1L) {
    stop("Daily cube must contain exactly one source and month.")
  }
  expected_dates <- daily_month_dates(month)
  expected_variables <- daily_source_terms(source)
  expected <- tidyr::expand_grid(
    variable = expected_variables,
    date = expected_dates
  )
  actual <- manifest[, c("variable", "date")]
  if (!identical(actual$variable, expected$variable) ||
      length(actual$date) != length(expected$date) ||
      any(actual$date != expected$date)) {
    stop("Daily cube variable/date order does not match the canonical schema.")
  }
  if (any(manifest$schema_version != daily_native_schema_version()) ||
      any(manifest$timezone != "Asia/Seoul") ||
      any(manifest$storage_type != "float32")) {
    stop("Daily cube schema version, timezone, or storage type is invalid.")
  }
  if (source != "aod" && any(manifest$n_source_hours != 24L)) {
    stop("ERA daily cube contains a KST date without 24 source timestamps.")
  }
  if (source != "aod") {
    kst_start <- as.POSIXct(
      paste(manifest$date, "00:00:00"),
      tz = "Asia/Seoul"
    )
    expected_utc_start <- format(
      kst_start,
      tz = "UTC",
      format = "%Y-%m-%dT%H:%M:%SZ"
    )
    expected_utc_end <- format(
      kst_start + 23L * 3600L,
      tz = "UTC",
      format = "%Y-%m-%dT%H:%M:%SZ"
    )
    if (!identical(manifest$utc_start, expected_utc_start) ||
        !identical(manifest$utc_end, expected_utc_end)) {
      stop("ERA daily cube UTC/KST interval metadata is invalid.")
    }
    source_month_tokens <- sub("-", "_", era5_required_utc_months(manifest$date))
    if (!all(vapply(
      source_month_tokens,
      function(token) any(grepl(token, manifest$source_provenance, fixed = TRUE)),
      logical(1)
    ))) {
      stop("ERA daily cube provenance does not cover the UTC months required by its KST dates.")
    }
  }
  schema <- daily_dynamic_schema()
  schema_idx <- match(manifest$variable, schema$variable)
  if (anyNA(schema_idx) ||
      !identical(manifest$unit, schema$unit[schema_idx]) ||
      !identical(manifest$aggregation, schema$aggregation[schema_idx])) {
    stop("Daily cube units or aggregation differ from the canonical schema.")
  }
  if (is.na(terra::crs(raster)) || terra::nrow(raster) < 1L || terra::ncol(raster) < 1L) {
    stop("Daily cube has invalid native raster geometry.")
  }
  actual_signature <- daily_raster_grid_signature(raster)
  if (length(unique(manifest$grid_signature)) != 1L ||
      unique(manifest$grid_signature) != actual_signature) {
    stop("Daily cube raster geometry differs from its manifest.")
  }
  if (source == "aod" && any(!manifest$source_available)) {
    missing_layers <- manifest$layer[!manifest$source_available]
    non_missing <- terra::global(
      !is.na(raster[[missing_layers]]),
      fun = "sum",
      na.rm = TRUE
    )[, 1L]
    if (any(non_missing != 0)) {
      stop("AOD missing source dates must be represented by all-NA layers.")
    }
  }
  invisible(manifest)
}

pad_native_raster_to_template <- function(raster, template, label) {
  if (isTRUE(terra::compareGeom(template, raster, stopOnError = FALSE))) {
    return(list(raster = raster, padded = FALSE))
  }
  same_crs <- isTRUE(terra::same.crs(template, raster))
  same_res <- isTRUE(all.equal(terra::res(template), terra::res(raster), tolerance = 1e-8))
  template_ext <- as.vector(terra::ext(template))
  raster_ext <- as.vector(terra::ext(raster))
  inside <- raster_ext[[1L]] >= template_ext[[1L]] - 1e-4 &&
    raster_ext[[2L]] <= template_ext[[2L]] + 1e-4 &&
    raster_ext[[3L]] >= template_ext[[3L]] - 1e-4 &&
    raster_ext[[4L]] <= template_ext[[4L]] + 1e-4
  if (!same_crs || !same_res || !inside) {
    stop(label, " native geometry is incompatible with the monthly template.")
  }
  padded <- terra::extend(raster, template)
  if (!isTRUE(terra::compareGeom(template, padded, stopOnError = FALSE))) {
    stop(label, " could not be NA-padded to the monthly native geometry.")
  }
  list(raster = padded, padded = TRUE)
}

build_aod_monthly_cube <- function(
  month,
  aod_dir,
  output_root
) {
  daily_limit_nested_threads()
  dates <- daily_month_dates(month)
  inventory <- canonical_aod_inventory(aod_dir, dates)
  files <- inventory$raster
  available <- inventory$available
  if (!any(available)) {
    stop("AOD month has no source raster from which to obtain native geometry: ", month)
  }
  template <- terra::rast(files[[which(available)[[1L]]]])[[1L]]
  padded <- rep(FALSE, length(dates))
  layers <- lapply(seq_along(dates), function(i) {
    if (!available[[i]]) {
      return(terra::setValues(template, NA_real_))
    }
    raster <- terra::rast(files[[i]])[[1L]]
    aligned <- pad_native_raster_to_template(
      raster,
      template,
      paste0("AOD ", dates[[i]])
    )
    padded[[i]] <<- aligned$padded
    aligned$raster
  })
  cube <- do.call(c, layers)
  names(cube) <- paste0("aod_", format(dates, "%Y%m%d"))
  provenance <- rep(NA_character_, length(files))
  provenance[available] <- vapply(
    files[available],
    daily_source_provenance,
    character(1)
  )
  manifest <- daily_cube_manifest(
    cube,
    source = "aod",
    month = month,
    variables = rep("aod", length(dates)),
    dates = dates,
    source_files = provenance,
    source_available = available,
    source_geometry_padded = padded
  )
  paths <- daily_cube_paths(output_root, "aod", month)
  out <- write_daily_cube_bundle(cube, manifest, paths)
  validate_daily_cube(out, "aod", month)
  out
}

neighbor_months <- function(month) {
  current <- as.Date(paste0(month, "-01"))
  format(
    c(
      seq(current, by = "-1 month", length.out = 2L)[[2L]],
      current,
      seq(current, by = "month", length.out = 2L)[[2L]]
    ),
    "%Y-%m"
  )
}

era5_land_archives <- function(era5_dir, month) {
  paths <- file.path(
    era5_dir,
    paste0("ERA5_Land_", sub("-", "_", neighbor_months(month)), ".nc")
  )
  missing <- paths[!file.exists(paths)]
  if (length(missing) > 0L) {
    stop("Missing adjacent ERA5-Land archive(s): ", paste(missing, collapse = ", "))
  }
  paths
}

unpack_era5_land_archives <- function(paths, temp_root) {
  dir.create(temp_root, recursive = TRUE, showWarnings = FALSE)
  vapply(seq_along(paths), function(i) {
    extract_dir <- file.path(temp_root, as.character(i))
    dir.create(extract_dir, recursive = TRUE, showWarnings = FALSE)
    status <- utils::unzip(paths[[i]], files = "data_0.nc", exdir = extract_dir)
    path <- file.path(extract_dir, "data_0.nc")
    if (length(status) < 1L || !file.exists(path)) {
      stop("Could not extract data_0.nc from ", paths[[i]], ".")
    }
    path
  }, character(1))
}

build_era5_land_monthly_cube <- function(
  month,
  era5_dir,
  output_root
) {
  daily_limit_nested_threads()
  archives <- era5_land_archives(era5_dir, month)
  root <- daily_native_artifact_root(output_root)
  temp_dir <- tempfile(
    pattern = paste0("era5_land_", gsub("-", "", month), "_"),
    tmpdir = file.path(root, ".tmp")
  )
  dir.create(dirname(temp_dir), recursive = TRUE, showWarnings = FALSE)
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE, force = TRUE), add = TRUE)
  nc_files <- unpack_era5_land_archives(archives, temp_dir)
  rasters <- lapply(nc_files, terra::rast)
  specs <- c(t2m = "mean", u10 = "mean", v10 = "mean", sp = "mean", ssr = "sum", tp = "sum")
  dates <- daily_month_dates(month)
  parts <- lapply(names(specs), function(variable) {
    value <- if (variable %in% era5_land_accumulated_variables()) {
      aggregate_era5_land_accumulated_kst_daily(rasters, variable, month)
    } else {
      aggregate_era5_kst_daily(rasters, variable, month, specs[[variable]])
    }
    if (variable == "t2m") {
      value <- value - 273.15
    }
    names(value) <- paste0(variable, "_", format(dates, "%Y%m%d"))
    value
  })
  cube <- do.call(c, parts)
  variables <- rep(names(specs), each = length(dates))
  layer_dates <- rep(dates, times = length(specs))
  provenance <- daily_source_provenance(archives)
  manifest <- daily_cube_manifest(
    cube,
    source = "era5_land",
    month = month,
    variables = variables,
    dates = layer_dates,
    source_files = provenance,
    n_hours = 24L
  )
  paths <- daily_cube_paths(root, "era5_land", month)
  out <- write_daily_cube_bundle(cube, manifest, paths)
  validate_daily_cube(out, "era5_land", month)
  out
}

build_blh_monthly_cube <- function(
  month,
  era5_dir,
  output_root
) {
  daily_limit_nested_threads()
  dates <- daily_month_dates(month)
  daily <- build_canonical_blh_daily_raster(dates, era5_dir)
  cube <- daily$raster
  names(cube) <- paste0("blh_", format(dates, "%Y%m%d"))
  manifest <- daily_cube_manifest(
    cube,
    source = "blh",
    month = month,
    variables = rep("blh", length(dates)),
    dates = dates,
    source_files = daily_source_provenance(daily$files),
    n_hours = 24L
  )
  paths <- daily_cube_paths(output_root, "blh", month)
  out <- write_daily_cube_bundle(cube, manifest, paths)
  validate_daily_cube(out, "blh", month)
  out
}

daily_raster_spec <- function(raster) {
  extent <- as.vector(terra::ext(raster))
  list(
    nrow = terra::nrow(raster),
    ncol = terra::ncol(raster),
    xmin = extent[[1L]],
    xmax = extent[[2L]],
    ymin = extent[[3L]],
    ymax = extent[[4L]],
    crs = terra::crs(raster)
  )
}

daily_raster_from_spec <- function(spec) {
  terra::rast(
    nrows = spec$nrow,
    ncols = spec$ncol,
    xmin = spec$xmin,
    xmax = spec$xmax,
    ymin = spec$ymin,
    ymax = spec$ymax,
    crs = spec$crs
  )
}

as_daily_cube_branches <- function(x, label = "daily cube branches") {
  if (is.character(x)) {
    x <- list(x)
  }
  if (!is.list(x) || length(x) < 1L) {
    stop(label, " must contain at least one cube file bundle.")
  }
  valid <- vapply(x, function(bundle) {
    files <- unname(unlist(bundle, use.names = FALSE))
    sum(grepl("\\.tif$", files, ignore.case = TRUE)) == 1L &&
      sum(grepl("_manifest\\.csv$", files, ignore.case = TRUE)) == 1L &&
      all(file.exists(files))
  }, logical(1))
  if (!all(valid)) {
    stop(label, " contains an invalid raster/manifest bundle.")
  }
  x
}

daily_cube_specs_from_branches <- function(aod, era5_land, blh) {
  inputs <- list(aod = aod, era5_land = era5_land, blh = blh)
  lapply(names(inputs), function(source) {
    branches <- as_daily_cube_branches(inputs[[source]], paste0(source, " cube branches"))
    validate_daily_cube(branches[[1L]], source = source)
    daily_raster_spec(terra::rast(daily_cube_file(branches[[1L]]))[[1L]])
  }) |>
    stats::setNames(names(inputs))
}

assert_point_input <- function(points, id_cols, label) {
  if (!inherits(points, "sf") || is.na(sf::st_crs(points))) {
    stop(label, " must be an sf point object with a valid CRS.")
  }
  missing <- setdiff(id_cols, names(points))
  if (length(missing) > 0L) {
    stop(label, " is missing ID columns: ", paste(missing, collapse = ", "))
  }
  if (any(sf::st_is_empty(points))) {
    stop(label, " contains empty geometries.")
  }
  invisible(points)
}

build_single_daily_source_cell_map <- function(points, source_spec, source, id_cols) {
  assert_point_input(points, id_cols, "Daily source-cell points")
  locations <- points |>
    dplyr::select(dplyr::all_of(id_cols)) |>
    dplyr::distinct(dplyr::across(dplyr::all_of(id_cols)), .keep_all = TRUE)
  assert_daily_unique_key(locations, id_cols, "Daily source-cell locations")
  out <- sf::st_drop_geometry(locations)
  raster <- daily_raster_from_spec(source_spec)
  chunk_size <- as.integer(Sys.getenv("HUIMORI_DAILY_CELL_MAP_CHUNK_SIZE", "50000"))
  if (length(chunk_size) != 1L || is.na(chunk_size) || chunk_size < 1L) {
    chunk_size <- 50000L
  }
  row_chunks <- split(
    seq_len(nrow(locations)),
    ceiling(seq_len(nrow(locations)) / chunk_size)
  )
  cell <- unlist(
    lapply(row_chunks, function(idx) {
      transformed <- sf::st_transform(locations[idx, ], terra::crs(raster))
      xy <- sf::st_coordinates(transformed)
      out_i <- terra::cellFromXY(raster, xy)
      gc()
      out_i
    }),
    use.names = FALSE
  )
  if (anyNA(cell)) {
    stop("Daily ", source, " cell map has points outside the native raster.")
  }
  out[[paste0(source, "_cell")]] <- as.integer(cell)
  out
}

build_daily_source_cell_map <- function(points, source_specs, id_cols) {
  daily_limit_nested_threads()
  assert_point_input(points, id_cols, "Daily source-cell points")
  locations <- points |>
    dplyr::select(dplyr::all_of(id_cols)) |>
    dplyr::distinct(dplyr::across(dplyr::all_of(id_cols)), .keep_all = TRUE)
  out <- sf::st_drop_geometry(locations)
  for (source in c("aod", "era5_land", "blh")) {
    source_map <- build_single_daily_source_cell_map(
      locations,
      source_specs[[source]],
      source,
      id_cols
    )
    out <- dplyr::left_join(out, source_map, by = id_cols)
  }
  assert_daily_unique_key(out, id_cols, "Daily source-cell map")
  attr(out, "schema_version") <- daily_native_schema_version()
  attr(out, "extraction_method") <- "simple_containing_cell"
  out
}

daily_grid_chunk_key <- function(grid) {
  bbox <- round(as.numeric(sf::st_bbox(grid)), digits = 3L)
  meta <- sf::st_drop_geometry(grid)
  key <- sprintf(
    "bbox_%s_n%d_gid_%s_%s",
    paste(bbox, collapse = "_"),
    nrow(meta),
    min(meta$gid, na.rm = TRUE),
    max(meta$gid, na.rm = TRUE)
  )
  gsub("[^A-Za-z0-9_.-]+", "_", key)
}

write_grid_daily_cell_map <- function(grid, source_specs, output_root) {
  root <- daily_native_artifact_root(output_root)
  out_dir <- file.path(root, "cell_maps", "grid")
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  stem <- daily_grid_chunk_key(grid)
  path <- file.path(out_dir, paste0(stem, ".parquet"))
  manifest_path <- file.path(out_dir, paste0(stem, "_manifest.csv"))
  existing <- c(parquet = path, manifest = manifest_path)
  if (all(file.exists(existing))) {
    reuse_manifest <- tryCatch(
      validate_grid_daily_cell_map(existing),
      error = function(e) NULL
    )
    if (!is.null(reuse_manifest) &&
        identical(reuse_manifest$chunk_id[[1L]], stem) &&
        identical(reuse_manifest$aod_grid_signature[[1L]], daily_raster_grid_signature(source_specs$aod)) &&
        identical(reuse_manifest$era5_land_grid_signature[[1L]], daily_raster_grid_signature(source_specs$era5_land)) &&
        identical(reuse_manifest$blh_grid_signature[[1L]], daily_raster_grid_signature(source_specs$blh))) {
      return(existing)
    }
  }
  map <- build_daily_source_cell_map(grid, source_specs, id_cols = "gid")
  temp <- tempfile(pattern = ".cell_map_", tmpdir = out_dir, fileext = ".parquet")
  on.exit(unlink(temp, force = TRUE), add = TRUE)
  nanoparquet::write_parquet(
    map,
    temp,
    compression = "zstd",
    metadata = c(
      schema_version = daily_native_schema_version(),
      extraction_method = "simple_containing_cell"
    )
  )
  if (!file.rename(temp, path)) {
    stop("Could not atomically install grid daily cell map: ", path)
  }
  manifest <- data.frame(
    schema_version = daily_native_schema_version(),
    extraction_method = "simple_containing_cell",
    chunk_id = stem,
    n_points = nrow(map),
    aod_grid_signature = daily_raster_grid_signature(source_specs$aod),
    era5_land_grid_signature = daily_raster_grid_signature(source_specs$era5_land),
    blh_grid_signature = daily_raster_grid_signature(source_specs$blh),
    stringsAsFactors = FALSE
  )
  atomic_write_daily_csv(manifest, manifest_path)
  c(parquet = path, manifest = manifest_path)
}

grid_daily_cell_map_file <- function(paths) {
  paths <- unname(unlist(paths, use.names = FALSE))
  path <- paths[grepl("\\.parquet$", paths, ignore.case = TRUE)]
  if (length(path) != 1L || !file.exists(path)) {
    stop("Grid daily cell-map bundle must contain one existing Parquet file.")
  }
  path
}

grid_daily_cell_map_manifest_file <- function(paths) {
  paths <- unname(unlist(paths, use.names = FALSE))
  path <- paths[grepl("_manifest\\.csv$", paths, ignore.case = TRUE)]
  if (length(path) != 1L || !file.exists(path)) {
    stop("Grid daily cell-map bundle must contain one existing manifest CSV.")
  }
  path
}

read_grid_daily_cell_map <- function(paths) {
  map <- nanoparquet::read_parquet(grid_daily_cell_map_file(paths))
  required <- c("gid", "aod_cell", "era5_land_cell", "blh_cell")
  missing <- setdiff(required, names(map))
  if (length(missing) > 0L) {
    stop("Grid daily cell map is missing columns: ", paste(missing, collapse = ", "))
  }
  map
}

validate_grid_daily_cell_map <- function(paths, source_contracts = NULL) {
  map <- read_grid_daily_cell_map(paths)
  manifest <- utils::read.csv(
    grid_daily_cell_map_manifest_file(paths),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
  if (!"chunk_id" %in% names(manifest) || !nzchar(manifest$chunk_id[[1L]]) ||
      nrow(manifest) != 1L || manifest$n_points[[1L]] != nrow(map) ||
      manifest$schema_version[[1L]] != daily_native_schema_version() ||
      manifest$extraction_method[[1L]] != "simple_containing_cell") {
    stop("Grid daily cell-map manifest is invalid.")
  }
  if (!is.null(source_contracts)) {
    for (source in names(source_contracts)) {
      cube_manifest <- read_daily_cube_manifest(source_contracts[[source]]$cube_files)
      expected <- unique(cube_manifest$grid_signature)
      actual <- manifest[[paste0(source, "_grid_signature")]][[1L]]
      if (length(expected) != 1L || !identical(expected, actual)) {
        stop("Grid cell map and ", source, " cube native geometry differ.")
      }
    }
  }
  invisible(manifest)
}

extract_daily_cube_keys <- function(keys, cell_map, cube_files, source, id_cols) {
  daily_limit_nested_threads()
  manifest <- validate_daily_cube(cube_files, source = source)
  month <- unique(manifest$month)
  assert_daily_branch(keys, month, paste0(source, " daily extraction keys"), id_cols = id_cols)
  assert_daily_unique_key(cell_map, id_cols, paste0(source, " daily cell map"))
  cell_col <- paste0(source, "_cell")
  if (!cell_col %in% names(cell_map)) {
    stop("Daily cell map is missing ", cell_col, ".")
  }
  key_data <- daily_key_frame(keys, id_cols) |>
    dplyr::left_join(cell_map[, c(id_cols, cell_col), drop = FALSE], by = id_cols)
  if (anyNA(key_data[[cell_col]])) {
    stop(source, " extraction has keys without a source-cell mapping.")
  }
  raster <- terra::rast(daily_cube_file(cube_files))
  out <- key_data[, c(id_cols, "date"), drop = FALSE]
  for (variable in daily_source_terms(source)) {
    rows <- which(manifest$variable == variable)
    values <- terra::extract(
      raster[[manifest$layer[rows]]],
      unique(key_data[[cell_col]])
    )
    cell_levels <- unique(key_data[[cell_col]])
    entity_row <- match(key_data[[cell_col]], cell_levels)
    date_col <- match(key_data$date, manifest$date[rows])
    if (anyNA(date_col)) {
      stop(source, " extraction keys contain dates outside cube month.")
    }
    matrix_values <- as.matrix(values)
    out[[variable]] <- as.numeric(matrix_values[cbind(entity_row, date_col)])
  }
  assert_daily_branch(
    out,
    month,
    paste0(source, " daily point extraction"),
    required_cols = daily_source_terms(source),
    id_cols = id_cols
  )
  assert_same_daily_keys(out, keys, paste0(source, " daily point extraction"), id_cols)
  attr(out, "schema_version") <- daily_native_schema_version()
  attr(out, "extraction_method") <- "simple_containing_cell"
  attr(out, "cube_files") <- unname(cube_files)
  out
}

extract_daily_source_points <- function(base, cube_files, source, id_cols) {
  assert_daily_branch(
    base,
    unique(read_daily_cube_manifest(cube_files)$month),
    paste0(source, " point base"),
    id_cols = id_cols
  )
  specs <- list()
  specs[[source]] <- daily_raster_spec(terra::rast(daily_cube_file(cube_files))[[1L]])
  cell_map <- build_single_daily_source_cell_map(
    points = base,
    source_spec = specs[[source]],
    source = source,
    id_cols = id_cols
  )
  extract_daily_cube_keys(base, cell_map, cube_files, source, id_cols)
}

merge_daily_dynamic_features <- function(base, aod, era5_land, blh, month, id_cols) {
  daily_limit_nested_threads()
  specs <- list(
    aod = list(data = aod, terms = daily_aod_terms()),
    era5_land = list(data = era5_land, terms = daily_era5_land_terms()),
    blh = list(data = blh, terms = daily_blh_terms())
  )
  assert_daily_branch(base, month, "Daily dynamic base", id_cols = id_cols)
  out <- if (inherits(base, "sf")) sf::st_drop_geometry(base) else base
  out$date <- as.Date(out$date)
  base_n <- nrow(out)
  for (source in names(specs)) {
    spec <- specs[[source]]
    assert_daily_branch(
      spec$data,
      month,
      paste0(source, " daily feature"),
      required_cols = spec$terms,
      id_cols = id_cols
    )
    assert_same_daily_keys(spec$data, base, paste0(source, " daily feature"), id_cols)
    out <- dplyr::left_join(
      out,
      spec$data[, c(id_cols, "date", spec$terms), drop = FALSE],
      by = c(id_cols, "date")
    )
    if (nrow(out) != base_n) {
      stop(source, " daily join changed the base row count.")
    }
  }
  predictors <- daily_predictor_terms()
  metadata <- setdiff(names(out), predictors)
  out <- out[, c(metadata, predictors), drop = FALSE]
  if (!identical(names(out)[names(out) %in% predictors], predictors)) {
    stop("Daily merged predictor names/order differ from the canonical schema.")
  }
  forbidden <- grep("_[0-9]+m$|^(aod|blh)_yearly", names(out), value = TRUE)
  if (length(forbidden) > 0L) {
    stop("Daily merged table contains retired buffer/yearly columns: ", paste(forbidden, collapse = ", "))
  }
  assert_daily_branch(
    out,
    month,
    "Daily merged output",
    required_cols = predictors,
    id_cols = id_cols
  )
  attr(out, "predictor_terms") <- predictors
  attr(out, "schema_version") <- daily_native_schema_version()
  attr(out, "extraction_method") <- "simple_containing_cell"
  out
}

merge_correct_daily_month <- function(base, aod, era5_land, blh, month) {
  merge_daily_dynamic_features(
    base = base,
    aod = aod,
    era5_land = era5_land,
    blh = blh,
    month = month,
    id_cols = daily_key_cols()[1:2]
  )
}

new_daily_grid_source_contract <- function(cube_files, source, month) {
  validate_daily_cube(cube_files, source, month)
  structure(
    list(
      source = source,
      month = month,
      cube_files = unname(cube_files),
      terms = daily_source_terms(source),
      schema_version = daily_native_schema_version(),
      extraction_method = "simple_containing_cell"
    ),
    class = "daily_grid_source_contract"
  )
}

new_daily_grid_month_contract <- function(cell_map_file, aod, era5_land, blh) {
  contracts <- list(aod = aod, era5_land = era5_land, blh = blh)
  months <- vapply(contracts, `[[`, character(1), "month")
  if (length(unique(months)) != 1L) {
    stop("Grid daily source contracts do not refer to the same month.")
  }
  cell_manifest <- validate_grid_daily_cell_map(cell_map_file, contracts)
  structure(
    list(
      month = months[[1L]],
      chunk_id = cell_manifest$chunk_id[[1L]],
      cell_map_file = cell_map_file,
      sources = contracts,
      predictor_terms = daily_predictor_terms(),
      schema_version = daily_native_schema_version(),
      extraction_method = "simple_containing_cell"
    ),
    class = "daily_grid_month_contract"
  )
}

materialize_grid_daily_contract <- function(contract, dates = NULL) {
  if (!inherits(contract, "daily_grid_month_contract")) {
    stop("Expected a daily_grid_month_contract.")
  }
  dates <- as.Date(if (is.null(dates)) daily_month_dates(contract$month) else dates)
  expected <- daily_month_dates(contract$month)
  if (anyNA(dates) || any(!dates %in% expected) || anyDuplicated(dates)) {
    stop("Requested grid dates must be unique dates in contract month.")
  }
  cell_map <- read_grid_daily_cell_map(contract$cell_map_file)
  keys <- tidyr::expand_grid(gid = cell_map$gid, date = dates)
  parts <- lapply(contract$sources, function(source_contract) {
    extract_daily_cube_keys(
      keys = keys,
      cell_map = cell_map,
      cube_files = source_contract$cube_files,
      source = source_contract$source,
      id_cols = "gid"
    )
  })
  out <- merge_daily_dynamic_features(
    base = keys,
    aod = parts$aod,
    era5_land = parts$era5_land,
    blh = parts$blh,
    month = contract$month,
    id_cols = "gid"
  )
  attr(out, "streaming_contract") <- TRUE
  out
}
