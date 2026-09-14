source("R/daily_static_missing_values.R")

january_smoke_dates <- function() seq(as.Date("2023-01-01"), as.Date("2023-01-31"), by = "day")

assert_january_smoke_dates <- function(dates) {
  if (!inherits(dates, "Date") || !identical(dates, january_smoke_dates())) stop("Expected exactly 31 ordered January 2023 dates.")
  invisible(TRUE)
}

january_smoke_raster_index <- function() {
  out <- expand.grid(date = january_smoke_dates(), pollutant = c("PM10", "PM25"), stringsAsFactors = FALSE)
  stopifnot(nrow(out) == 62L, !anyDuplicated(out))
  out
}

assert_january_registry <- function(registry, expected_chunks = 593L) {
  daily_assert_keys(registry, "chunk_id")
  daily_assert_keys(registry, "contract_branch")
  if (nrow(registry) != expected_chunks || anyNA(registry$month) || any(registry$month != "2023-01")) {
    stop("January contract count/month differs from registry contract.")
  }
  if (anyNA(registry$cells) || any(registry$cells < 1)) stop("Invalid registered cell counts.")
  invisible(TRUE)
}

assert_national_cells <- function(cells, registered_cells) {
  if (anyNA(cells) || anyDuplicated(cells)) stop("Duplicate/missing global cell identity; overlaps cannot be averaged.")
  if (anyNA(registered_cells) || anyDuplicated(registered_cells) || !setequal(cells, registered_cells)) {
    stop("Missing/unexpected registered global cells.")
  }
  invisible(TRUE)
}

national_static_chunk <- function(annual, grid, chunk_id, template, annual_branch) {
  validate_daily_grid_chunk(annual, chunk_id, grid, template)
  idx <- match(grid$gid, annual$gid)
  for (column in c("gid", "x", "y", "layer")) {
    if (!identical(as.numeric(annual[[column]][idx]), as.numeric(grid[[column]]))) stop("Annual coordinates/metadata must match exactly.")
  }
  if (anyNA(annual$year) || any(annual$year != 2023L)) stop("Annual static year must be 2023.")
  metadata <- c("gid", "x", "y", "layer", "year",
    intersect(c("chunk_id", "grid_cell_id", "grid_template_id"), names(annual)))
  imported <- normalize_daily_model_missing_values(
    as.data.frame(annual[c(metadata, daily_static_predictor_terms())]), chunk_id, annual_branch)
  out <- imported$data
  if (!identical(names(out)[names(out) %in% daily_static_predictor_terms()], daily_static_predictor_terms())) stop("Static schema mismatch.")
  daily_assert_no_forbidden(out)
  attr(out, "static_normalization") <- imported$audit
  out
}

new_january_smoke_directory <- function(path) {
  if (length(path) != 1L || !grepl("^daehoon/data/daily_xgb_national_smoke/year=2023/month=01/run=[A-Za-z0-9_-]+$", path)) {
    stop("Invalid January nonproduction output path.")
  }
  if (file.exists(path)) stop("Output directory exists; refusing overwrite.")
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  if (!dir.create(path)) stop("Cannot create exclusive run directory.")
  path
}

read_national_target_catalog <- function() {
  meta <- utils::read.delim("_targets/meta/meta", sep = "|", quote = "", colClasses = "character")
  read_meta <- targets::tar_meta(store = "_targets")
  children <- function(parent) {
    row <- meta[meta$name == parent, ]
    if (nrow(row) != 1L || !nzchar(row$children)) stop("Missing target parent: ", parent)
    strsplit(row$children, "*", fixed = TRUE)[[1L]]
  }
  formats <- stats::setNames(meta$format, meta$name)
  errors <- stats::setNames(meta$error, meta$name)
  read <- function(name) {
    if (is.na(formats[[name]]) || nzchar(errors[[name]])) stop("Missing/failed stored input: ", name)
    targets::tar_read_raw(name, meta = read_meta, store = "_targets")
  }
  list(meta = meta, children = children, read = read)
}

build_january_registry <- function(catalog) {
  selected <- list()
  for (name in catalog$children("df_feat_grid_merged_daily")) {
    contract <- catalog$read(name)
    if (!inherits(contract, "daily_grid_month_contract")) stop("Invalid stored month contract.")
    if (!identical(contract$month, "2023-01")) next
    if (!identical(contract$predictor_terms, daily_predictor_terms()) ||
        !identical(contract$schema_version, daily_native_schema_version())) stop("Daily source schema mismatch.")
    manifest <- validate_grid_daily_cell_map(contract$cell_map_file, contract$sources)
    if (!identical(manifest$chunk_id, contract$chunk_id)) stop("Cell map/contract identity mismatch.")
    selected[[length(selected) + 1L]] <- data.frame(chunk_id = contract$chunk_id,
      month = contract$month, cells = manifest$n_points, contract_branch = name,
      cell_map = grid_daily_cell_map_file(contract$cell_map_file))
  }
  registry <- do.call(rbind, selected)
  assert_january_registry(registry)
  registry[order(registry$chunk_id), ]
}

audit_january_registered_grid <- function(registry, catalog, progress) {
  assert_january_registry(registry)
  template <- daily_canonical_template()
  reference <- "outputs/prediction_maps/rasters/PM10/2023_PM10.tif"
  if (!file.exists(reference)) stop("Missing canonical annual reference raster.")
  assert_daily_raster_geometry(terra::rast(reference), template)
  names <- catalog$children("list_pred_calc_grid")
  if (length(names) != 593L) stop("Registered grid must contain 593 chunks.")
  # A byte occupancy map is bounded by the fixed template, not January rows.
  seen <- raw(terra::ncell(template))
  matched <- character()
  registry$grid_branch <- rep(NA_character_, nrow(registry))
  for (name in names) {
    grid_sf <- catalog$read(name)
    if (!inherits(grid_sf, "sf") || !isTRUE(sf::st_crs(grid_sf) == sf::st_crs(5179))) stop("Registered grid CRS mismatch.")
    id <- daily_grid_chunk_key(grid_sf)
    idx <- match(id, registry$chunk_id)
    if (is.na(idx) || id %in% matched) stop("Missing/duplicate registered chunk identity.")
    grid <- as.data.frame(sf::st_drop_geometry(grid_sf))
    daily_assert_keys(grid, "gid")
    contract <- catalog$read(registry$contract_branch[idx])
    map <- read_grid_daily_cell_map(contract$cell_map_file)
    if (nrow(grid) != registry$cells[idx] || !identical(grid$gid, map$gid)) stop("Registered grid/cell map mismatch.")
    progress$chunk_id <- id
    progress$grid_branch <- name
    progress$checked_chunks <- length(matched)
    progress$rows_in_chunk <- nrow(grid)
    progress$coordinate_sample <- head(grid[c("gid", "x", "y")], 3L)
    cells <- daily_global_cell_id(grid, template)
    if (anyDuplicated(cells) || any(seen[cells] != as.raw(0))) stop("Duplicate global cells across chunks; overlap averaging forbidden.")
    seen[cells] <- as.raw(1)
    matched <- c(matched, id)
    progress$checked_chunks <- length(matched)
    registry$grid_branch[idx] <- name
    if (length(matched) %% 50L == 0L) message("Static lattice audited: ", length(matched), "/593")
  }
  if (!setequal(matched, registry$chunk_id) || sum(as.integer(seen)) != sum(registry$cells)) stop("Missing registered national cells.")
  registry
}

audit_january_annual_mapping <- function(registry, catalog, progress, evidence_dir, workers = 1L) {
  if (!workers %in% c(1L, 40L)) stop("Audit workers must be 1 or explicitly authorized 40.")
  assert_january_registry(registry)
  daily_assert_keys(registry, "grid_branch")
  years <- catalog$read("int_years_spatial")
  annual_names <- catalog$children("df_feat_grid_merged")
  if (sum(years == 2023L) != 1L || length(annual_names) != nrow(registry) * length(years)) stop("Annual candidate inventory mismatch.")
  # Stored cross ordering supplies candidates only. The published mapping is
  # established from each object's year and exact chunk coordinates, not offsets.
  candidates <- annual_names[(seq_along(annual_names) - 1L) %% length(years) + 1L == which(years == 2023L)]
  registry$annual_branch <- rep(NA_character_, nrow(registry))
  registry$static_missing <- rep(NA_real_, nrow(registry))
  template <- daily_canonical_template()
  audits <- list()
  expected <- read.csv("daehoon/data/daily_xgb_static_nan_audit/year=2023/run=provenance_001/branch_summary.csv")
  if (!dir.exists(evidence_dir) || file.exists(file.path(evidence_dir, "normalization_by_branch.csv"))) stop("Invalid/existing normalization evidence directory.")
  if (workers == 40L) {
    source("R/daily_static_parallel_audit.R")
    audits <- parallel_january_static_audit(candidates, registry, expected, evidence_dir)
  } else for (name in candidates) {
    row <- audit_one_january_annual_branch(name, registry, catalog, expected, template)
    audits[[length(audits) + 1L]] <- row
    progress$annual_chunks_checked <- length(audits)
    progress$annual_branch <- name
    progress$chunk_id <- row$chunk_id
    if (length(audits) %% 50L == 0L) message("Annual semantic matches: ", length(audits), "/593")
    gc(FALSE)
  }
  records <- do.call(rbind, audits)
  daily_assert_keys(records, "annual_branch")
  daily_assert_keys(records, "chunk_id")
  idx <- match(registry$chunk_id, records$chunk_id)
  if (anyNA(idx)) stop("Missing annual semantic match.")
  registry$annual_branch <- records$annual_branch[idx]
  registry$static_missing <- records$normalized_na[idx]
  progress$annual_chunks_checked <- nrow(records)
  daily_assert_keys(registry, "annual_branch")
  summary <- assert_static_normalization_reconciliation(records, expected)
  utils::write.csv(records, file.path(evidence_dir, "normalization_by_branch.csv"), row.names = FALSE)
  jsonlite::write_json(summary, file.path(evidence_dir, "normalization_summary.json"), auto_unbox = TRUE, pretty = TRUE)
  registry
}

audit_one_january_annual_branch <- function(name, registry, catalog, expected, template) {
    path <- file.path("_targets/objects", name)
    file_before <- digest::digest(file = path, algo = "sha256")
    annual <- as.data.frame(catalog$read(name))
    object_before <- digest::digest(annual, algo = "sha256")
    if (anyNA(annual$year) || !identical(unique(as.integer(annual$year)), 2023L)) stop("Annual candidate is not 2023; do not infer identity from offsets.")
    spatial <- sf::st_as_sf(annual[c("gid", "x", "y", "layer")], coords = c("x", "y"), crs = 5179, remove = FALSE)
    id <- daily_grid_chunk_key(spatial)
    idx <- match(id, registry$chunk_id)
    if (is.na(idx)) stop("Annual semantic chunk matching is missing.")
    grid <- as.data.frame(sf::st_drop_geometry(catalog$read(registry$grid_branch[idx])))
    static <- national_static_chunk(annual, grid, id, template, name)
    audit <- attr(static, "static_normalization")
    row <- static_normalization_audit_row(audit)
    baseline <- expected[expected$annual_branch == name & expected$chunk_id == id, ]
    if (nrow(baseline) != 1L || row$n_rows != baseline$n_rows ||
        row$nan_converted != baseline$n_static_nan || row$affected_rows != baseline$rows_nan ||
        row$raw_na != baseline$n_static_na) stop("Normalization provenance/count drift.")
    row$annual_file_sha256_before <- file_before
    row$annual_file_sha256_after <- digest::digest(file = path, algo = "sha256")
    row$annual_object_sha256_before <- object_before
    row$annual_object_sha256_after <- digest::digest(annual, algo = "sha256")
    row$finite_sha256_before <- row$finite_sha256_after <- NA_character_
    if (audit$nan_converted > 0) {
      finite <- function(x) lapply(x[daily_static_predictor_terms()], function(v) v[is.finite(v)])
      row$finite_sha256_before <- digest::digest(finite(annual), algo = "sha256")
      row$finite_sha256_after <- digest::digest(finite(static), algo = "sha256")
      reread <- as.data.frame(catalog$read(name))
      if (!identical(annual, reread, num.eq = FALSE)) stop("Annual artifact changed on re-read.")
      rm(reread)
      if (row$finite_sha256_before != row$finite_sha256_after) stop("Finite static values changed.")
    }
    if (row$annual_file_sha256_before != row$annual_file_sha256_after ||
        row$annual_object_sha256_before != row$annual_object_sha256_after) stop("Annual artifact/object mutated.")
    row
}
