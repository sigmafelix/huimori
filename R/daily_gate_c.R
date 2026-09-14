gate_c_reference_root <- function() "daehoon/data/daily_xgb_gate_b/year=2023/month=01/run=representative_month_001"

require_gate_c_preconditions <- function() {
  require_gate_b_precondition(gate_b_root())
  result <- jsonlite::read_json(file.path(gate_c_reference_root(), "result.json"))
  if (!identical(result$status, "PASS")) stop("Final Gate B PASS required.")
  invisible(TRUE)
}

select_gate_c_chunks <- function(registry) {
  assert_january_registry(registry)
  out <- registry[order(registry$chunk_id, method = "radix")[seq_len(10L)], , drop = FALSE]
  out$class <- sprintf("chunk_%02d", seq_len(10L)); rownames(out) <- NULL
  out
}

new_gate_c_directory <- function(path) {
  if (!grepl("^daehoon/data/daily_xgb_gate_c/year=2023/month=01/run=[A-Za-z0-9_-]+$", path)) stop("Invalid Gate C path.")
  if (file.exists(path)) stop("Gate C output exists.")
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  if (!dir.create(path)) stop("Cannot create Gate C directory.")
}

gate_c_assert_date <- function(date) {
  if (!inherits(date, "Date") || length(date) != 1L || is.na(date) ||
      !date %in% january_smoke_dates()) stop("Expected one January 2023 date.")
}

gate_c_file_state <- function(paths) {
  info <- file.info(paths)
  if (anyNA(info$size)) stop("Prepared source missing.")
  data.frame(path = paths, size = info$size, mtime = as.numeric(info$mtime), row.names = NULL)
}

# Immutable, plain R vectors: no data.table/reference mutation is used. All
# spatial identities and index mappings are proved once before sealing state.
prepare_daily_static_chunk_for_prediction <- function(annual, grid, row, contract, template) {
  if (!identical(class(annual), "data.frame") || !identical(class(grid), "data.frame")) stop("Plain data.frame required.")
  if (!inherits(contract, "daily_grid_month_contract") || contract$month != "2023-01" ||
      !identical(contract$chunk_id, row$chunk_id)) stop("Chunk/month contract mismatch.")
  cell_manifest <- validate_grid_daily_cell_map(contract$cell_map_file, contract$sources)
  if (!identical(cell_manifest$chunk_id, row$chunk_id)) stop("Cell map chunk mismatch.")
  map <- read_grid_daily_cell_map(contract$cell_map_file)
  daily_assert_keys(map, "gid")
  imported <- gate_b_static_import(annual, grid, row, template)
  static <- imported$data
  idx <- match(map$gid, static$gid)
  if (nrow(map) != row$cells || anyNA(idx) || anyDuplicated(idx) || length(idx) != nrow(static)) stop("Static mapping mismatch.")
  # The accepted join defines column order and all model/grid invariants.
  fixture <- data.frame(gid = map$gid, date = rep(as.Date("2023-01-01"), nrow(map)))
  for (nm in daily_predictor_terms()) fixture[[nm]] <- NA_real_
  blueprint <- join_grid_daily_annual_features(fixture, static, row$chunk_id, grid, template, month = "2023-01")
  sources <- lapply(contract$sources, function(src) {
    manifest <- validate_daily_cube(src$cube_files, source = src$source)
    if (!identical(unique(manifest$month), "2023-01")) stop("Wrong cube month.")
    cells <- map[[paste0(src$source, "_cell")]]
    if (!is.numeric(cells) || anyNA(cells) || any(!is.finite(cells))) stop("Source-cell mapping missing.")
    levels <- unique(cells)
    list(source = src$source, files = src$cube_files, manifest = manifest,
         cells = levels, index = match(cells, levels))
  })
  if (!setequal(names(sources), c("aod", "era5_land", "blh"))) stop("Dynamic sources differ.")
  paths <- unique(c(contract$cell_map_file, unlist(lapply(sources, `[[`, "files"))))
  state <- new.env(parent = emptyenv())
  state$blueprint <- blueprint
  state$gid <- map$gid
  state$static_idx <- idx
  state$sources <- sources
  state$file_state <- gate_c_file_state(paths)
  state$row <- row
  state$audit <- imported$audit
  state$import_times <- imported[c("normalization_seconds", "import_validation_seconds")]
  state$static_missing <- sum(rowSums(is.na(blueprint[daily_static_predictor_terms()])) > 0L)
  state$schema <- daily_model_schema()
  state$invariant_validation_count <- 1L
  class(state) <- "prepared_daily_static_chunk"
  lockEnvironment(state, bindings = TRUE)
  state
}

gate_c_assert_prepared <- function(prepared) {
  if (!inherits(prepared, "prepared_daily_static_chunk") || !is.environment(prepared) ||
      !environmentIsLocked(prepared) || !all(vapply(ls(prepared), bindingIsLocked, logical(1), env = prepared)) ||
      !identical(prepared$schema, daily_model_schema())) stop("Unvalidated prepared chunk.")
}

# Keep exactly the reference terra extraction (including all monthly layers).
# Only the repeated R key joins/date-vector matching are replaced by validated
# integer mappings. No raster resampling, source cache, or NA policy changes.
materialize_gate_c_date <- function(prepared, date) {
  gate_c_assert_prepared(prepared); gate_c_assert_date(date)
  if (!identical(prepared$file_state, gate_c_file_state(prepared$file_state$path))) stop("Prepared source changed.")
  out <- data.frame(gid = prepared$gid, date = rep(date, length(prepared$gid)))
  timings <- list()
  for (name in names(prepared$sources)) {
    start <- proc.time()["elapsed"]
    src <- prepared$sources[[name]]; manifest <- src$manifest
    raster <- terra::rast(daily_cube_file(src$files))
    for (variable in daily_source_terms(src$source)) {
      rows <- which(manifest$variable == variable)
      column <- match(date, manifest$date[rows])
      if (is.na(column)) stop("Date absent from cube.")
      values <- as.matrix(terra::extract(raster[[manifest$layer[rows]]], src$cells))
      out[[variable]] <- as.numeric(values[cbind(src$index, rep.int(column, length(src$index)))])
    }
    timings[[name]] <- unname(proc.time()["elapsed"] - start)
  }
  start <- proc.time()["elapsed"]
  out <- out[c("gid", "date", daily_predictor_terms())]
  gate_c_assert_dynamic(out, prepared, date)
  timings$dynamic_validation <- unname(proc.time()["elapsed"] - start)
  list(data = out, timing = timings)
}

gate_c_assert_dynamic <- function(dynamic, prepared, date) {
  gate_c_assert_prepared(prepared); gate_c_assert_date(date)
  # Exact identity to a unique, immutable gid vector proves qualified key
  # uniqueness without reconstructing chunk/gid/date strings on every date.
  if (!identical(names(dynamic), c("gid", "date", daily_predictor_terms())) ||
      nrow(dynamic) != length(prepared$gid) || !identical(dynamic$gid, prepared$gid) ||
      !identical(dynamic$date, rep(date, length(prepared$gid)))) stop("Dynamic date/key/order mismatch.")
  daily_assert_numeric(dynamic, daily_predictor_terms())
  invisible(TRUE)
}

attach_gate_c_static <- function(dynamic, prepared, date) {
  gate_c_assert_dynamic(dynamic, prepared, date)
  out <- prepared$blueprint
  out$date <- dynamic$date
  for (name in daily_predictor_terms()) out[[name]] <- dynamic[[name]]
  out
}

gate_c_assert_batch <- function(batch, prepared, date) {
  gate_c_assert_prepared(prepared); gate_c_assert_date(date)
  if (!identical(names(batch), names(prepared$blueprint)) || nrow(batch) != nrow(prepared$blueprint)) stop("Prepared schema/rows differ.")
  invariant <- setdiff(names(batch), c("date", daily_predictor_terms()))
  if (!all(vapply(invariant, function(nm) identical(batch[[nm]], prepared$blueprint[[nm]]), logical(1))) ||
      !identical(batch$date, rep(date, nrow(batch)))) stop("Prepared metadata/static identity changed.")
  daily_assert_numeric(batch, daily_predictor_terms())
  invisible(TRUE)
}

predict_gate_c_batch <- function(batch, prepared, date, models) {
  gate_c_assert_batch(batch, prepared, date)
  if (!identical(names(models), c("PM10", "PM25"))) stop("Both pollutant models required.")
  metadata <- c("chunk_id", "gid", "grid_cell_id", "grid_template_id", "date", "x", "y")
  out <- batch[metadata]; predictors <- batch[daily_model_predictor_terms()]
  timing <- list()
  for (outcome in names(models)) {
    model <- models[[outcome]]
    if (model$metadata$outcome != outcome || model$metadata$target_year != 2023L ||
        !identical(model$metadata$schema, prepared$schema) ||
        !identical(model$metadata$configuration, daily_monitor_validation_config())) stop("Model contract mismatch.")
    check_engine <- function() {
      cfg <- xgboost::xgb.config(workflows::extract_fit_engine(model$workflow))$learner$generic_param
      if (cfg$nthread != "40" || cfg$device != "cpu") stop("XGBoost configuration mismatch.")
    }
    check_engine(); before <- assert_daily_monitor_threads(); start <- proc.time()["elapsed"]
    values <- with_daily_xgb_threads(predict(model$workflow, new_data = predictors)$.pred)
    seconds <- unname(proc.time()["elapsed"] - start)
    after <- assert_daily_monitor_threads(); check_engine()
    if (length(values) != nrow(batch) || any(!is.finite(values))) stop("Nonfinite/incomplete prediction.")
    out[[outcome]] <- values
    timing[[outcome]] <- list(seconds = seconds, blas_before = before$effective_blas_threads,
      blas_after = after$effective_blas_threads, xgb_nthread = 40L, negative = sum(values < 0))
  }
  if (!identical(out[metadata], batch[metadata])) stop("Prediction identity changed.")
  list(data = out, timing = timing)
}

gate_c_equal <- function(actual, expected) {
  if (!identical(names(actual), names(expected)) || nrow(actual) != nrow(expected)) stop("Reference schema/rows differ.")
  for (nm in names(actual)) {
    a <- actual[[nm]]; b <- expected[[nm]]
    if (inherits(a, "Date") && inherits(b, "Date")) { a <- as.double(a); b <- as.double(b) }
    if (!identical(a, b)) stop("Reference differs: ", nm)
  }
  invisible(TRUE)
}

gate_c_partition_index <- function(selected) {
  if (nrow(selected) != 10L || anyDuplicated(selected$chunk_id)) stop("Expected ten unique chunks.")
  dates <- january_smoke_dates(); assert_january_smoke_dates(dates)
  out <- data.frame(chunk_id = rep(selected$chunk_id, each = 31L),
                    date = rep(dates, 10L), n_rows = rep(selected$cells, each = 31L))
  if (nrow(out) != 310L) stop("Expected 310 partitions.")
  out
}

run_gate_c_date <- function(prepared, models, date, directory, reference_path = NULL) {
  start <- proc.time(); before <- gate_b_rss(); stages <- list()
  timed <- function(name, expr) {
    t <- proc.time()["elapsed"]; value <- force(expr)
    stages[[name]] <<- unname(proc.time()["elapsed"] - t); value
  }
  material <- timed("materialization", materialize_gate_c_date(prepared, date))
  batch <- timed("static_attach", attach_gate_c_static(material$data, prepared, date))
  timed("validation", gate_c_assert_batch(batch, prepared, date))
  pred <- timed("prediction_boundary", predict_gate_c_batch(batch, prepared, date, models))
  if (!is.null(reference_path)) {
    expected <- as.data.frame(nanoparquet::read_parquet(reference_path))
    gate_c_equal(pred$data, expected)
    rm(expected)
  }
  fields <- c("chunk_id", "gid", "grid_cell_id", "date", "x", "y", "PM10", "PM25")
  output <- pred$data[fields]
  path <- file.path(directory, paste0(date, ".parquet"))
  if (any(file.exists(c(path, paste0(path, ".partial"), paste0(path, ".json"))))) stop("Partition output exists.")
  timed("serialization", {
    nanoparquet::write_parquet(output, paste0(path, ".partial"), compression = "zstd")
    reread <- as.data.frame(nanoparquet::read_parquet(paste0(path, ".partial")))
    gate_c_equal(reread, output)
    if (!file.rename(paste0(path, ".partial"), path)) stop("Partition atomic write failed.")
    rm(reread)
  })
  dynamic <- daily_predictor_terms(); era <- c("t2m", "u10", "v10", "sp", "ssr", "tp")
  missing <- vapply(batch[dynamic], function(x) sum(is.na(x)), integer(1))
  audit <- prepared$audit; audit$per_predictor <- as.list(audit$per_predictor)
  manifest <- list(status = "PASS", chunk_id = prepared$row$chunk_id,
    annual_branch = prepared$row$annual_branch, contract_branch = prepared$row$contract_branch,
    date = as.character(date), rows = nrow(batch), fields = fields,
    schema = prepared$schema, models = lapply(models, `[[`, "source"),
    static_normalization = audit, predictor_missing = as.list(missing),
    static_missing_rows = prepared$static_missing, timing = pred$timing,
    sha256 = digest::digest(file = path, algo = "sha256"),
    finite_counts = list(PM10 = nrow(batch), PM25 = nrow(batch)),
    spatial_sha256 = digest::digest(output[c("chunk_id", "gid", "grid_cell_id", "x", "y")], algo = "sha256"),
    identity_preserved = TRUE, reference_equal = !is.null(reference_path))
  write_daily_validation_json(manifest, paste0(path, ".json"))
  record <- data.frame(class = prepared$row$class, chunk_id = prepared$row$chunk_id,
    date = as.character(date), n_rows = nrow(batch), materialization_seconds = stages$materialization,
    dynamic_preparation_seconds = material$timing$dynamic_validation,
    aod_seconds = material$timing$aod, era5_seconds = material$timing$era5_land, blh_seconds = material$timing$blh,
    static_attach_seconds = stages$static_attach, validation_seconds = stages$validation,
    prediction_boundary_seconds = stages$prediction_boundary,
    pm10_seconds = pred$timing$PM10$seconds, pm25_seconds = pred$timing$PM25$seconds,
    serialization_seconds = stages$serialization, rss_before_mib = before[[1]],
    hwm_mib = gate_b_rss()[[2]], output_bytes = file.info(path)$size,
    any_dynamic_missing = sum(rowSums(is.na(batch[dynamic])) > 0L),
    era5_missing = sum(rowSums(is.na(batch[era])) > 0L), static_missing = prepared$static_missing,
    negative_pm10 = pred$timing$PM10$negative, negative_pm25 = pred$timing$PM25$negative)
  for (nm in dynamic) record[[paste0(nm, "_missing")]] <- missing[[nm]]
  rm(batch, material, pred, output); gc()
  elapsed <- proc.time() - start
  record$total_seconds <- unname(elapsed["elapsed"])
  record$cpu_seconds <- sum(elapsed[c("user.self", "sys.self")])
  record$rss_after_gc_mib <- gate_b_rss()[[1]]
  record
}

validate_gate_c_partitions <- function(directory, row) {
  paths <- file.path(directory, paste0(january_smoke_dates(), ".parquet"))
  if (!setequal(list.files(directory, pattern = "[.]parquet$", full.names = TRUE), paths) ||
      any(!file.exists(paste0(paths, ".json")))) stop("Incomplete/duplicate chunk partitions.")
  records <- lapply(seq_along(paths), function(i) {
    path <- paths[i]; m <- jsonlite::read_json(paste0(path, ".json"), simplifyVector = TRUE)
    if (m$status != "PASS" || m$chunk_id != row$chunk_id || m$annual_branch != row$annual_branch ||
        m$date != as.character(january_smoke_dates()[i]) || m$rows != row$cells ||
        m$sha256 != digest::digest(file = path, algo = "sha256") ||
        m$finite_counts$PM10 != row$cells || m$finite_counts$PM25 != row$cells ||
        !isTRUE(m$identity_preserved)) stop("Partition manifest/checksum mismatch.")
    for (t in m$timing) if (t$blas_before != 1 || t$blas_after != 1 || t$xgb_nthread != 40) stop("Partition thread mismatch.")
    data.frame(chunk_id = row$chunk_id, date = m$date, rows = m$rows,
      spatial_sha256 = m$spatial_sha256, sha256 = m$sha256, bytes = file.info(path)$size)
  })
  out <- do.call(rbind, records)
  if (length(unique(out$spatial_sha256)) != 1L) stop("Spatial identity varies across dates.")
  out
}

gate_c_memory_check <- function(records) {
  if (length(records)) {
    chunk <- tail(records, 1L)[[1L]]$chunk_id
    records <- Filter(function(x) identical(x$chunk_id, chunk), records)
  }
  if (length(records) < 12L) return(invisible(TRUE))
  rss <- vapply(records, function(x) x$rss_after_gc_mib, numeric(1))
  # Stable live one-date state should plateau after allocator warm-up. Fail if
  # three consecutive four-date windows each grow by > one full input matrix.
  unit <- max(vapply(records, function(x) x$n_rows, numeric(1))) * 113 * 8 / 1024^2
  last <- tail(rss, 12L); windows <- colMeans(matrix(last, nrow = 4L))
  if (all(diff(windows) > unit)) stop("Persistent memory growth exceeds a full predictor matrix per window.")
  mem <- readLines("/proc/meminfo")
  total <- as.numeric(strsplit(trimws(sub("^MemTotal:", "", mem[grepl("^MemTotal:", mem)])), " +")[[1]][1]) / 1024
  if (tail(rss, 1) > total / 4) stop("Sequential benchmark exceeds one quarter of host memory.")
  invisible(TRUE)
}
