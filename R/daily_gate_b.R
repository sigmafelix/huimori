gate_b_root <- function() "daehoon/data/daily_xgb_national_smoke/year=2023/month=01/run=january_gate_a_nanfix_w40_001"

require_gate_b_precondition <- function(root) {
  path <- file.path(root, "result.json")
  if (!file.exists(path)) stop("Final Gate A result.json required.")
  x <- jsonlite::read_json(path, simplifyVector = TRUE)
  if (!identical(x$status, "PASS") || !identical(x$phase, "gate-a") ||
      x$branches != 593 || x$cells != 111637615 || x$converted != 1586 ||
      x$affected_rows != 605 || !isTRUE(x$annual_artifacts_immutable) ||
      !isTRUE(x$protected_inputs_immutable)) stop("Final Gate A contract failed.")
  x
}

select_gate_b_chunks <- function(registry) {
  assert_january_registry(registry)
  small <- match("bbox_1002626_1585240_1003676_1586230_n846_gid_1_846", registry$chunk_id)
  candidates <- which(registry$cells >= 50000 & registry$cells <= 150000)
  if (!length(candidates)) stop("No intermediate chunk in 50000-150000 range.")
  medium <- candidates[order(abs(registry$cells[candidates] - 100000), registry$chunk_id[candidates])[1]]
  large <- order(-registry$cells, registry$chunk_id)[1]
  idx <- c(small, medium, large)
  if (anyNA(idx) || anyDuplicated(idx) || registry$cells[small] != 846) stop("Invalid representative selection.")
  out <- registry[idx, ]; out$class <- c("small", "medium", "large"); rownames(out) <- NULL
  out
}

new_gate_b_directory <- function(path) {
  if (!grepl("^daehoon/data/daily_xgb_gate_b/year=2023/month=01/run=[A-Za-z0-9_-]+$", path)) stop("Invalid isolated benchmark path.")
  if (file.exists(path)) stop("Benchmark output exists.")
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  if (!dir.create(path)) stop("Cannot create benchmark directory.")
}

gate_b_rss <- function() {
  s <- strsplit(trimws(readLines("/proc/self/status")), "[[:space:]]+")
  get <- function(k) as.numeric(s[[which(vapply(s, `[`, "", 1) == k)]][2]) / 1024
  c(rss_mib = get("VmRSS:"), hwm_mib = get("VmHWM:"))
}

gate_b_snapshot <- function(paths) {
  paths <- sort(unique(paths)); if (any(!file.exists(paths))) stop("Missing protected artifact.")
  data.frame(path = paths, sha256 = vapply(paths, function(p) digest::digest(file = p, algo = "sha256"), ""),
    bytes = file.info(paths)$size, mtime = as.numeric(file.info(paths)$mtime), row.names = NULL)
}

# Instrument the accepted algorithm without changing layer selection or extraction.
materialize_gate_b_date <- function(contract, date) {
  if (!inherits(contract, "daily_grid_month_contract") || contract$month != "2023-01" ||
      !inherits(date, "Date") || length(date) != 1L || !date %in% january_smoke_dates()) stop("Expected one January date.")
  timing <- list()
  timing$cell_map <- unname(system.time(map <- read_grid_daily_cell_map(contract$cell_map_file))["elapsed"])
  keys <- tidyr::expand_grid(gid = map$gid, date = date)
  parts <- list()
  for (s in names(contract$sources)) {
    src <- contract$sources[[s]]
    timing[[s]] <- unname(system.time(parts[[s]] <- extract_daily_cube_keys(
      keys, map, src$cube_files, src$source, "gid"))["elapsed"])
  }
  timing$dynamic_merge <- unname(system.time(out <- merge_daily_dynamic_features(
    keys, parts$aod, parts$era5_land, parts$blh, contract$month, "gid"))["elapsed"])
  attr(out, "streaming_contract") <- TRUE
  if (!identical(out$gid, keys$gid) || !identical(out$date, keys$date)) stop("Materialization changed keys/order.")
  list(data = out, timing = timing)
}

gate_b_static_import <- function(annual, grid, row, template) {
  if (!"annual_branch" %in% names(row) || !nzchar(row$annual_branch)) stop("annual_branch required by benchmark.")
  seconds <- 0
  env <- new.env(parent = environment(national_static_chunk))
  env$normalize_daily_model_missing_values <- function(...) {
    t <- system.time(result <- normalize_daily_model_missing_values(...))
    seconds <<- unname(t["elapsed"]); result
  }
  importer <- national_static_chunk; environment(importer) <- env
  total <- system.time(out <- importer(annual, grid, row$chunk_id, template, annual_branch = row$annual_branch))
  audit <- attr(out, "static_normalization")
  if (audit$nan_converted != row$static_missing || audit$normalized_nan != 0 ||
      audit$positive_inf != 0 || audit$negative_inf != 0) stop("Static normalization drift.")
  list(data = out, normalization_seconds = seconds,
       import_validation_seconds = unname(total["elapsed"]) - seconds, audit = audit)
}

run_gate_b_date <- function(contract, static, grid, row, models, template, date, directory, compare_reference = FALSE) {
  start <- proc.time(); before <- gate_b_rss()
  material <- materialize_gate_b_date(contract, date)
  reference_seconds <- 0
  if (compare_reference) {
    reference_seconds <- unname(system.time(reference <- materialize_grid_daily_contract(contract, dates = date))["elapsed"])
    if (!identical(material$data, reference)) stop("Instrumented/reference materialization differs.")
    rm(reference)
  }
  join_seconds <- unname(system.time(joined <- join_grid_daily_annual_features(
    material$data, static$data, row$chunk_id, grid, template, month = "2023-01"))["elapsed"])
  validation_seconds <- unname(system.time({
    assert_daily_model_schema(joined); assert_daily_grid_identity(joined, template)
    if (nrow(joined) != row$cells || !identical(joined$gid, material$data$gid) ||
        !identical(joined$date, material$data$date)) stop("Benchmark row/order mismatch.")
  })["elapsed"])
  pred <- predict_january_batch(joined, models, template, static$audit)
  path <- file.path(directory, paste0(date, ".parquet"))
  if (file.exists(path) || file.exists(paste0(path, ".partial"))) stop("Prediction output exists.")
  serialization <- unname(system.time({
    nanoparquet::write_parquet(pred$data, paste0(path, ".partial"), compression = "zstd")
    reread <- as.data.frame(nanoparquet::read_parquet(paste0(path, ".partial")))
    # Parquet Date is INT32; R Date may store the same exact days as doubles.
    other <- setdiff(names(pred$data), "date")
    if (!identical(names(reread), names(pred$data)) || !inherits(reread$date, "Date") ||
        !identical(as.double(reread$date), as.double(pred$data$date)) ||
        !identical(reread[other], pred$data[other])) stop("Parquet roundtrip differs.")
    if (!file.rename(paste0(path, ".partial"), path)) stop("Atomic write failed.")
  })["elapsed"])
  era <- c("t2m", "u10", "v10", "sp", "ssr", "tp")
  record <- data.frame(class = row$class, chunk_id = row$chunk_id, date = as.character(date), n_rows = nrow(joined),
    cell_map_seconds = material$timing$cell_map, aod_seconds = material$timing$aod,
    era5_seconds = material$timing$era5_land, blh_seconds = material$timing$blh,
    dynamic_merge_seconds = material$timing$dynamic_merge, reference_seconds = reference_seconds,
    join_seconds = join_seconds, validation_seconds = validation_seconds,
    pm10_seconds = pred$timing$PM10$seconds, pm25_seconds = pred$timing$PM25$seconds,
    serialization_seconds = serialization, rss_before_mib = before[[1]], hwm_mib = gate_b_rss()[[2]],
    output_bytes = file.info(path)$size, aod_missing = sum(is.na(joined$aod)), blh_missing = sum(is.na(joined$blh)),
    era5_missing = sum(rowSums(is.na(joined[era])) > 0),
    static_missing = sum(rowSums(is.na(joined[daily_static_predictor_terms()])) > 0),
    negative_pm10 = pred$timing$PM10$negative, negative_pm25 = pred$timing$PM25$negative)
  normalization_record <- static$audit
  normalization_record$per_predictor <- as.list(normalization_record$per_predictor)
  manifest <- list(status = "PASS", chunk_id = row$chunk_id, annual_branch = row$annual_branch,
    contract_branch = row$contract_branch, date = as.character(date), rows = nrow(joined),
    schema = daily_model_schema(), models = lapply(models, `[[`, "source"),
    static_normalization = normalization_record, predictor_missing = as.list(pred$missing), timing = pred$timing,
    pollutants = c("PM10", "PM25"),
    finite_counts = list(PM10 = sum(is.finite(pred$data$PM10)), PM25 = sum(is.finite(pred$data$PM25))),
    sha256 = digest::digest(file = path, algo = "sha256"), identity_preserved = TRUE,
    sources = lapply(contract$sources, unclass), cell_map = contract$cell_map_file)
  write_daily_validation_json(manifest, paste0(path, ".json"))
  rm(joined, material, pred); gc()
  elapsed <- proc.time() - start
  record$total_seconds <- unname(elapsed["elapsed"])
  record$cpu_seconds <- sum(elapsed[c("user.self", "sys.self")])
  record$rss_after_gc_mib <- gate_b_rss()[[1]]
  record
}
