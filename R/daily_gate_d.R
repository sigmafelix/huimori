gate_d_gate_c_root <- function() "daehoon/data/daily_xgb_gate_c/year=2023/month=01/run=first10_optimized_001"
gate_d_model_root <- function() "daehoon/data/daily_xgb_validation/year=2023/run=monitor_fixed_300_threadfix_001"
gate_d_hashes <- function() c(PM10 = "7588b456714d12d92da92facdf6335c4c25962a4ca3f54a9886ef16257582a16",
  PM25 = "2926dc25ffda59e011fa2aca8f2ed17269454a252d9abeec8b1dc22181ded1fc")
gate_d_fields <- function() c("chunk_id", "gid", "grid_cell_id", "date", "x", "y", "PM10", "PM25")
gate_d_spatial_fields <- function() c("chunk_id", "gid", "grid_cell_id", "x", "y")

gate_d_preconditions <- function(c_root = gate_d_gate_c_root()) {
  require_gate_c_preconditions()
  for (name in c("result.json", "diagnostics/result.json")) {
    path <- file.path(c_root, name)
    if (!file.exists(path) || jsonlite::read_json(path)$status != "PASS") stop("Final Gate C PASS required.")
  }
  c <- jsonlite::read_json(file.path(c_root, "result.json"), simplifyVector = TRUE)
  if (c$mode != "first10" || c$partitions != 310 || c$actual_rows_per_pollutant != 57281552) stop("Gate C scope differs.")
  for (i in seq_len(nrow(c$implementation))) {
    if (digest::digest(file = c$implementation$path[i], algo = "sha256") != c$implementation$sha256[i]) stop("Accepted Gate C implementation changed.")
  }
  if (daily_model_schema()$hash != "0f31205d333c8e41dfcf1368e8cc98a54fd49b0ec44e5c237a3f0024e2b0779b") stop("Schema hash drift.")
  for (p in names(gate_d_hashes())) {
    file <- file.path(gate_d_model_root(), paste0(p, "_NONPRODUCTION_REDUCED_VALIDATION.rds"))
    if (digest::digest(file = file, algo = "sha256") != gate_d_hashes()[[p]]) stop("Model hash drift.")
  }
  invisible(TRUE)
}

gate_d_registry <- function(registry, first10) {
  assert_january_registry(registry)
  if (sum(registry$cells) != 111637615 || anyDuplicated(registry$annual_branch)) stop("National registry differs.")
  out <- registry[order(registry$chunk_id, method = "radix"), ]; rownames(out) <- NULL
  cols <- setdiff(names(registry), "class")
  gate_c_equal(out[1:10, cols], first10[cols])
  out$position <- seq_len(593L); out$class <- sprintf("chunk_%03d", out$position)
  if (sum(out$cells) * 31 != 3460766065 || nrow(out) * 31 != 18383) stop("January cardinality differs.")
  out
}

gate_d_phase <- function(position) {
  if (anyNA(position) || any(!position %in% 1:593)) stop("Invalid chunk position.")
  as.character(cut(position, c(0, 10, 50, 200, 400, 593), labels = c("REUSE", "D1", "D2", "D3", "D4")))
}

gate_d_atomic_csv <- function(x, path) {
  if (file.exists(path) || file.exists(paste0(path, ".partial"))) stop("Exclusive CSV exists.")
  write.csv(x, paste0(path, ".partial"), row.names = FALSE)
  if (!file.rename(paste0(path, ".partial"), path)) stop("CSV install failed.")
}

gate_d_publish_csv <- function(x, path) {
  if (file.exists(path)) {
    if (!isTRUE(all.equal(read.csv(path), x, check.attributes = FALSE, tolerance = 0))) stop("Existing CSV differs on resume.")
  } else gate_d_atomic_csv(x, path)
}

gate_d_new_root <- function(root, resume = FALSE) {
  if (!grepl("^daehoon/data/daily_xgb_gate_d/year=2023/month=01/run=[A-Za-z0-9_-]+$", root)) stop("Invalid isolated Gate D root.")
  if (file.exists(root)) {
    if (!resume || !file.exists(file.path(root, "run_contract.json"))) stop("Output exists or is not resumable.")
  } else {
    if (resume) stop("Resume root absent.")
    dir.create(root, recursive = TRUE)
    for (d in c("partitions", "chunk_manifests", "diagnostics", "logs", "progress", "phases", "inputs")) dir.create(file.path(root, d))
  }
}

gate_d_disk_guard <- function(root, bytes = 0, rows = 0) {
  line <- tail(system2("df", c("-Pk", shQuote(root)), stdout = TRUE), 1L)
  fields <- strsplit(trimws(line), "[[:space:]]+")[[1L]]
  available <- as.numeric(fields[4]) * 1024
  remaining <- max(0, 100e9 - bytes)
  if (!is.finite(available) || available < max(50e9, remaining * 1.25)) stop("Unsafe disk headroom.")
  if (rows > 1e6 && bytes / rows > 100) stop("Unexpected output expansion above 100 bytes/row.")
  available
}

gate_d_memory_guard <- function(history, current = gate_b_rss()) {
  rss <- current[[1L]]
  if (rss > 16384) stop("Post-GC RSS exceeds 16 GiB; review required.")
  if (rss > 12288) warning("Post-GC RSS exceeds 12 GiB.")
  if (length(history) >= 15L) {
    windows <- colMeans(matrix(tail(history, 15L), nrow = 5L))
    if (all(diff(windows) > 512) && diff(range(windows)) > 2048) stop("Persistent inter-chunk memory growth.")
  }
  invisible(TRUE)
}

gate_d_validate_manifest <- function(m, row, date) {
  gate_c_assert_date(date)
  needed <- c("status", "chunk_id", "annual_branch", "contract_branch", "date", "rows", "fields", "schema",
    "models", "static_normalization", "predictor_missing", "static_missing_rows", "timing", "sha256", "finite_counts", "spatial_sha256", "identity_preserved")
  if (!all(needed %in% names(m))) stop("Incomplete partition manifest.")
  if (m$status != "PASS" || m$chunk_id != row$chunk_id || m$annual_branch != row$annual_branch ||
      m$contract_branch != row$contract_branch || m$date != as.character(date) || m$rows != row$cells ||
      !identical(unlist(m$fields, use.names = FALSE), gate_d_fields()) ||
      !identical(m$schema, daily_model_schema()) || !isTRUE(m$identity_preserved)) stop("Partition identity/schema mismatch.")
  if (!identical(names(m$models), c("PM10", "PM25")) || !identical(names(m$timing), c("PM10", "PM25"))) stop("Pollutant manifest mismatch.")
  for (p in c("PM10", "PM25")) {
    t <- m$timing[[p]]
    if (m$models[[p]]$sha256 != gate_d_hashes()[[p]] || m$finite_counts[[p]] != row$cells ||
        t$blas_before != 1 || t$blas_after != 1 || t$xgb_nthread != 40 ||
        !is.finite(t$negative) || t$negative < 0 || t$negative > row$cells) stop("Model/thread/finite manifest mismatch.")
  }
  a <- m$static_normalization
  if (a$chunk_id != row$chunk_id || a$annual_branch != row$annual_branch || a$nan_converted != row$static_missing ||
      a$normalized_nan != 0 || a$positive_inf != 0 || a$negative_inf != 0 ||
      !isTRUE(a$finite_values_unchanged) || !isTRUE(a$metadata_unchanged)) stop("Normalization manifest drift.")
  if (!identical(names(m$predictor_missing), daily_predictor_terms()) ||
      any(!is.finite(unlist(m$predictor_missing))) || any(unlist(m$predictor_missing) < 0 | unlist(m$predictor_missing) > row$cells)) stop("Missingness manifest invalid.")
  invisible(TRUE)
}

gate_d_validate_partition <- function(path, row, date, spatial = NULL, require_d = TRUE) {
  exists <- file.exists(c(path, paste0(path, ".json")))
  if (any(file.exists(c(paste0(path, ".partial"), paste0(path, ".json.partial"))))) stop("Interrupted partial output requires explicit review.")
  if (!any(exists)) return(NULL)
  if (!all(exists)) stop("Incomplete partition/manifest; refusing overwrite.")
  m <- jsonlite::read_json(paste0(path, ".json"), simplifyVector = TRUE)
  gate_d_validate_manifest(m, row, date)
  if (m$sha256 != digest::digest(file = path, algo = "sha256")) stop("Corrupt Parquet checksum.")
  if (require_d && (!all(c("origin", "source_snapshot", "source_snapshot_sha256", "metrics", "output_bytes") %in% names(m)) ||
      !m$origin %in% c("reused_gate_c", "predicted_gate_d") || m$output_bytes != file.info(path)$size ||
      !file.exists(m$source_snapshot) || digest::digest(file = m$source_snapshot, algo = "sha256") != m$source_snapshot_sha256)) stop("Incomplete Gate D provenance.")
  data <- as.data.frame(nanoparquet::read_parquet(path))
  if (!identical(names(data), gate_d_fields()) || nrow(data) != row$cells || !inherits(data$date, "Date") ||
      !all(as.double(data$date) == as.double(date)) || !all(data$chunk_id == row$chunk_id) ||
      anyNA(data[gate_d_spatial_fields()]) || anyDuplicated(data$gid) || anyDuplicated(data$grid_cell_id) ||
      any(!is.finite(data$PM10)) || any(!is.finite(data$PM25))) stop("Stored partition values/keys invalid.")
  if (digest::digest(data[gate_d_spatial_fields()], algo = "sha256") != m$spatial_sha256) stop("Stored spatial digest mismatch.")
  if (!is.null(spatial)) gate_c_equal(data[gate_d_spatial_fields()], spatial)
  for (p in c("PM10", "PM25")) if (sum(data[[p]] < 0) != m$timing[[p]]$negative) stop("Negative count mismatch.")
  list(manifest = m, data = data)
}

gate_d_chunk_check <- function(directory, row, spatial = NULL) {
  dates <- january_smoke_dates(); paths <- file.path(directory, paste0(dates, ".parquet"))
  if (!setequal(paths, list.files(directory, pattern = "[.]parquet$", full.names = TRUE))) stop("Chunk requires exactly 31 partitions.")
  records <- lapply(seq_along(dates), function(j) {
    x <- gate_d_validate_partition(paths[j], row, dates[j], spatial)
    if (is.null(x)) stop("Missing chunk partition.")
    m <- x$manifest
    data.frame(date = m$date, rows = m$rows, sha256 = m$sha256, spatial_sha256 = m$spatial_sha256,
      bytes = m$output_bytes, origin = m$origin, manifest_sha256 = digest::digest(file = paste0(paths[j], ".json"), algo = "sha256"))
  })
  out <- do.call(rbind, records)
  if (length(unique(out$spatial_sha256)) != 1L || sum(out$rows) != row$cells * 31) stop("Chunk spatial/count drift.")
  out
}

gate_d_write_date <- function(prepared, models, date, directory, snapshot, reuse = NULL, old_metrics = NULL) {
  path <- file.path(directory, paste0(date, ".parquet"))
  spatial <- prepared$blueprint[gate_d_spatial_fields()]
  existing <- gate_d_validate_partition(path, prepared$row, date, spatial)
  if (!is.null(existing)) return(list(record = as.data.frame(existing$manifest$metrics), skipped = TRUE))
  if (!is.null(reuse)) {
    prior <- gate_d_validate_partition(reuse, prepared$row, date, spatial, require_d = FALSE)
    if (is.null(prior)) stop("Accepted Gate C partition missing.")
    if (!file.copy(reuse, paste0(path, ".partial"), overwrite = FALSE) ||
        digest::digest(file = paste0(path, ".partial"), algo = "sha256") != prior$manifest$sha256 ||
        !file.rename(paste0(path, ".partial"), path)) stop("Immutable reuse copy failed.")
    manifest <- prior$manifest; record <- old_metrics
    manifest$origin <- "reused_gate_c"; manifest$reused_path <- reuse
    manifest$reused_manifest_sha256 <- digest::digest(file = paste0(reuse, ".json"), algo = "sha256")
  } else {
    # Keep the accepted prediction function unchanged. Delay publication of its
    # manifest until resource/provenance fields are available for atomic commit.
    manifest <- NULL
    env <- new.env(parent = environment(run_gate_c_date))
    env$write_daily_validation_json <- function(x, path) { manifest <<- x; invisible(NULL) }
    run <- run_gate_c_date; environment(run) <- env
    record <- run(prepared, models, date, directory)
    manifest$origin <- "predicted_gate_d"
  }
  manifest$metrics <- as.list(record[1, ])
  manifest$source_snapshot <- snapshot
  manifest$source_snapshot_sha256 <- digest::digest(file = snapshot, algo = "sha256")
  manifest$source_identities <- read.csv(snapshot)[c("path", "sha256", "bytes")]
  manifest$output_bytes <- file.info(path)$size
  manifest$any_era5_missing <- record$era5_missing
  manifest$any_dynamic_missing <- record$any_dynamic_missing
  write_daily_validation_json(manifest, paste0(path, ".json"))
  gate_d_validate_partition(path, prepared$row, date, spatial)
  list(record = record, skipped = FALSE)
}

gate_d_normalization_check <- function(audits, expected) {
  rows <- do.call(rbind, lapply(audits, function(a) {
    a$per_predictor <- unlist(a$per_predictor)
    static_normalization_audit_row(a)
  }))
  idx <- match(rows$chunk_id, expected$chunk_id)
  if (anyNA(idx) || anyDuplicated(rows$chunk_id) || any(rows$nan_converted != expected$nan_converted[idx]) ||
      any(rows$affected_rows != expected$affected_rows[idx]) || any(rows$raw_na != 0 | rows$normalized_nan != 0 |
      rows$positive_inf != 0 | rows$negative_inf != 0) || any(rows$normalized_na != rows$nan_converted)) stop("Static normalization count drift.")
  terms <- paste0("nan_", daily_static_predictor_terms())
  if (!identical(as.matrix(rows[terms]), as.matrix(expected[idx, terms]))) {
    if (any(as.matrix(rows[terms]) != as.matrix(expected[idx, terms]))) stop("Static per-predictor drift.")
  }
  if (nrow(rows) == 593 && (sum(rows$nan_converted) != 1586 || sum(rows$affected_rows) != 605 || sum(rows$nan_converted > 0) != 4)) stop("Nationwide normalization differs.")
  rows
}

gate_d_init_occupancy <- function() {
  if (!requireNamespace("bit", quietly = TRUE)) stop("The bit package is required for bounded occupancy.")
}

gate_d_new_occupancy <- function(n) {
  gate_d_init_occupancy()
  out <- new.env(parent = emptyenv()); out$bits <- bit::bit(n); out$count <- 0
  out
}

gate_d_mark_cells <- function(seen, ids, expected = NULL) {
  if (any(!is.finite(ids)) || any(ids != floor(ids) | ids < 1 | ids > length(seen$bits))) stop("Invalid global cell.")
  if (anyDuplicated(ids) || any(seen$bits[ids])) stop("Duplicate global cell.")
  if (!is.null(expected) && any(!expected$bits[ids])) stop("Unexpected registered cell.")
  seen$bits[ids] <- TRUE; seen$count <- seen$count + length(ids)
  length(ids)
}

gate_d_complete_occupancy <- function(n_seen, n_expected) {
  if (n_seen != n_expected) stop("Missing registered cells.")
  invisible(TRUE)
}
