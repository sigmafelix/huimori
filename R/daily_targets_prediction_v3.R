daily_2023_prediction_threads_v3 <- function() {
  Sys.setenv(OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1", NUMEXPR_NUM_THREADS = "1",
    GDAL_NUM_THREADS = "1", OMP_NUM_THREADS = "2", OMP_THREAD_LIMIT = "2",
    OMP_MAX_ACTIVE_LEVELS = "1", OMP_NESTED = "FALSE")
  RhpcBLASctl::blas_set_num_threads(1L)
  RhpcBLASctl::omp_set_num_threads(2L)
  stopifnot(RhpcBLASctl::blas_get_num_procs() == 1L)
}

daily_2023_load_models_v3 <- function(paths) {
  models <- daily_2023_load_models(paths)
  for (p in names(models)) {
    engine <- workflows::extract_fit_engine(models[[p]]$workflow)
    xgboost::xgb.model.parameters(engine) <- list(nthread = 2L)
    models[[p]]$workflow$fit$fit$fit <- engine
  }
  models
}

daily_2023_predict_v3 <- function(batch, prepared, date, models) {
  daily_2023_assert_prepared(prepared, date)
  invariant <- setdiff(names(prepared$blueprint), c("date", daily_predictor_terms()))
  stopifnot(identical(names(batch), names(prepared$blueprint)),
    identical(batch$date, rep(date, length(prepared$gid))),
    all(vapply(invariant, function(nm) identical(batch[[nm]], prepared$blueprint[[nm]]), logical(1))),
    identical(names(batch)[names(batch) %in% daily_model_predictor_terms()], daily_model_predictor_terms()))
  daily_assert_numeric(batch, daily_predictor_terms())
  out <- batch[c("chunk_id", "gid", "grid_cell_id", "date", "x", "y")]
  predictors <- batch[daily_model_predictor_terms()]; timing <- list()
  for (p in c("PM10", "PM25")) {
    daily_2023_prediction_threads_v3()
    engine <- workflows::extract_fit_engine(models[[p]]$workflow)
    stopifnot(xgboost::xgb.config(engine)$learner$generic_param$nthread == "2")
    seconds <- system.time(values <- predict(models[[p]]$workflow, new_data = predictors)$.pred)["elapsed"]
    daily_2023_prediction_threads_v3()
    stopifnot(xgboost::xgb.config(engine)$learner$generic_param$nthread == "2",
      length(values) == nrow(batch), all(is.finite(values)))
    out[[p]] <- values
    timing[[p]] <- list(seconds = unname(seconds), blas_before = 1L, blas_after = 1L,
      xgb_nthread = 2L, finite = length(values), negative = sum(values < 0))
  }
  list(data = out, timing = timing)
}

daily_2023_revalidate_reuse <- function(record, contract, catalog, model_files) {
  stopifnot(all(file.exists(record$files)),
    identical(unname(vapply(record$files, daily_2023_sha, "")), unname(record$sha256)),
    record$chunk_id == contract$chunk_id, record$month == contract$month)
  prepared <- daily_2023_prepare_chunk(contract, catalog[[contract$chunk_id]])
  models <- daily_2023_load_models(model_files)
  paths <- record$files[endsWith(record$files, ".parquet")]
  dates <- sub("[.]parquet$", "", basename(paths))
  stopifnot(identical(dates, as.character(daily_2023_dates(contract$month))))
  for (i in seq_along(paths)) {
    m <- jsonlite::read_json(paste0(paths[i], ".json"))
    data <- as.data.frame(nanoparquet::read_parquet(paths[i]))
    expected <- prepared$blueprint[c("chunk_id", "gid", "grid_cell_id", "date", "x", "y")]
    expected$date <- rep(as.Date(dates[i]), nrow(expected))
    gate_c_equal(data[names(expected)], expected)
    stopifnot(identical(names(data), c(names(expected), "PM10", "PM25")),
      m$status == "PASS", m$date == dates[i], m$rows == nrow(data),
      m$schema$hash == daily_model_schema()$hash,
      identical(m$models, lapply(models, `[[`, "source")),
      identical(jsonlite::toJSON(m$source_hashes, auto_unbox = TRUE, digits = 16),
        jsonlite::toJSON(prepared$source_hashes, auto_unbox = TRUE, digits = 16)),
      m$static_normalization$nan_converted == prepared$audit$nan_converted,
      m$static_normalization$affected_rows == prepared$audit$affected_rows,
      identical(as.numeric(unlist(m$static_normalization$per_predictor)), as.numeric(prepared$audit$per_predictor)))
    for (p in c("PM10", "PM25")) stopifnot(all(is.finite(data[[p]])),
      m$timing[[p]]$finite == nrow(data), m$timing[[p]]$negative == sum(data[[p]] < 0),
      m$timing[[p]]$xgb_nthread == 5L, m$timing[[p]]$blas_before == 1L, m$timing[[p]]$blas_after == 1L)
    rm(data, expected); gc(FALSE)
    rss <- gate_b_rss()[[1]]
    if (rss > 16384) stop("Reuse post-GC RSS exceeds 16 GiB: ", rss, " MiB at ", dates[i])
  }
  message("REUSED validated chunk/month: ", contract$chunk_id, "/", contract$month)
  record$files
}

daily_2023_check_reuse_target <- function(record, contracts, catalog, model_files, configuration) {
  matches <- vapply(contracts, function(x) x$chunk_id == record$chunk_id && x$month == record$month, logical(1))
  stopifnot(sum(matches) == 1L)
  daily_2023_prediction_threads_v3()
  daily_2023_revalidate_reuse(record, contracts[[which(matches)]], catalog, model_files)
  cfg <- daily_2023_read_config(configuration)
  directory <- daily_2023_attempt(cfg$root, "reuse20_validation")
  daily_2023_atomic_json(list(status = "PASS", chunk_id = record$chunk_id, month = record$month,
    partitions = sum(endsWith(record$files, ".parquet")), rows = record$rows,
    files = record$files, sha256 = as.list(record$sha256)), file.path(directory, "result.json"))
}

daily_2023_reuse_gate <- function(checks, reuse_file, configuration) {
  records <- readRDS(reuse_file)
  paths <- unlist(checks, use.names = FALSE)
  stopifnot(length(paths) == length(records))
  results <- lapply(paths, jsonlite::read_json)
  keys <- vapply(results, function(x) paste(x$chunk_id, x$month, sep = "|"), "")
  stopifnot(!anyDuplicated(keys), setequal(keys, names(records)),
    all(vapply(results, function(x) x$status == "PASS", logical(1))))
  cfg <- daily_2023_read_config(configuration)
  daily_2023_atomic_json(list(status = "PASS", months = length(records),
    reuse_sha256 = daily_2023_sha(reuse_file), checks = paths),
    file.path(daily_2023_attempt(cfg$root, "reuse20_gate"), "result.json"))
}

daily_2023_predict_month_v3 <- function(contract, catalog, model_files, configuration, january_gate, reuse_file, reuse_gate) {
  daily_2023_prediction_threads_v3()
  gate <- jsonlite::read_json(reuse_gate)
  stopifnot(gate$status == "PASS", gate$reuse_sha256 == daily_2023_sha(reuse_file))
  frozen <- readRDS(reuse_file)
  record <- frozen[[paste(contract$chunk_id, contract$month, sep = "|")]]
  if (!is.null(record)) {
    stopifnot(identical(unname(vapply(record$files, daily_2023_sha, "")), unname(record$sha256)))
    return(record$files)
  }
  # Bind only the execution-specific hooks on a private function copy. The
  # accepted month algorithm, its memory guards and original function stay intact.
  original_environment <- environment(daily_2023_predict_month)
  hooks <- new.env(parent = original_environment)
  hooks$daily_2023_prediction_threads <- daily_2023_prediction_threads_v3
  hooks$daily_2023_load_models <- daily_2023_load_models_v3
  hooks$daily_2023_predict <- daily_2023_predict_v3
  hooks$daily_2023_atomic_json <- function(value, path) {
    value$resource_mode <- list(version = 3L, prediction_workers = 20L, xgboost_threads = 2L, blas_threads = 1L)
    daily_2023_atomic_json(value, path)
  }
  runner <- daily_2023_predict_month
  environment(runner) <- hooks
  stopifnot(identical(environment(daily_2023_predict_month), original_environment))
  runner(contract, catalog, model_files, configuration, january_gate)
}
