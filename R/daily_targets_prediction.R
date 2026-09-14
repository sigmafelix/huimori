daily_2023_static_descriptor <- function(annual, registry_path, normalization_path) {
  registry <- read.csv(registry_path)
  grid <- as.data.frame(annual[c("gid", "x", "y", "layer")])
  spatial <- sf::st_as_sf(grid, coords = c("x", "y"), crs = 5179, remove = FALSE)
  id <- daily_grid_chunk_key(spatial)
  row <- registry[match(id, registry$chunk_id), , drop = FALSE]
  if (nrow(row) != 1L || anyNA(row$chunk_id)) stop("Annual branch lacks semantic registration.")
  map <- read_grid_daily_cell_map(row$cell_map)
  stopifnot(identical(map$gid, grid$gid), nrow(grid) == row$cells)
  imported <- national_static_chunk(as.data.frame(annual), grid, id,
    daily_canonical_template(), row$annual_branch)
  audit <- attr(imported, "static_normalization")
  if (audit$nan_converted != row$static_missing) stop("Static normalization changed.")
  path <- file.path("_targets", "objects", row$annual_branch)
  expected <- read.csv(normalization_path)
  expected <- expected[match(row$annual_branch, expected$annual_branch), ]
  hash <- daily_2023_sha(path)
  stopifnot(nrow(expected) == 1L, !is.na(expected$annual_branch),
    hash == expected$annual_file_sha256_before, hash == expected$annual_file_sha256_after,
    audit$affected_rows == expected$affected_rows,
    identical(as.numeric(audit$per_predictor), as.numeric(expected[paste0("nan_", daily_static_predictor_terms())])))
  list(row = row, path = path, sha256 = hash, normalization = audit)
}

daily_2023_static_catalog <- function(descriptors) {
  if (length(descriptors) != 593L) stop("Expected 593 annual descriptors.")
  ids <- vapply(descriptors, function(x) x$row$chunk_id, "")
  stopifnot(!anyDuplicated(ids), sum(vapply(descriptors, function(x) x$row$cells, 0)) == 111637615,
    sum(vapply(descriptors, function(x) x$normalization$nan_converted, 0)) == 1586,
    sum(vapply(descriptors, function(x) x$normalization$affected_rows, 0)) == 605,
    sum(vapply(descriptors, function(x) x$normalization$nan_converted > 0, logical(1))) == 4L)
  stats::setNames(descriptors, ids)
}

daily_2023_contract <- function(contract) {
  if (!inherits(contract, "daily_grid_month_contract") ||
      !contract$month %in% sprintf("2023-%02d", 1:12) ||
      !identical(contract$predictor_terms, daily_predictor_terms())) stop("Invalid 2023 source contract.")
  contract
}

daily_2023_prepare_chunk <- function(contract, descriptor) {
  daily_2023_contract(contract)
  row <- descriptor$row
  if (!identical(contract$chunk_id, row$chunk_id) ||
      daily_2023_sha(descriptor$path) != descriptor$sha256) stop("Static source identity changed.")
  annual <- as.data.frame(targets::tar_read_raw(row$annual_branch))
  grid <- annual[c("gid", "x", "y", "layer")]
  static <- national_static_chunk(annual, grid, row$chunk_id, daily_canonical_template(), row$annual_branch)
  audit <- attr(static, "static_normalization")
  stopifnot(identical(audit$per_predictor, descriptor$normalization$per_predictor))
  manifest <- validate_grid_daily_cell_map(contract$cell_map_file, contract$sources)
  map <- read_grid_daily_cell_map(contract$cell_map_file)
  stopifnot(manifest$chunk_id == row$chunk_id, nrow(map) == row$cells, identical(map$gid, grid$gid))
  fixture <- data.frame(gid = map$gid, date = rep(daily_2023_dates(contract$month)[1], nrow(map)))
  for (nm in daily_predictor_terms()) fixture[[nm]] <- NA_real_
  blueprint <- join_grid_daily_annual_features(fixture, static, row$chunk_id, grid,
    daily_canonical_template(), month = contract$month)
  sources <- lapply(contract$sources, function(src) {
    manifest <- validate_daily_cube(src$cube_files, source = src$source, month = contract$month)
    cells <- map[[paste0(src$source, "_cell")]]
    if (!is.numeric(cells) || anyNA(cells) || any(!is.finite(cells))) stop("Invalid source cells.")
    levels <- unique(cells)
    list(source = src$source, files = src$cube_files, manifest = manifest,
      cells = levels, index = match(cells, levels))
  })
  stopifnot(setequal(names(sources), c("aod", "era5_land", "blh")))
  state <- new.env(parent = emptyenv())
  state$blueprint <- blueprint; state$gid <- map$gid; state$sources <- sources
  state$month <- contract$month; state$row <- row; state$audit <- audit
  state$schema <- daily_model_schema()
  paths <- unique(c(contract$cell_map_file, unlist(lapply(sources, `[[`, "files"))))
  state$file_state <- gate_c_file_state(paths)
  state$source_hashes <- gate_b_snapshot(paths)
  state$static_missing <- sum(rowSums(is.na(blueprint[daily_static_predictor_terms()])) > 0L)
  class(state) <- "daily_2023_prepared_chunk"
  lockEnvironment(state, bindings = TRUE)
  state
}

daily_2023_assert_prepared <- function(prepared, date) {
  daily_2023_assert_date(date, prepared$month)
  if (!inherits(prepared, "daily_2023_prepared_chunk") || !environmentIsLocked(prepared) ||
      !all(vapply(ls(prepared), bindingIsLocked, logical(1), env = prepared)) ||
      !identical(prepared$schema, daily_model_schema())) stop("Unvalidated static state.")
}

daily_2023_materialize <- function(prepared, date) {
  daily_2023_assert_prepared(prepared, date)
  if (!identical(prepared$file_state, gate_c_file_state(prepared$file_state$path))) stop("Monthly source drift.")
  out <- data.frame(gid = prepared$gid, date = rep(date, length(prepared$gid)))
  # Preserve the accepted monthly-layer extraction and integer mapping exactly.
  for (src in prepared$sources) {
    raster <- terra::rast(daily_cube_file(src$files))
    for (variable in daily_source_terms(src$source)) {
      rows <- which(src$manifest$variable == variable)
      column <- match(date, src$manifest$date[rows])
      if (is.na(column)) stop("Date absent from cube.")
      values <- as.matrix(terra::extract(raster[[src$manifest$layer[rows]]], src$cells))
      out[[variable]] <- as.numeric(values[cbind(src$index, rep.int(column, length(src$index)))])
    }
  }
  out <- out[c("gid", "date", daily_predictor_terms())]
  daily_2023_assert_dynamic(out, prepared, date)
  out
}

daily_2023_assert_dynamic <- function(dynamic, prepared, date) {
  daily_2023_assert_prepared(prepared, date)
  if (!identical(names(dynamic), c("gid", "date", daily_predictor_terms())) ||
      !identical(dynamic$gid, prepared$gid) || !identical(dynamic$date, rep(date, length(prepared$gid)))) stop("Dynamic key/order drift.")
  daily_assert_numeric(dynamic, daily_predictor_terms())
  invisible(TRUE)
}

daily_2023_attach <- function(dynamic, prepared, date) {
  daily_2023_assert_dynamic(dynamic, prepared, date)
  out <- prepared$blueprint; out$date <- dynamic$date
  for (nm in daily_predictor_terms()) out[[nm]] <- dynamic[[nm]]
  out
}

daily_2023_prediction_threads <- function() {
  Sys.setenv(OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1", NUMEXPR_NUM_THREADS = "1",
    GDAL_NUM_THREADS = "1", OMP_NUM_THREADS = "5", OMP_THREAD_LIMIT = "5",
    OMP_MAX_ACTIVE_LEVELS = "1", OMP_NESTED = "FALSE")
  RhpcBLASctl::blas_set_num_threads(1L)
  RhpcBLASctl::omp_set_num_threads(5L)
  if (RhpcBLASctl::blas_get_num_procs() != 1L) stop("Effective BLAS must equal 1.")
  invisible(TRUE)
}

daily_2023_load_models <- function(paths) {
  stopifnot(length(paths) == 2L)
  models <- lapply(seq_along(paths), function(i) {
    model <- readRDS(paths[i]); p <- c("PM10", "PM25")[i]
    if (model$metadata$outcome != p || model$metadata$target_year != 2023L ||
        !identical(model$metadata$schema, daily_model_schema()) || model$metadata$scientific_release) stop("Model metadata drift.")
    engine <- workflows::extract_fit_engine(model$workflow)
    cfg <- xgboost::xgb.config(engine)
    stopifnot(cfg$learner$gradient_booster$gbtree_model_param$num_trees == "300", cfg$learner$generic_param$nthread == "40")
    xgboost::xgb.model.parameters(engine) <- list(nthread = 5L)
    model$workflow$fit$fit$fit <- engine
    model$source <- list(path = paths[i], sha256 = daily_2023_sha(paths[i]))
    model
  })
  stats::setNames(models, c("PM10", "PM25"))
}

daily_2023_predict <- function(batch, prepared, date, models) {
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
    daily_2023_prediction_threads()
    engine <- workflows::extract_fit_engine(models[[p]]$workflow)
    stopifnot(xgboost::xgb.config(engine)$learner$generic_param$nthread == "5")
    seconds <- system.time(values <- predict(models[[p]]$workflow, new_data = predictors)$.pred)["elapsed"]
    daily_2023_prediction_threads()
    stopifnot(xgboost::xgb.config(engine)$learner$generic_param$nthread == "5", length(values) == nrow(batch), all(is.finite(values)))
    out[[p]] <- values
    timing[[p]] <- list(seconds = unname(seconds), blas_before = 1L, blas_after = 1L,
      xgb_nthread = 5L, finite = length(values), negative = sum(values < 0))
  }
  list(data = out, timing = timing)
}

daily_2023_predict_month <- function(contract, catalog, model_files, configuration,
                                     january_gate = NULL) {
  cfg <- daily_2023_read_config(configuration); daily_2023_contract(contract)
  if (contract$month != "2023-01") {
    if (is.null(january_gate) || jsonlite::read_json(january_gate)$status != "PASS") stop("January equivalence gate required.")
  }
  descriptor <- catalog[[contract$chunk_id]]
  if (is.null(descriptor)) stop("Missing annual static match.")
  directory <- daily_2023_attempt(cfg$root, file.path("predictions", contract$month, contract$chunk_id))
  daily_2023_prediction_threads()
  prepared <- daily_2023_prepare_chunk(contract, descriptor)
  models <- daily_2023_load_models(model_files)
  january <- NULL
  if (contract$month == "2023-01") {
    january <- read.csv("daehoon/data/daily_xgb_gate_d/year=2023/month=01/run=all593_parallel8x5_v3_001/partition_index.csv")
    january <- january[january$position == descriptor$row$position, ]
    stopifnot(nrow(january) == 31L)
  }
  outputs <- character(); records <- list(); memory_history <- NULL
  for (day in as.character(daily_2023_dates(contract$month))) {
    date <- as.Date(day); start <- proc.time()
    daily_2023_disk_guard(directory)
    tm <- system.time(dynamic <- daily_2023_materialize(prepared, date))["elapsed"]
    tj <- system.time(batch <- daily_2023_attach(dynamic, prepared, date))["elapsed"]
    result <- daily_2023_predict(batch, prepared, date, models)
    if (!is.null(january)) {
      reference <- january$path[match(day, january$date)]
      expected <- as.data.frame(nanoparquet::read_parquet(reference))
      manifest <- jsonlite::read_json(paste0(reference, ".json"))
      stopifnot(daily_2023_sha(reference) == manifest$sha256)
      gate_c_equal(result$data, expected)
      rm(expected)
    }
    path <- file.path(directory, paste0(day, ".parquet"))
    ts <- system.time({
      nanoparquet::write_parquet(result$data, paste0(path, ".partial"), compression = "zstd")
      gate_c_equal(as.data.frame(nanoparquet::read_parquet(paste0(path, ".partial"))), result$data)
      stopifnot(file.rename(paste0(path, ".partial"), path))
    })["elapsed"]
    missing <- vapply(batch[daily_predictor_terms()], function(x) sum(is.na(x)), 0)
    audit <- prepared$audit; audit$per_predictor <- as.list(audit$per_predictor)
    record <- list(status = "PASS", chunk_id = contract$chunk_id, position = descriptor$row$position,
      annual_branch = descriptor$row$annual_branch, date = day, rows = nrow(batch),
      fields = names(result$data), schema = daily_model_schema(), models = lapply(models, `[[`, "source"),
      static_normalization = audit, static_missing_rows = prepared$static_missing,
      source_hashes = prepared$source_hashes, predictor_missing = as.list(missing),
      any_era5_missing = sum(rowSums(is.na(batch[daily_era5_land_terms()])) > 0),
      any_dynamic_missing = sum(rowSums(is.na(batch[daily_predictor_terms()])) > 0),
      timing = result$timing, materialization_seconds = unname(tm), attach_seconds = unname(tj),
      serialization_seconds = unname(ts), sha256 = daily_2023_sha(path), bytes = file.info(path)$size,
      label = cfg$label, estimand = cfg$estimand, release = cfg$release, scientific_release = FALSE,
      january_exact = !is.null(january), identity_preserved = TRUE)
    rm(batch, dynamic, result); gc()
    rss <- gate_b_rss()
    if (rss[[1]] > 16384) stop("Daily worker post-GC RSS exceeds 16 GiB.")
    if (rss[[1]] > 12288) warning("Daily worker RSS above 12 GiB.")
    record$rss <- as.list(rss); record$elapsed <- as.list(proc.time() - start)
    observation <- data.frame(position = length(records) + 1L, chunk_id = contract$chunk_id,
      phase = "D1", session = directory, cells = descriptor$row$cells, post_gc_mib = rss[[1]])
    decision <- gate_d_v2_memory(memory_history, observation)
    record$memory_guard <- decision
    if (decision$decision == "STOP") {
      daily_2023_atomic_json(record, paste0(path, ".failed_memory.json"))
      stop(decision$reason)
    }
    memory_history <- rbind(memory_history, observation)
    json <- daily_2023_atomic_json(record, paste0(path, ".json"))
    outputs <- c(outputs, path, json); records[[day]] <- record
  }
  if (!identical(prepared$file_state, gate_c_file_state(prepared$file_state$path)) ||
      daily_2023_sha(descriptor$path) != descriptor$sha256) stop("Source changed during prediction.")
  summary <- daily_2023_atomic_json(list(status = "PASS", chunk_id = contract$chunk_id,
    month = contract$month, dates = names(records), rows = sum(vapply(records, `[[`, 0, "rows")),
    partitions = outputs[endsWith(outputs, ".parquet")], normalization = audit), file.path(directory, "month.json"))
  c(outputs, summary)
}

daily_2023_prediction_catalog <- function(january, remaining) {
  paths <- unlist(c(january, remaining), use.names = FALSE)
  partitions <- paths[endsWith(paths, ".parquet")]
  stopifnot(length(partitions) == 216445L, !anyDuplicated(partitions))
  dates <- sub("[.]parquet$", "", basename(partitions))
  stopifnot(setequal(dates, as.character(daily_2023_dates())), all(table(dates) == 593L))
  paths
}
