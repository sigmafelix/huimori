assert_daily_monitor_threads <- function(nthread = 40L) {
  if (!requireNamespace("RhpcBLASctl", quietly = TRUE)) stop("BLOCKED: RhpcBLASctl is required for effective BLAS verification.")
  blas <- RhpcBLASctl::blas_get_num_procs()
  if (length(blas) != 1L || is.na(blas) || blas != 1L) stop("BLOCKED: effective BLAS threads must equal 1.")
  if (!identical(as.integer(nthread), 40L)) stop("BLOCKED: requested XGBoost nthread must equal 40.")
  if (requireNamespace("foreach", quietly = TRUE) && foreach::getDoParRegistered() &&
      (foreach::getDoParWorkers() != 1L || !identical(foreach::getDoParName(), "doSEQ"))) {
    stop("BLOCKED: parallel foreach backend is registered.")
  }
  if (requireNamespace("future", quietly = TRUE) && !inherits(future::plan(), "sequential")) {
    stop("BLOCKED: future backend must be sequential.")
  }
  cfg <- daily_monitor_validation_config()
  if (cfg$workers != 1L || cfg$allow_par) stop("BLOCKED: model execution must be sequential.")
  list(effective_blas_threads = as.integer(blas), requested_xgb_nthread = as.integer(nthread),
       workers = 1L, parallel_backend = FALSE)
}

setup_daily_monitor_threads <- function() {
  if (!requireNamespace("RhpcBLASctl", quietly = TRUE)) stop("BLOCKED: RhpcBLASctl is required for effective BLAS control.")
  do.call(Sys.setenv, as.list(daily_xgb_thread_environment()))
  # R may initialize OpenBLAS before script-level environment changes take effect.
  RhpcBLASctl::blas_set_num_threads(1L)
  assert_daily_monitor_threads()
}

daily_monitor_validation_config <- function() {
  list(label = "NONPRODUCTION_REDUCED_VALIDATION", year = 2023L, folds = 5L,
       trees = 300L, tree_depth = 6L, min_n = 10L, learn_rate = 0.05,
       seed = 20260911L, spatial_seed = 20260728L, nstart = 100L,
       nthread = 40L, device = "cpu", workers = 1L, allow_par = FALSE,
       missing_policy = "native_NA_no_imputation_no_predictor_row_filter")
}

read_daily_monitor_validation_inputs <- function() {
  meta <- utils::read.delim("_targets/meta/meta", sep = "|", quote = "", colClasses = "character")
  names_read <- character()
  children <- function(parent) {
    row <- meta[meta$name == parent, ]
    if (nrow(row) != 1L || !nzchar(row$children)) stop("Missing parent metadata: ", parent)
    strsplit(row$children, "*", fixed = TRUE)[[1L]]
  }
  read <- function(name) {
    row <- meta[meta$name == name, ]
    if (nrow(row) != 1L || nzchar(row$error)) stop("Missing/failed input: ", name)
    names_read <<- unique(c(names_read, name))
    targets::tar_read_raw(name, store = "_targets")
  }
  months <- read("chr_months_spatial")
  years <- read("int_years_spatial")
  daily_model_predictor_terms(read("chr_terms_x"))
  monthly <- children("df_feat_correct_merged_daily")
  annual_names <- children("df_feat_correct_merged")
  if (length(monthly) != length(months) || length(annual_names) != length(years)) stop("Branch/calendar mismatch.")
  wanted <- sprintf("2023-%02d", 1:12)
  index <- match(wanted, months)
  if (anyNA(index) || sum(years == 2023L) != 1L) stop("Required 2023 input branches missing.")
  parts <- lapply(seq_along(index), function(i) {
    x <- as.data.frame(read(monthly[[index[[i]]]]))
    daily_check_dates(x, month = wanted[[i]])
    x
  })
  daily <- do.call(rbind, parts)
  rownames(daily) <- NULL
  annual <- as.data.frame(read(annual_names[[which(years == 2023L)]]))
  joined <- join_correct_daily_annual_features(daily, annual)
  assert_full_daily_monitor_year(joined)
  stopifnot(nrow(joined) == nrow(daily), identical(joined$PM10, daily$PM10),
            identical(joined$PM25, daily$PM25))
  paths <- c("_targets/meta/meta", file.path("_targets", "objects", names_read))
  list(data = joined, snapshot = daily_smoke_file_snapshot(paths), branches = names_read)
}

assert_full_daily_monitor_year <- function(data) {
  daily_check_dates(data)
  daily_assert_keys(data, daily_key_cols())
  assert_daily_model_schema(data)
  expected <- seq(as.Date("2023-01-01"), as.Date("2023-12-31"), by = "day")
  if (length(unique(data$date)) != 365L || !setequal(data$date, expected)) stop("Expected exactly 365 calendar dates.")
  daily_assert_numeric(data, c("PM10", "PM25", "lon", "lat"))
  if (anyNA(data[c("lon", "lat")])) stop("Missing station coordinates.")
  invisible(data)
}

daily_monitor_missingness <- function(data) {
  predictor <- do.call(rbind, lapply(daily_model_predictor_terms(), function(term) {
    missing <- is.na(data[[term]])
    data.frame(predictor = term, rows = nrow(data), missing = sum(missing),
      percent_missing = 100 * mean(missing), stations_affected = length(unique(data$TMSID[missing])),
      dates_affected = length(unique(data$date[missing])))
  }))
  grouped <- function(group) {
    do.call(rbind, lapply(daily_predictor_terms(), function(term) {
      frame <- data.frame(group = group, missing = is.na(data[[term]]), rows = 1L)
      x <- stats::aggregate(frame[c("missing", "rows")], frame["group"], sum)
      x$percent_missing <- 100 * x$missing / x$rows
      x$predictor <- term
      x
    }))
  }
  monthly <- grouped(format(data$date, "%Y-%m")); names(monthly)[1] <- "month"
  station <- grouped(data$TMSID); names(station)[1] <- "TMSID"
  date <- grouped(format(data$date)); names(date)[1] <- "date"
  locations <- stats::aggregate(data[c("lon", "lat")], list(TMSID = data$TMSID), stats::median)
  station <- merge(station, locations, by = "TMSID", sort = FALSE)
  list(predictor = predictor, monthly = monthly, station = station, date = date)
}

make_daily_monitor_folds <- function(data) {
  cfg <- daily_monitor_validation_config()
  daily_assert_keys(data, daily_key_cols())
  meta <- data[c("TMSID", "TMSID2", "date", "lon", "lat")]
  meta$.row_id <- seq_len(nrow(meta))
  points <- sf::st_as_sf(meta, coords = c("lon", "lat"), crs = 4326)
  assigned <- xgb_kmeans_fold_assignment(points, v = 5L, id_col = "TMSID",
    crs = "EPSG:5179", seed = cfg$spatial_seed, nstart = cfg$nstart)
  meta$fold <- assigned$rows$fold_id[match(meta$.row_id, assigned$rows$.row_id)]
  meta$center_x <- assigned$rows$center_x
  meta$center_y <- assigned$rows$center_y
  validate_daily_monitor_folds(data, meta)
  list(rows = meta, units = assigned$units, centers = assigned$centers)
}

validate_daily_monitor_folds <- function(data, assignment) {
  daily_assert_keys(data, daily_key_cols())
  daily_assert_keys(assignment, daily_key_cols())
  if (nrow(data) != nrow(assignment) ||
      !identical(data[daily_key_cols()], assignment[daily_key_cols()])) stop("Fold row assignment changed.")
  if (anyNA(assignment$fold) || !setequal(assignment$fold, paste0("Fold", 1:5))) stop("Expected five nonempty folds.")
  groups <- unique(assignment[c("TMSID", "fold")])
  if (anyDuplicated(groups$TMSID)) stop("TMSID leakage across folds.")
  checks <- list()
  for (outcome in c("PM10", "PM25")) {
    valid <- is.finite(data[[outcome]])
    coverage <- integer(nrow(data))
    for (fold in paste0("Fold", 1:5)) {
      assessment <- valid & assignment$fold == fold
      analysis <- valid & assignment$fold != fold
      overlap <- intersect(data$TMSID[assessment], data$TMSID[analysis])
      n_train_groups <- length(unique(data$TMSID[analysis]))
      n_test_groups <- length(unique(data$TMSID[assessment]))
      pass <- !length(overlap) && sum(assessment) >= 2L && sum(analysis) >= 2L &&
        n_train_groups >= 2L && n_test_groups >= 2L
      if (!pass) stop("Invalid spatial fold diagnostic: ", outcome, " ", fold)
      coverage[assessment] <- coverage[assessment] + 1L
      checks[[length(checks) + 1L]] <- data.frame(pollutant = outcome, fold = fold,
        analysis_rows = sum(analysis), assessment_rows = sum(assessment),
        analysis_groups = n_train_groups, assessment_groups = n_test_groups,
        overlapping_groups = length(overlap), pass = pass)
    }
    if (any(coverage[valid] != 1L) || any(coverage[!valid] != 0L)) stop("Assessment coverage is not exactly once.")
  }
  do.call(rbind, checks)
}

daily_validation_metrics <- function(observed, predicted) {
  if (length(observed) != length(predicted) || !length(observed) ||
      any(!is.finite(observed)) || any(!is.finite(predicted))) stop("Invalid assessment predictions/outcomes.")
  residual <- predicted - observed
  sst <- sum((observed - mean(observed))^2)
  data.frame(n = length(observed), rmse = sqrt(mean(residual^2)), mae = mean(abs(residual)),
    r2 = if (sst > 0) 1 - sum(residual^2) / sst else NA_real_, bias = mean(residual),
    observed_mean = mean(observed), predicted_mean = mean(predicted),
    observed_sd = stats::sd(observed), predicted_sd = stats::sd(predicted),
    observed_min = min(observed), observed_max = max(observed),
    predicted_min = min(predicted), predicted_max = max(predicted),
    unique_predictions = length(unique(predicted)), repeated_fraction = 1 - length(unique(predicted)) / length(predicted),
    largest_prediction_mass = max(tabulate(match(predicted, unique(predicted)))) / length(predicted),
    near_constant = length(predicted) > 1L && stats::sd(predicted) <= max(1e-8, stats::sd(observed) * 1e-6))
}

daily_validation_group_metrics <- function(oof, groups) {
  split_ids <- interaction(oof[groups], drop = TRUE, lex.order = TRUE)
  do.call(rbind, lapply(split(seq_len(nrow(oof)), split_ids), function(idx) {
    cbind(oof[idx[1L], groups, drop = FALSE], daily_validation_metrics(oof$observed[idx], oof$predicted[idx]))
  }))
}

daily_validation_memory <- function() {
  if (!file.exists("/proc/self/status")) return(NA_real_)
  line <- readLines("/proc/self/status")
  value <- line[startsWith(line, "VmHWM:")]
  if (length(value) != 1L) return(NA_real_)
  as.numeric(strsplit(trimws(sub("VmHWM:", "", value)), "[[:space:]]+")[[1L]][1L]) / 1024
}

fit_daily_monitor_validation <- function(training, outcome, fold) {
  cfg <- daily_monitor_validation_config()
  daily_check_dates(training)
  daily_assert_keys(training, daily_key_cols())
  assert_daily_model_schema(training)
  if (!outcome %in% c("PM10", "PM25") || any(!is.finite(training[[outcome]]))) stop("Invalid validation outcome.")
  terms <- daily_model_predictor_terms()
  model_data <- training[c(terms, outcome)]
  rownames(model_data) <- NULL
  formula <- stats::reformulate(terms, outcome)
  spec <- parsnip::boost_tree(mode = "regression", trees = cfg$trees, tree_depth = cfg$tree_depth,
                              min_n = cfg$min_n, learn_rate = cfg$learn_rate) |>
    parsnip::set_engine("xgboost", device = "cpu", nthread = 40L, seed = cfg$seed)
  workflow <- workflows::workflow() |>
    workflows::add_recipe(recipes::recipe(formula, model_data)) |>
    workflows::add_model(spec)
  timing <- system.time({
    fit <- with_daily_xgb_threads({
      set.seed(cfg$seed)
      thread_before <- assert_daily_monitor_threads(rlang::eval_tidy(spec$eng_args$nthread))
      fitted <- generics::fit(workflow, model_data)
      thread_after <- assert_daily_monitor_threads(cfg$nthread)
      fitted
    })
  })
  molded <- as.data.frame(workflows::extract_mold(fit)$predictors)
  if (!identical(molded, model_data[terms])) stop("BLOCKED: native NA predictor pathway changed data.")
  engine <- xgboost::xgb.config(workflows::extract_fit_engine(fit))
  if (as.integer(engine$learner$generic_param$nthread) != 40L ||
      engine$learner$generic_param$device != "cpu" ||
      as.integer(engine$learner$learner_model_param$num_feature) != 113L) stop("BLOCKED: engine configuration mismatch.")
  metadata <- list(label = cfg$label, outcome = outcome, target_year = 2023L, fold = fold,
    configuration = cfg, schema = daily_model_schema(), formula = paste(deparse(formula), collapse = " "),
    training_rows = nrow(training), training_groups = length(unique(training$TMSID)),
    training_range = format(range(training$date)), grouping = "TMSID", native_na_verified = TRUE,
    engine_nthread = as.integer(engine$learner$generic_param$nthread),
    engine_device = engine$learner$generic_param$device,
    blas_threads_before = thread_before$effective_blas_threads,
    requested_xgb_nthread = thread_before$requested_xgb_nthread,
    blas_threads_after = thread_after$effective_blas_threads,
    parallel_backend = thread_before$parallel_backend,
    thread_environment = as.list(daily_xgb_thread_environment()), elapsed_fit_seconds = unname(timing["elapsed"]),
    workflow_bytes = as.numeric(object.size(fit)), process_high_water_mib = daily_validation_memory())
  list(workflow = fit, metadata = metadata)
}

evaluate_daily_monitor_oof <- function(oof) {
  if (any(!is.finite(oof$predicted))) stop("BLOCKED: nonfinite OOF prediction.")
  daily_assert_keys(oof, c("pollutant", "TMSID", "TMSID2", "date"))
  oof$month <- format(oof$date, "%Y-%m")
  oof$quarter <- paste0("Q", (as.integer(format(oof$date, "%m")) - 1L) %/% 3L + 1L)
  metrics <- list(overall = daily_validation_group_metrics(oof, "pollutant"),
    fold = daily_validation_group_metrics(oof, c("pollutant", "fold")),
    monthly = daily_validation_group_metrics(oof, c("pollutant", "month")),
    quarterly = daily_validation_group_metrics(oof, c("pollutant", "quarter")),
    date = daily_validation_group_metrics(oof, c("pollutant", "date")),
    site_type = daily_validation_group_metrics(oof, c("pollutant", "site_type")))
  metrics$missingness <- do.call(rbind, lapply(c("aod_status", "era5_status", "dynamic_status"), function(group) {
    x <- daily_validation_group_metrics(oof, c("pollutant", group))
    names(x)[2L] <- "availability"
    x$stratum <- group
    x
  }))
  metrics$residual_quantiles <- do.call(rbind, lapply(split(oof, oof$pollutant), function(x) {
    probs <- c(0, .01, .05, .25, .5, .75, .95, .99, 1)
    data.frame(pollutant = x$pollutant[1], probability = probs,
                residual = unname(stats::quantile(x$predicted - x$observed, probs)))
  }))
  metrics$extreme_residuals <- do.call(rbind, lapply(split(oof, oof$pollutant), function(x) {
    x$residual <- x$predicted - x$observed
    head(x[order(-abs(x$residual)), ], 20L)
  }))
  if (any(metrics$overall$near_constant) || any(metrics$fold$near_constant)) stop("Degenerate OOF prediction variation.")
  metrics
}

write_daily_validation_json <- function(x, path) {
  if (file.exists(path) || file.exists(paste0(path, ".partial"))) stop("Refusing to overwrite validation output.")
  json <- jsonlite::toJSON(x, auto_unbox = TRUE, pretty = TRUE, digits = NA, na = "null")
  writeLines(json, paste0(path, ".partial"))
  if (!file.rename(paste0(path, ".partial"), path)) stop("Could not publish validation JSON.")
}
