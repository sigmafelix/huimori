# Run from the repository root:
# Rscript tools/run_models/multistage_models.R features.parquet [output_dir] [standardize]
# Or source this file and call run_multistage_models() to change the settings.
# Input: prepared station covariates, TMSID, site_type, and PM10/PM25.
# Include year when a station's site_type changes between years.
# Port rows are excluded before validation, splitting, and fitting either pollutant.
# Both stages use concentration units (no log transform or prediction clipping).
# Supplied years are pooled; NULL uses all years. Station holdout tests spatial
# transfer, not future-year forecasting. Learning-rate selection is conditional
# on nrounds and the initial tree settings, not a joint optimum of all parameters.

multistage_stations <- function(data) {
  keys <- c("TMSID", intersect("year", names(data)))
  assignments <- unique(data[c(keys, "site_type")])
  if (anyNA(assignments)) stop("Station IDs, years, and types cannot be missing.")
  if (anyDuplicated(assignments[keys])) {
    stop("Each TMSID must have one site_type per year; include year for type changes.")
  }
  # Use the latest year's type as the station's stratification label. Original
  # row-level types still determine which modeling stage each observation enters.
  if ("year" %in% keys) {
    assignments <- assignments[order(assignments$year, decreasing = TRUE), , drop = FALSE]
  }
  assignments[!duplicated(assignments$TMSID), c("TMSID", "site_type"), drop = FALSE]
}

# Allocate stations, not observations, with approximately equal proportions of
# latest site types. Rare types cannot appear in every fold; stations never split.
multistage_station_folds <- function(data, v, seed) {
  stations <- multistage_stations(data)
  if (nrow(stations) < v) stop("Not enough training stations for ", v, " folds.")
  set.seed(seed)
  stations$fold <- NA_integer_
  offset <- 0L
  for (type in unique(stations$site_type)) {
    indices <- which(stations$site_type == type)
    indices <- indices[sample.int(length(indices))]
    stations$fold[indices] <- (seq_along(indices) - 1L + offset) %% v + 1L
    offset <- offset + length(indices)
  }
  row_folds <- stations$fold[match(data$TMSID, stations$TMSID)]
  splits <- lapply(seq_len(v), function(fold) {
    rsample::make_splits(
      list(analysis = which(row_folds != fold), assessment = which(row_folds == fold)),
      data = data
    )
  })
  list(resamples = rsample::manual_rset(splits, paste0("Fold", seq_len(v))),
       stations = stations)
}

multistage_tune <- function(data, predictors, prefix, v, seed, nrounds,
                            nthread, learning_rates, grid_size, standardize = FALSE) {
  folds <- multistage_station_folds(data, v, seed)
  utils::write.csv(folds$stations, paste0(prefix, "_folds.csv"), row.names = FALSE)
  # Median imputation is learned separately inside each analysis fold.
  model_recipe <- recipes::recipe(
    stats::reformulate(predictors, response = ".response"), data = data
  ) |>
    recipes::step_impute_median(recipes::all_numeric_predictors())
  # Estimate centering/scaling on each analysis fold, then reuse it for assessment.
  if (standardize) {
    model_recipe <- model_recipe |>
      recipes::step_zv(recipes::all_predictors()) |>
      recipes::step_normalize(recipes::all_numeric_predictors())
  }
  metrics <- yardstick::metric_set(yardstick::rmse, yardstick::mae)
  selected_rate <- NULL
  for (phase in c("learning_rate", "other_parameters")) {
    message("  ", basename(prefix), ": ", phase)
    if (phase == "learning_rate") {
      spec <- parsnip::boost_tree(
        mode = "regression", trees = nrounds, learn_rate = tune::tune(),
        mtry = length(predictors), min_n = 5L, tree_depth = 6L
      )
      grid <- data.frame(learn_rate = learning_rates)
    } else {
      spec <- parsnip::boost_tree(
        mode = "regression", trees = nrounds, learn_rate = selected_rate,
        mtry = tune::tune(), min_n = tune::tune(), tree_depth = tune::tune(),
        loss_reduction = tune::tune(), sample_size = tune::tune()
      )
      set.seed(seed + 1L)
      grid <- dials::grid_space_filling(
        dials::mtry(range = c(1L, length(predictors))),
        dials::min_n(range = c(2L, 30L)),
        dials::tree_depth(range = c(2L, 10L)),
        dials::loss_reduction(range = c(-5, 1)),
        dials::sample_prop(range = c(0.6, 1)), size = grid_size
      )
    }
    workflow <- workflows::workflow() |>
      workflows::add_recipe(model_recipe) |>
      workflows::add_model(parsnip::set_engine(spec, "xgboost", nthread = nthread))
    set.seed(seed)
    tuning <- tune::tune_grid(
      workflow, resamples = folds$resamples, grid = grid, metrics = metrics,
      control = tune::control_grid(verbose = TRUE, save_workflow = TRUE)
    )
    scores <- tune::collect_metrics(tuning)
    if (!any(is.finite(scores$mean[scores$.metric == "rmse"]))) {
      stop("All candidates failed for ", prefix, " / ", phase, ". Inspect tuning notes.")
    }
    saveRDS(tuning, paste0(prefix, "_", phase, "_tuning.rds"))
    utils::write.csv(scores, paste0(prefix, "_", phase, "_metrics.csv"), row.names = FALSE)
    best <- tune::select_best(tuning, metric = "rmse")
    if (phase == "learning_rate") selected_rate <- best$learn_rate
  }
  fitted <- generics::fit(tune::finalize_workflow(workflow, best), data = data)
  list(model = fitted, parameters = cbind(learn_rate = selected_rate, best),
       predictors = predictors, standardize = standardize)
}

run_multistage_models <- function(
  parquet_file,
  output_dir = file.path("outputs", "multistage_models"),
  outcomes = c("PM10", "PM25"),
  years = NULL,
  baseline_types = c("Background", "Island", "Suburban"),
  baseline_predictors = NULL,
  full_predictors = NULL,
  v = 5L,
  test_prop = 0.2,
  learning_rates = 10^seq(-4, log10(0.3), length.out = 100L),
  grid_size = 100L,
  nrounds = 1000L,
  nthread = 10L,
  seed = 20260924L,
  standardize = FALSE
) {
  if (!is.logical(standardize) || length(standardize) != 1L || is.na(standardize)) {
    stop("standardize must be TRUE or FALSE.")
  }
  data <- as.data.frame(nanoparquet::read_parquet(parquet_file))
  if (!is.null(years) && !"year" %in% names(data)) stop("Filtering years requires a year column.")
  if (!is.null(years)) data <- data[data$year %in% years, , drop = FALSE]
  # Recognize both annual buffered predictors and the aod/blh point columns.
  # Explicit predictor arguments can include additional prepared covariates.
  if (is.null(baseline_predictors)) {
    baseline_predictors <- intersect(c("dem", "mtpi", "mtpi_1km"), names(data))
    baseline_predictors <- c(baseline_predictors,
                            grep("^(aod|blh)($|_)", names(data), value = TRUE))
  }
  if (is.null(full_predictors)) {
    full_predictors <- unique(c(
      baseline_predictors, intersect(c("dsm", "d_road"), names(data)),
      grep("^(landuse_frac_|lc_)", names(data), value = TRUE)
    ))
  }
  predictors <- unique(c(baseline_predictors, full_predictors))
  required <- c("TMSID", "site_type", intersect("year", names(data)), outcomes, predictors)
  missing <- setdiff(required, names(data))
  if (length(missing)) stop("Missing columns: ", paste(missing, collapse = ", "))
  data <- data[!data$site_type %in% "Port", , drop = FALSE]
  if (!length(baseline_predictors) || !length(full_predictors)) stop("Supply predictors for both stages.")
  if (length(intersect(predictors, c(outcomes, "PM10", "PM25", "TMSID", "site_type")))) {
    stop("Predictors must exclude outcomes and station identifiers.")
  }
  if (!all(vapply(data[predictors], is.numeric, logical(1)))) stop("Predictors must be numeric.")
  if (anyNA(data$TMSID) || anyNA(data$site_type)) stop("Station IDs and types cannot be missing.")
  if (v < 2L || v != as.integer(v) || test_prop <= 0 || test_prop >= 1) {
    stop("Use an integer v >= 2 and 0 < test_prop < 1.")
  }
  if (!length(learning_rates) || any(!is.finite(learning_rates) | learning_rates <= 0 | learning_rates > 1)) {
    stop("learning_rates must be finite and in (0, 1].")
  }
  data <- data[, unique(required), drop = FALSE]
  data$site_type <- as.character(data$site_type)
  multistage_stations(data)
  for (column in predictors) data[[column]][!is.finite(data[[column]])] <- NA_real_
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  results <- list()
  for (outcome in outcomes) {
    message("Fitting two-stage model: ", outcome)
    current <- data[is.finite(data[[outcome]]), , drop = FALSE]
    stations <- multistage_stations(current)
    # Reserve unseen stations stratified by latest type BEFORE either stage is tuned.
    set.seed(seed)
    stations$holdout <- FALSE
    for (type in unique(stations$site_type)) {
      indices <- which(stations$site_type == type)
      if (length(indices) < 2L) stop("Need >= 2 stations for holdout in site type: ", type)
      n_test <- min(length(indices) - 1L, max(1L, floor(length(indices) * test_prop)))
      stations$holdout[indices[sample.int(length(indices), n_test)]] <- TRUE
    }
    holdout <- stations$holdout[match(current$TMSID, stations$TMSID)]
    train <- current[!holdout, , drop = FALSE]
    test <- current[holdout, , drop = FALSE]
    baseline_rows <- train$site_type %in% baseline_types
    if (!any(baseline_rows) || all(baseline_rows)) stop("Need baseline and remaining site types.")
    prefix <- file.path(output_dir, outcome)
    utils::write.csv(stations, paste0(prefix, "_holdout_stations.csv"), row.names = FALSE)

    baseline <- train[baseline_rows, , drop = FALSE]
    baseline$.response <- baseline[[outcome]]
    residual <- train[!baseline_rows, , drop = FALSE]
    # Fail early if the requested folds cannot be formed in either stage.
    multistage_station_folds(baseline, v, seed)
    multistage_station_folds(residual, v, seed)
    stage1 <- multistage_tune(baseline, baseline_predictors, paste0(prefix, "_stage1"),
                             v, seed, nrounds, nthread, learning_rates, grid_size, standardize)
    # These rows never trained stage 1, though a station that changed type may
    # occur there in another year. Stage-2 CV is conditional on the frozen stage 1;
    # only the separate station holdout evaluates the whole pipeline independently.
    residual$.response <- residual[[outcome]] - predict(stage1$model, residual)$.pred
    stage2 <- multistage_tune(residual, full_predictors, paste0(prefix, "_stage2"),
                             v, seed + 1L, nrounds, nthread, learning_rates, grid_size, standardize)

    predictions <- test[c("TMSID", intersect("year", names(test)), "site_type")]
    predictions$truth <- test[[outcome]]
    predictions$baseline <- predict(stage1$model, test)$.pred
    # Apply the correction only to the types represented by stage 2.
    predictions$correction <- 0
    other <- !test$site_type %in% baseline_types
    if (any(other)) {
      predictions$correction[other] <- predict(stage2$model, test[other, , drop = FALSE])$.pred
    }
    predictions$combined <- predictions$baseline + predictions$correction
    scores <- do.call(rbind, lapply(c("all", unique(test$site_type)), function(type) {
      rows <- if (type == "all") rep(TRUE, nrow(test)) else test$site_type == type
      do.call(rbind, lapply(c("baseline", "combined"), function(model) {
        error <- predictions[[model]][rows] - predictions$truth[rows]
        data.frame(site_type = type, model = model, n = sum(rows),
                   rmse = sqrt(mean(error^2)), mae = mean(abs(error)))
      }))
    }))
    utils::write.csv(predictions, paste0(prefix, "_holdout_predictions.csv"), row.names = FALSE)
    utils::write.csv(scores, paste0(prefix, "_holdout_metrics.csv"), row.names = FALSE)
    bundle <- list(outcome = outcome, years = years, baseline_types = baseline_types,
                   stage1 = stage1, stage2 = stage2, holdout_stations = stations,
                   seed = seed, nrounds = nrounds, standardize = standardize,
                   holdout_metrics = scores)
    # Models are fitted on training stations only; the holdout remains untouched.
    saveRDS(bundle, paste0(prefix, "_models.rds"))
    results[[outcome]] <- scores
  }
  invisible(results)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) < 1L || length(args) > 3L) {
    stop("Usage: Rscript tools/run_models/multistage_models.R input.parquet [output_dir] [TRUE|FALSE]")
  }
  standardize <- if (length(args) == 3L) as.logical(args[[3L]]) else FALSE
  output_dir <- if (length(args) >= 2L) args[[2L]] else file.path(
    "outputs", if (isTRUE(standardize)) "multistage_models_standardized" else "multistage_models"
  )
  run_multistage_models(args[[1L]], output_dir, standardize = standardize)
}
