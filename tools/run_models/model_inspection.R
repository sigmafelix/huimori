# Standalone entry point for the corrected-location XGBoost pipeline.
# Run from the repository root:
# Rscript tools/run_models/model_inspection.R features.parquet outputs/model_inspection
# Or source this file and call run_model_inspection() with the desired years.
# Input: the 113 prepared predictors below, PM10/PM25, year, TMSID,
# site_type, and coords_google (latitude/longitude text, as in the pipeline).
# No feature extraction or targets store is required.
# Tune learning rate first, then fix it while tuning mtry, min_n, and tree_depth.

source("R/models.R")
source("R/xgb_cumulative.R")

run_model_inspection <- function(
  parquet_file,
  output_dir = file.path("outputs", "model_inspection"),
  target_years = 2018:2023,
  outcomes = c("PM10", "PM25"),
  grid_size = 100L,
  nrounds = 200L,
  nthread = 10L,
  learning_rate_grid_size = 20L
) {
  # Match chr_terms_x in inst/targets/4_tune_models.R.
  radii <- c(30, 100, 500, 2000)
  landuse_classes <- c(
    10, 11, 20, 51, 52, 61, 62, 71, 72, 81, 82,
    91, 120, 130, 140, 150, 181, 182, 183, 186, 187,
    190, 200, 210
  )
  landuse_class_ext <- unlist(lapply(radii, function(radius) {
      paste0("landuse_frac_", landuse_classes, "_", radius)
    }), use.names = FALSE)
  # match from the data
  landuse_class_ext <- landuse_class_ext[landuse_class_ext %in% names(nanoparquet::read_parquet(parquet_file))]
  predictors <- c(
    "dsm", "dem", "d_road", "mtpi", "mtpi_1km",
    landuse_class_ext,
    "aod", "blh"
    # paste0("aod_yearly_", radii),
    # paste0("blh_yearly_", radii)
  )

  # Normalize nanoparquet's table class for sf and ggplot2 compatibility.
  data <- as.data.frame(nanoparquet::read_parquet(parquet_file))
  # Treat missing land-use fractions as zero before modeling.
  for (column in names(data)[startsWith(names(data), "landuse_frac_")]) {
    data[[column]][is.na(data[[column]])] <- 0
  }
  required_columns <- c(
    "year", "TMSID", "site_type", "coords_google", outcomes, predictors
  )
  missing_columns <- setdiff(required_columns, names(data))
  if (length(missing_columns) > 0L) {
    stop("Parquet is missing required columns: ", paste(missing_columns, collapse = ", "))
  }
  # Parquet stores site_type as text; the training helper expects a factor.
  # Discard any serialized geometry: the helper rebuilds it from coords_google.
  data <- data[, required_columns, drop = FALSE]
  data$site_type <- factor(data$site_type)
  if (anyNA(data$TMSID) || any(as.character(data$TMSID) == "")) {
    stop("TMSID must be present for every row to keep stations in separate folds.")
  }
  if (length(target_years) == 0L || anyNA(target_years) ||
      any(target_years != as.integer(target_years))) {
    stop("target_years must contain non-missing integer years.")
  }
  if (length(outcomes) == 0L) {
    stop("Provide at least one outcome.")
  }
  if (!requireNamespace("finetune", quietly = TRUE)) {
    stop("Install the finetune package before tuning models.")
  }

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  results <- list()
  for (target_year in unique(target_years)) {
    for (outcome in unique(outcomes)) {
      key <- paste(outcome, target_year, sep = "_")
      message("Tuning and fitting ", key)
      formula <- stats::reformulate(predictors, response = outcome)
      # Train on all available years <= target_year, excluding missing outcomes.
      training_data <- prepare_xgb_correct_training_data(data, formula, target_year)
      resamples <- make_xgb_spatial_resamples(
        data = training_data,
        v = 5L,
        method = "kmeans",
        id_col = "TMSID",
        crs = "EPSG:5179",
        seed = 20260728L,
        nstart = 100L
      )
      diagnostics <- write_xgb_spatial_fold_diagnostics(
        data = training_data,
        resamples = resamples,
        output_dir = file.path(output_dir, "cv_blocks")
      )
      if (!all(diagnostics$validation$pass)) {
        stop("Spatial fold validation failed for ", key, ".")
      }
      prefix <- file.path(output_dir, key)
      tuning_path <- paste0(prefix, "_tuning.rds")
      model_path <- paste0(prefix, "_model.rds")
      metrics_path <- paste0(prefix, "_metrics.csv")
      learning_rate_tuning_path <- paste0(prefix, "_learning_rate_tuning.rds")
      learning_rate_metrics_path <- paste0(prefix, "_learning_rate_metrics.csv")
      model_recipe <- recipes::recipe(formula, data = training_data) |>
        recipes::step_zv(recipes::all_predictors()) |>
        recipes::step_normalize(recipes::all_predictors())

      for (stage in c("learning_rate", "other_parameters")) {
        message("  Tuning stage: ", stage)
        if (stage == "learning_rate") {
          # Hold the other parameters fixed to isolate the learning-rate search.
          model_spec <- parsnip::boost_tree(
            mode = "regression", trees = nrounds,
            mtry = 12L, min_n = 3L, tree_depth = 6L, learn_rate = tune::tune()
          )
          tuning_grid <- dials::grid_regular(
            dials::learn_rate(range = c(-4, -1), trans = scales::transform_log10()),
            levels = learning_rate_grid_size
          )
        } else {
          model_spec <- parsnip::boost_tree(
            mode = "regression", trees = nrounds,
            mtry = tune::tune(), min_n = tune::tune(), tree_depth = tune::tune(),
            learn_rate = best_learning_rate
          )
          # mtry is predictors sampled per split, starting at about 10% of 113.
          tuning_grid <- dials::grid_space_filling(
            dials::mtry(range = c(12L, length(predictors))),
            dials::min_n(range = c(3L, 31L)),
            dials::tree_depth(range = c(3L, 20L)),
            size = grid_size
          )
        }
        model_spec <- parsnip::set_engine(model_spec, "xgboost", device = "cpu", nthread = nthread)
        model_workflow <- workflows::workflow() |>
          workflows::add_recipe(model_recipe) |>
          workflows::add_model(model_spec)
        metrics_set <- yardstick::metric_set(yardstick::rmse, yardstick::mae)
        tuning_result <- tryCatch({
          finetune::tune_race_anova(
            model_workflow,
            resamples = resamples,
            grid = tuning_grid,
            metrics = metrics_set,
            control = finetune::control_race(
              verbose = TRUE, verbose_elim = TRUE,
              save_pred = TRUE, save_workflow = TRUE,
              burn_in = 4, alpha = 0.01, num_ties = 25L
            )
          )
        }, error = function(e) {
          result <- tune::tune_grid(
            model_workflow,
            resamples = resamples,
            grid = tuning_grid,
            metrics = metrics_set,
            control = tune::control_grid(save_pred = TRUE, save_workflow = TRUE)
          )
          attr(result, "error") <- e
          result
        })
        attr(tuning_result, "target_year") <- attr(training_data, "target_year")
        attr(tuning_result, "outcome") <- outcome
        attr(tuning_result, "cv_png") <- diagnostics$paths[["map_png"]]
        attr(tuning_result, "cv_diagnostics") <- diagnostics$paths
        attr(tuning_result, "tuning_stage") <- stage
        metrics <- collect_tune_metrics_with_metadata(tuning_result)
        metrics$tuning_stage <- stage
        if (stage == "learning_rate") {
          best_learning_rate <- tune::select_best(tuning_result, metric = "rmse")$learn_rate
          message("  Selected learning rate: ", signif(best_learning_rate, 5))
          saveRDS(tuning_result, learning_rate_tuning_path)
          utils::write.csv(metrics, learning_rate_metrics_path, row.names = FALSE)
        } else {
          attr(tuning_result, "learning_rate") <- best_learning_rate
          metrics$learn_rate <- best_learning_rate
          saveRDS(tuning_result, tuning_path)
          utils::write.csv(metrics, metrics_path, row.names = FALSE)
        }
      }

      # Refit the RMSE-selected configuration on all eligible training rows.
      fitted_model <- fit_best_tune_result(tuning_result, metric = "rmse")
      attr(fitted_model, "outcome") <- outcome
      attr(fitted_model, "target_year") <- attr(training_data, "target_year")
      saveRDS(fitted_model, model_path)
      results[[key]] <- list(
        learning_rate_tuning = learning_rate_tuning_path,
        learning_rate_metrics = learning_rate_metrics_path,
        tuning = tuning_path,
        model = model_path,
        metrics = metrics_path,
        cv_diagnostics = diagnostics$paths
      )
    }
  }
  invisible(results)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) < 1L || length(args) > 2L) {
    stop("Usage: Rscript tools/run_models/model_inspection.R input.parquet [output_dir]")
  }
  output_dir <- if (length(args) == 2L) args[[2L]] else file.path("outputs", "model_inspection")
  run_model_inspection(parquet_file = args[[1L]], output_dir = output_dir)
}
