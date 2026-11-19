load_january_smoke_models <- function(root) {
  models <- lapply(c("PM10", "PM25"), function(outcome) {
    path <- file.path(root, paste0(outcome, "_NONPRODUCTION_REDUCED_VALIDATION.rds"))
    if (!file.exists(path)) stop("Missing accepted model artifact.")
    model <- readRDS(path)
    if (!identical(model$metadata$label, "NONPRODUCTION_REDUCED_VALIDATION") ||
        model$metadata$target_year != 2023L || model$metadata$outcome != outcome ||
        !identical(model$metadata$schema, daily_model_schema()) ||
        !identical(model$metadata$configuration, daily_monitor_validation_config())) stop("Accepted model contract differs.")
    engine <- xgboost::xgb.config(workflows::extract_fit_engine(model$workflow))
    if (engine$learner$generic_param$nthread != "40" || engine$learner$generic_param$device != "cpu") stop("Loaded XGBoost thread/device mismatch.")
    model$source <- list(path = path, sha256 = digest::digest(file = path, algo = "sha256"))
    model
  })
  stats::setNames(models, c("PM10", "PM25"))
}

predict_january_batch <- function(batch, models, template, static_normalization = NULL) {
  assert_daily_model_schema(batch)
  assert_daily_grid_identity(batch, template)
  if (length(unique(batch$date)) != 1L || length(unique(batch$chunk_id)) != 1L ||
      !all(batch$date %in% january_smoke_dates())) stop("Expected one January chunk/date batch.")
  if (!identical(names(models), c("PM10", "PM25"))) stop("Expected exactly both pollutant models.")
  if (!is.null(static_normalization) &&
      (!identical(static_normalization$chunk_id, unique(batch$chunk_id)) ||
       !identical(static_normalization$schema_hash, daily_model_schema()$hash))) stop("Static normalization identity/schema mismatch.")
  metadata <- c("chunk_id", "gid", "grid_cell_id", "grid_template_id", "date", "x", "y")
  out <- batch[metadata]
  timings <- list()
  predictors <- batch[daily_model_predictor_terms()]
  for (outcome in names(models)) {
    model <- models[[outcome]]
    if (model$metadata$outcome != outcome || model$metadata$target_year != 2023L ||
        !identical(model$metadata$schema, daily_model_schema())) stop("Prediction model identity mismatch.")
    cfg <- xgboost::xgb.config(workflows::extract_fit_engine(model$workflow))
    if (cfg$learner$generic_param$nthread != "40") stop("XGBoost nthread must remain 40.")
    before <- assert_daily_monitor_threads()
    elapsed <- system.time({
      values <- with_daily_xgb_threads(predict(model$workflow, new_data = predictors)$.pred)
    })
    after <- assert_daily_monitor_threads()
    if (length(values) != nrow(batch) || any(!is.finite(values))) stop("Nonfinite/incomplete national prediction batch.")
    out[[outcome]] <- values
    timings[[outcome]] <- list(seconds = unname(elapsed["elapsed"]), blas_before = before$effective_blas_threads,
      blas_after = after$effective_blas_threads, xgb_nthread = 40L, negative = sum(values < 0))
  }
  if (!identical(out[metadata], batch[metadata])) stop("Prediction changed batch row order/identity.")
  list(data = out, timing = timings, static_normalization = static_normalization,
    missing = vapply(batch[daily_model_predictor_terms()], function(x) sum(is.na(x)), integer(1)))
}
