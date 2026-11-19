daily_xgb_execution_config <- function() {
  list(controller = "controller_01", workers = 1L, nthread = 40L,
       allow_par = FALSE, device = "cpu", nonproduction = TRUE)
}

daily_xgb_thread_environment <- function() {
  c(OMP_NUM_THREADS = "40", OMP_THREAD_LIMIT = "40", OMP_MAX_ACTIVE_LEVELS = "1",
    OMP_NESTED = "FALSE", OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1",
    VECLIB_MAXIMUM_THREADS = "1", NUMEXPR_NUM_THREADS = "1", GDAL_NUM_THREADS = "1")
}

with_daily_xgb_threads <- function(expr) {
  vars <- daily_xgb_thread_environment()
  old <- Sys.getenv(names(vars), unset = NA_character_)
  on.exit({
    Sys.unsetenv(names(old)[is.na(old)])
    if (any(!is.na(old))) do.call(Sys.setenv, as.list(old[!is.na(old)]))
  }, add = TRUE)
  do.call(Sys.setenv, as.list(vars))
  force(expr)
}

prepare_xgb_correct_daily_training_data <- function(data, outcome, month = "2023-01") {
  if (!identical(month, "2023-01")) stop("Smoke training is restricted to January 2023.")
  if (length(outcome) != 1L || !outcome %in% c("PM10", "PM25")) stop("Invalid daily outcome.")
  data <- daily_check_dates(data, month = month)
  daily_assert_keys(data, daily_key_cols())
  assert_daily_model_schema(data)
  daily_assert_numeric(data, c(outcome, "lon", "lat"))
  if (anyNA(data[c("lon", "lat")])) stop("Missing spatial grouping coordinates.")
  out <- data[is.finite(data[[outcome]]), , drop = FALSE]
  if (!nrow(out)) stop("No valid daily outcomes.")
  out$.row_id <- seq_len(nrow(out))
  out$spatial_group <- as.character(out$TMSID)
  attr(out, "outcome") <- outcome
  attr(out, "target_year") <- 2023L
  attr(out, "training_month") <- month
  out
}

daily_xgb_smoke_spec <- function(trees = 20L, tree_depth = 3L, min_n = 5L,
                                learn_rate = 0.1, nthread = 40L) {
  if (length(nthread) != 1L || is.na(nthread) || nthread != 40L) stop("nthread must be 40.")
  values <- c(trees, tree_depth, min_n, learn_rate)
  if (length(values) != 4L || any(!is.finite(values)) || trees < 1 || trees > 20 ||
      trees != floor(trees) || tree_depth != 3L || min_n != 5L || learn_rate != 0.1) {
    stop("Only fixed small smoke hyperparameters are supported (1:20 trees, depth 3, min_n 5, rate 0.1).")
  }
  parsnip::boost_tree(mode = "regression", trees = as.integer(trees),
                      tree_depth = as.integer(tree_depth), min_n = as.integer(min_n),
                      learn_rate = learn_rate) |>
    parsnip::set_engine("xgboost", device = "cpu", nthread = 40L, seed = 20260911L)
}

fit_daily_xgb_smoke <- function(training, outcome, trees = 20L, tree_depth = 3L,
                                min_n = 5L, learn_rate = 0.1, nthread = 40L) {
  training <- daily_check_dates(training, month = "2023-01")
  daily_assert_keys(training, daily_key_cols())
  assert_daily_model_schema(training)
  if (!identical(attr(training, "outcome"), outcome) ||
      !outcome %in% c("PM10", "PM25") || any(!is.finite(training[[outcome]]))) {
    stop("Training outcome metadata/values are invalid.")
  }
  terms <- daily_model_predictor_terms()
  formula <- stats::reformulate(terms, response = outcome)
  model_data <- as.data.frame(training[c(terms, outcome)])
  rownames(model_data) <- NULL
  spec <- daily_xgb_smoke_spec(trees, tree_depth, min_n, learn_rate, nthread)
  # A no-step numeric recipe preserves NA and the exact raw predictor schema.
  recipe <- recipes::recipe(formula, data = model_data)
  workflow <- workflows::workflow() |>
    workflows::add_recipe(recipe) |>
    workflows::add_model(spec)
  fit <- with_daily_xgb_threads({
    set.seed(20260911L)
    generics::fit(workflow, data = model_data)
  })
  molded <- as.data.frame(workflows::extract_mold(fit)$predictors)
  if (!identical(molded, model_data[terms])) {
    stop("BLOCKED: recipe changed predictor rows, values, NA positions or schema.")
  }
  engine <- workflows::extract_fit_engine(fit)
  config <- xgboost::xgb.config(engine)
  engine_threads <- as.integer(config$learner$generic_param$nthread)
  if (!identical(engine_threads, 40L) || config$learner$generic_param$device != "cpu" ||
      as.integer(config$learner$learner_model_param$num_feature) != 113L) {
    stop("BLOCKED: effective XGBoost configuration differs from daily contract.")
  }
  metadata <- list(outcome = outcome, target_year = 2023L, schema_version = daily_model_schema()$version,
    schema_hash = daily_model_schema()$hash, predictor_terms = terms, formula = paste(deparse(formula), collapse = " "),
    feature_count = length(terms), training_rows = nrow(training),
    training_date_range = format(range(training$date)), training_month = "2023-01",
    stations = length(unique(training$TMSID)), locations = length(unique(training$TMSID2)),
    spatial_group = "TMSID (all relocated TMSID2 and repeated dates together)",
    validation = "nonproduction fixed fit; no tuning or CV fits", nonproduction = TRUE,
    missing_policy = "native_NA_no_imputation_no_predictor_row_filter",
    native_na_verified = TRUE, predictor_na_counts = vapply(molded, function(x) sum(is.na(x)), integer(1)),
    trees = as.integer(trees), tree_depth = as.integer(tree_depth), min_n = as.integer(min_n),
    learn_rate = learn_rate, seed = 20260911L, nthread = 40L, engine_nthread = engine_threads,
    engine_device = config$learner$generic_param$device, workers = 1L, allow_par = FALSE,
    thread_environment = as.list(daily_xgb_thread_environment()),
    package_versions = lapply(c("xgboost", "parsnip", "recipes", "workflows"), function(p) {
      stats::setNames(as.character(utils::packageVersion(p)), p)
    }))
  structure(list(workflow = fit, metadata = metadata), class = "daily_xgb_smoke_model")
}

predict_grid_daily_with_matching_models <- function(grid, models) {
  grid <- daily_check_dates(grid)
  assert_daily_model_schema(grid)
  assert_daily_grid_identity(grid)
  if (!is.list(models) || !length(models) ||
      !all(vapply(models, inherits, logical(1), "daily_xgb_smoke_model"))) stop("Invalid pollutant models.")
  outcomes <- vapply(models, function(m) m$metadata$outcome, character(1))
  if (anyDuplicated(outcomes)) stop("Duplicate pollutant models.")
  if (!setequal(outcomes, c("PM10", "PM25"))) stop("Missing/invalid pollutant models.")
  schema <- daily_model_schema()
  for (model in models) {
    if (!identical(model$metadata$target_year, 2023L)) stop("Wrong model year.")
    if (!identical(model$metadata$schema_hash, schema$hash) ||
        !identical(model$metadata$predictor_terms, schema$terms)) stop("Model schema mismatch.")
    if (!isTRUE(model$metadata$nonproduction) || model$metadata$nthread != 40L) stop("Invalid smoke model configuration.")
  }
  metadata <- setdiff(names(grid), daily_model_predictor_terms())
  if (any(c("PM10", "PM25") %in% metadata)) stop("Prediction outcome collision.")
  result <- grid[metadata]
  for (outcome in c("PM10", "PM25")) {
    model <- models[[match(outcome, outcomes)]]
    pred <- with_daily_xgb_threads(predict(model$workflow, new_data = grid[schema$terms]))
    if (nrow(pred) != nrow(grid) || any(!is.finite(pred$.pred))) stop("Invalid daily predictions.")
    result[[outcome]] <- pred$.pred
  }
  if (!identical(result[metadata], grid[metadata])) stop("Prediction metadata changed.")
  assert_daily_grid_identity(result)
  result
}
