# Original fixed model only; no experiment-selection or national pipeline entry points.
for (.file in c("daily_correct", "daily_native_cube", "daily_xgb_contracts", "daily_xgb_inputs",
                "xgb_daily", "daily_xgb_monitor_validation")) source(paste0("R/", .file, ".R"))
original_baseline_root <- function() "daehoon/data/daily_xgb_validation/year=2023/run=monitor_fixed_300_threadfix_001"
original_baseline_sha <- function(path) digest::digest(file = path, algo = "sha256")
original_baseline_json <- function(x, path) {
  if (file.exists(path)) stop("Exclusive output already exists: ", path)
  jsonlite::write_json(x, path, auto_unbox = TRUE, pretty = TRUE, digits = 17, na = "null")
}
original_baseline_equal <- function(a, b, columns) {
  if (nrow(a) != nrow(b) || !all(columns %in% names(a)) || !all(columns %in% names(b))) stop("Row/schema mismatch.")
  for (column in columns) {
    av <- a[[column]]; bv <- b[[column]]
    if (inherits(av, "Date") && inherits(bv, "Date")) { av <- as.numeric(av); bv <- as.numeric(bv) }
    if (!identical(av, bv)) stop("Value/order/NA mismatch: ", column)
  }
  TRUE
}
original_baseline_fixed_engine <- function(config) {
  # Only the training-derived base score may vary between geographic fits.
  config$learner$learner_model_param$base_score <- NULL
  config
}
original_baseline_assert_engine <- function(engine, reference) {
  cfg <- xgboost::xgb.config(engine)
  if (!identical(original_baseline_fixed_engine(cfg), original_baseline_fixed_engine(reference))) stop("M0 fixed engine drift.")
  stopifnot(as.integer(cfg$learner$gradient_booster$gbtree_model_param$num_trees) == 300L,
    as.integer(cfg$learner$generic_param$nthread) == 40L,
    cfg$learner$learner_train_param$objective == "reg:squarederror",
    is.null(xgboost::xgb.attr(engine, "best_iteration")))
  invisible(cfg)
}
original_baseline_folds <- function(data, path) {
  f <- as.data.frame(nanoparquet::read_parquet(path))
  original_baseline_equal(data, f, c(daily_key_cols(), "lon", "lat"))
  f$date <- data$date
  # Canonical plain frames avoid irrelevant attribute-order differences from Parquet.
  f <- as.data.frame(as.list(f)); d <- as.data.frame(as.list(data))
  rownames(f) <- rownames(d) <- NULL
  checks <- validate_daily_monitor_folds(d, f)
  stopifnot(identical(f$.row_id, seq_len(nrow(data))), all(checks$pass))
  list(rows = f, checks = checks)
}
fit_original_daily_baseline <- function(training, outcome, fold, reference) {
  cfg <- daily_monitor_validation_config()
  stopifnot(cfg$trees == 300L, cfg$tree_depth == 6L, cfg$min_n == 10L,
    cfg$learn_rate == .05, cfg$seed == 20260911L, cfg$nthread == 40L, cfg$workers == 1L, !cfg$allow_par)
  fit <- fit_daily_monitor_validation(training, outcome, fold)
  engine <- workflows::extract_fit_engine(fit$workflow)
  original_baseline_assert_engine(engine, reference)
  stopifnot(length(workflows::extract_recipe(fit$workflow)$steps) == 0L)
  mold <- workflows::extract_mold(fit$workflow)
  original_baseline_equal(as.data.frame(mold$predictors), training, daily_model_predictor_terms())
  stopifnot(identical(mold$outcomes[[outcome]], training[[outcome]]))
  fit$metadata$label <- "ORIGINAL_DAILY_2023_BASELINE"
  fit$metadata$estimand <- "RETROSPECTIVE"
  fit$metadata$release <- "NONPRODUCTION"
  fit$metadata$early_stopping <- FALSE
  fit$metadata$tuning <- FALSE
  fit$metadata$case_weights <- FALSE
  fit$metadata$configuration$label <- "ORIGINAL_DAILY_2023_BASELINE"
  fit$metadata$engine <- xgboost::xgb.config(engine)
  fit
}
