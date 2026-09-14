# Isolated monitor-only experiment. No production targets or grid routines execute here.
for (.source in c("daily_correct", "daily_native_cube", "daily_xgb_contracts",
                  "daily_xgb_inputs", "xgb_daily", "daily_xgb_monitor_validation")) {
  source(paste0("R/", .source, ".R"))
}
M1_BASE <- "daehoon/data/daily_xgb_validation/year=2023/run=monitor_fixed_300_threadfix_001"
M1_SCHEMA_HASH <- "0f31205d333c8e41dfcf1368e8cc98a54fd49b0ec44e5c237a3f0024e2b0779b"
M1_MODEL_HASHES <- c(PM10 = "7588b456714d12d92da92facdf6335c4c25962a4ca3f54a9886ef16257582a16",
                     PM25 = "2926dc25ffda59e011fa2aca8f2ed17269454a252d9abeec8b1dc22181ded1fc")
m1_sha <- function(path) digest::digest(file = path, algo = "sha256")
m1_json <- function(x, path) {
  if (file.exists(path)) stop("Exclusive artifact already exists: ", path)
  jsonlite::write_json(x, path, auto_unbox = TRUE, pretty = TRUE, digits = 17, na = "null")
}
m1_assert_equal_columns <- function(a, b, columns) {
  if (nrow(a) != nrow(b) || !all(columns %in% names(a)) || !all(columns %in% names(b))) stop("Input row/schema mismatch.")
  for (name in columns) {
    left <- a[[name]]; right <- b[[name]]
    # Parquet may encode Date with integer rather than double storage, without changing dates.
    if (inherits(left, "Date") && inherits(right, "Date")) {
      left <- as.numeric(left); right <- as.numeric(right)
    }
    if (!identical(left, right)) stop("Input values/order/NA mismatch: ", name)
  }
  TRUE
}
m1_params <- function() list(objective = "reg:squarederror", booster = "gbtree", device = "cpu",
  nthread = 40L, seed = 20260911L, max_depth = 6L, min_child_weight = 10,
  eta = 0.05, subsample = 1, colsample_bytree = 1, colsample_bylevel = 1,
  colsample_bynode = 1, alpha = 0, lambda = 1, gamma = 0, tree_method = "auto", eval_metric = "rmse")
m1_fixed_engine <- function(engine) {
  l <- engine$learner
  list(generic = l$generic_param, tree = l$gradient_booster$tree_train_param,
       train = l$gradient_booster$gbtree_train_param, objective = l$learner_train_param,
       metrics = l$metrics, features = l$learner_model_param$num_feature,
       boost_from_average = l$learner_model_param$boost_from_average)
}
m1_select_rounds <- function(best) {
  if (!length(best) || anyNA(best) || any(best < 1 | best > 1500 | best != floor(best))) stop("Invalid inner iterations.")
  as.integer(floor(stats::median(best) + 0.5))
}
m1_assert_split <- function(analysis, assessment, forbidden, station) {
  if (!length(analysis) || anyDuplicated(analysis) || anyDuplicated(assessment) ||
      length(intersect(analysis, assessment)) || length(intersect(c(analysis, assessment), forbidden)) ||
      length(intersect(station[analysis], station[assessment]))) stop("Inner/outer row or TMSID leakage.")
  invisible(TRUE)
}
m1_inner_groups <- function(analysis_metadata, seed, min_groups = 20L) {
  units <- aggregate(analysis_metadata[c("lon", "lat")], list(TMSID = analysis_metadata$TMSID), median)
  units <- units[order(units$TMSID, method = "radix"), ]; rownames(units) <- NULL
  points <- sf::st_transform(sf::st_as_sf(units, coords = c("lon", "lat"), crs = 4326), 5179)
  xy <- sf::st_coordinates(points)
  set.seed(seed)
  km <- stats::kmeans(xy, centers = 2L, nstart = 100L, iter.max = 100L)
  # Canonical west-to-east centroid labels make cluster numbering deterministic.
  label <- order(km$centers[, 1], km$centers[, 2], method = "radix")
  units$inner <- match(km$cluster, label)
  units$x <- xy[, 1]; units$y <- xy[, 2]
  if (any(table(units$inner) < min_groups) || nrow(unique(km$centers)) != 2L) stop("Unsafe geographic inner folds.")
  distances <- sapply(1:2, function(i) rowSums((xy - matrix(km$centers[label[i], ], nrow(xy), 2, byrow = TRUE))^2))
  if (any(units$inner != max.col(-distances, ties.method = "first"))) stop("Inner geographic assignment inconsistent.")
  units
}
m1_fit <- function(x, y, analysis, station, rounds, role, assessment = integer(), forbidden = integer()) {
  if (!role %in% c("inner", "outer", "full") || !identical(colnames(x), daily_model_predictor_terms())) stop("Invalid fit role/schema.")
  if (role != "inner" && length(assessment)) stop("Outer/full refit cannot use an evaluation set.")
  if (role == "inner" && !length(assessment)) stop("Inner evaluation required.")
  m1_assert_split(analysis, assessment, forbidden, station)
  if (any(!is.finite(y[c(analysis, assessment)])) || rounds < 1L || rounds > 1500L) stop("Invalid training outcome/budget.")
  stopifnot(!any(is.infinite(x)), !any(is.nan(x)))
  before <- assert_daily_monitor_threads()
  train <- xgboost::xgb.DMatrix(x[analysis, , drop = FALSE], label = y[analysis], missing = NA, nthread = 40L)
  evals <- list()
  if (role == "inner") evals <- list(training = train,
    validation = xgboost::xgb.DMatrix(x[assessment, , drop = FALSE], label = y[assessment], missing = NA, nthread = 40L))
  set.seed(20260911L)
  timing <- system.time(model <- xgboost::xgb.train(params = m1_params(), data = train,
    nrounds = as.integer(rounds), evals = evals, verbose = 0,
    early_stopping_rounds = if (role == "inner") 50L else NULL))
  after <- assert_daily_monitor_threads()
  config <- xgboost::xgb.config(model)
  trees <- as.integer(config$learner$gradient_booster$gbtree_model_param$num_trees)
  stopifnot(as.integer(config$learner$generic_param$nthread) == 40L,
            as.integer(config$learner$learner_model_param$num_feature) == 113L)
  curve <- if (role == "inner") as.data.frame(attr(model, "evaluation_log")) else NULL
  best <- if (role == "inner") as.integer(xgboost::xgb.attr(model, "best_iteration")) + 1L else as.integer(rounds)
  if (role == "inner") {
    stopifnot(best == which.min(curve$validation_rmse), trees == nrow(curve))
  } else stopifnot(trees == rounds, is.null(attr(model, "evaluation_log")))
  metadata <- list(label = "NONPRODUCTION_M1_EXPERIMENT", role = role, requested_rounds = rounds,
    trees = trees, best_round = best, best_score = if (role == "inner") curve$validation_rmse[best] else NULL,
    early_stopping_rounds = if (role == "inner") 50L else 0L,
    stopped_before_ceiling = role == "inner" && trees < rounds,
    training_rows = length(analysis), evaluation_rows = length(assessment),
    training_row_ids = analysis, evaluation_row_ids = assessment, forbidden_rows_sha256 = digest::digest(forbidden, algo = "sha256"),
    blas_before = before$effective_blas_threads, blas_after = after$effective_blas_threads,
    nthread = 40L, workers = 1L, wall_seconds = unname(timing["elapsed"]),
    cpu_seconds = unname(sum(timing[c("user.self", "sys.self")])),
    peak_rss_mib = daily_validation_memory(), model_bytes = as.numeric(object.size(model)), engine = config)
  list(model = model, metadata = metadata, curve = curve)
}
