# Full run (not launched by sourcing this script):
# Rscript tools/run_models/ensemble_models.R --full input.parquet output_dir
# Small synthetic end-to-end test: Rscript tools/run_models/ensemble_models.R --smoke
# See ensemble_models.md for design, dependencies, and prediction instructions.

ensemble_dependencies <- function() {
  packages <- c("parsnip", "recipes", "workflows", "rsample", "tune", "yardstick",
                "xgboost", "bonsai", "lightgbm", "mgcv", "glmnet", "sf", "nanoparquet")
  missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing)) stop("Install missing R packages: ", paste(missing, collapse = ", "))
}

ensemble_dist2 <- function(a, b) {
  a <- as.matrix(a)
  b <- as.matrix(b)
  pmax(outer(rowSums(a^2), rowSums(b^2), "+") - 2 * tcrossprod(a, b), 0)
}

# Exact GP posterior mean: separate environmental, spatial, and temporal scales.
# The response is centered using training data only. Noise is a variance ratio.
ensemble_gp_kernel <- function(a, b, feature_scale, spatial_scale, time_scale, kernel) {
  env <- setdiff(names(a), c(".coord_x", ".coord_y", "year"))
  feature <- ensemble_dist2(a[env], b[env]) / feature_scale^2
  spatial <- ensemble_dist2(a[c(".coord_x", ".coord_y")],
                            b[c(".coord_x", ".coord_y")]) / spatial_scale^2
  temporal <- outer(a$year, b$year, "-")^2 / time_scale^2
  distance <- feature + spatial + temporal
  if (kernel == "rbf") return(exp(-distance / 2))
  radius <- sqrt(3 * distance)
  (1 + radius) * exp(-radius) # Matern 3/2
}

ensemble_fit_gp <- function(x, y, feature_scale = 3, spatial_scale = 100,
                            time_scale = 5, noise = 0.1, kernel = "matern") {
  x <- as.data.frame(x)
  settings <- list(feature_scale = feature_scale, spatial_scale = spatial_scale,
                   time_scale = time_scale, kernel = kernel)
  covariance <- do.call(ensemble_gp_kernel, c(list(a = x, b = x), settings))
  diag(covariance) <- diag(covariance) + noise + 1e-8
  factor <- chol(covariance)
  center <- mean(y)
  alpha <- backsolve(factor, forwardsolve(t(factor), as.numeric(y) - center))
  structure(list(x = x, alpha = alpha, center = center, settings = settings),
            class = "huimori_gp")
}

predict.huimori_gp <- function(object, newdata, ...) {
  covariance <- do.call(ensemble_gp_kernel,
                        c(list(a = as.data.frame(newdata), b = object$x), object$settings))
  as.numeric(object$center + covariance %*% object$alpha)
}

# Gaussian, fixed-distance MGWR with one bandwidth per coefficient (including
# the intercept). Bandwidths minimize leave-location-out partial-residual error
# inside backfitting. Excluding coincident coordinates also excludes their years.
# Ridge is a local numerical/predictive regularizer, not global OLS substitution.
ensemble_fit_mgwr <- function(x, y, bandwidths = c(10, 30, 100, 300, 1000),
                              ridge = 0.01, max_iter = 200L, tolerance = 1e-4) {
  x <- as.data.frame(x)
  coordinates <- as.matrix(x[c(".coord_x", ".coord_y")])
  terms <- setdiff(names(x), c(".coord_x", ".coord_y"))
  design <- cbind(`(Intercept)` = 1, as.matrix(x[terms]))
  distance <- ensemble_dist2(coordinates, coordinates)
  weights <- lapply(bandwidths, function(bw) exp(-distance / (2 * bw^2)))
  cv_weights <- lapply(weights, function(w) { w[distance < 1e-8] <- 0; w })
  beta <- solve(crossprod(design) + diag(ridge, ncol(design)), crossprod(design, y))
  contributions <- sweep(design, 2, as.numeric(beta), "*")
  selected <- rep(length(bandwidths), ncol(design))
  converged <- FALSE
  for (iteration in seq_len(max_iter)) {
    previous <- rowSums(contributions)
    previous_bandwidths <- selected
    for (j in seq_len(ncol(design))) {
      partial <- as.numeric(y) - rowSums(contributions) + contributions[, j]
      fit_coefficient <- function(w) {
        as.numeric(w %*% (design[, j] * partial)) /
          pmax(as.numeric(w %*% design[, j]^2) + ridge * rowSums(w), 1e-12)
      }
      errors <- vapply(cv_weights, function(w) {
        mean((partial - design[, j] * fit_coefficient(w))^2)
      }, numeric(1))
      selected[j] <- which.min(errors)
      contributions[, j] <- design[, j] * fit_coefficient(weights[[selected[j]]])
    }
    change <- sqrt(mean((rowSums(contributions) - previous)^2)) /
      max(stats::sd(as.numeric(y)), 1e-8)
    if (change < tolerance && identical(previous_bandwidths, selected)) {
      converged <- TRUE
      break
    }
  }
  if (!converged) warning("MGWR reached max_iter without convergence.")
  # At unseen locations, re-estimate each coefficient from its training-only
  # partial residual and chosen bandwidth. No new outcomes enter prediction.
  partials <- sweep(contributions, 1, as.numeric(y) - rowSums(contributions), "+")
  structure(list(coordinates = coordinates, terms = terms, design = design,
                 partials = partials, bandwidths = bandwidths[selected], ridge = ridge,
                 converged = converged, iterations = iteration), class = "huimori_mgwr")
}

predict.huimori_mgwr <- function(object, newdata, ...) {
  newdata <- as.data.frame(newdata)
  distance <- ensemble_dist2(newdata[c(".coord_x", ".coord_y")], object$coordinates)
  design <- cbind(1, as.matrix(newdata[object$terms]))
  prediction <- numeric(nrow(newdata))
  for (j in seq_len(ncol(design))) {
    # Subtract row minima to avoid kernel underflow far outside training coverage.
    exponent <- -distance / (2 * object$bandwidths[j]^2)
    weights <- exp(exponent - apply(exponent, 1, max))
    coefficients <- as.numeric(weights %*% (object$design[, j] * object$partials[, j])) /
      pmax(as.numeric(weights %*% object$design[, j]^2) + object$ridge * rowSums(weights), 1e-12)
    prediction <- prediction + design[, j] * coefficients
  }
  prediction
}

ensemble_fit_gam <- function(x, y, basis_size = 6L, spatial_basis = 30L, gamma = 1) {
  x <- as.data.frame(x)
  terms <- setdiff(names(x), c(".coord_x", ".coord_y"))
  # Limit rank to the available training rows, including small inner folds.
  budget <- max(3L, floor((nrow(x) - 5L) / (length(terms) + 2L)))
  smooth <- vapply(terms, function(term) {
    k <- min(basis_size, budget, length(unique(x[[term]])) - 1L)
    if (k < 3L) return(paste0("`", term, "`"))
    sprintf("s(`%s`, k = %d, bs = 'ts')", term, k)
  }, character(1))
  k_space <- min(spatial_basis, 2L * budget,
                 nrow(unique(x[c(".coord_x", ".coord_y")])) - 1L)
  if (k_space < 4L) stop("Too few distinct training locations for the spatial GAM.")
  formula <- stats::as.formula(paste(".response ~",
    paste(c(smooth, sprintf("s(.coord_x, .coord_y, k = %d, bs = 'ts')", k_space)), collapse = " + ")))
  environment(formula) <- asNamespace("mgcv")
  x$.response <- as.numeric(y)
  mgcv::gam(formula, data = x, method = "REML", select = TRUE, gamma = gamma,
            control = mgcv::gam.control(nthreads = 1L, ncv.threads = 1L))
}

# These custom engines use the same recipe/workflow/fit_resamples interface as
# XGBoost and LightGBM. Source this script before loading saved models to register
# prediction methods. Fixed engine arguments are searched by candidate workflows.
ensemble_register <- function() {
  if (!"huimori_spatial" %in% parsnip::get_model_env()$models) {
    parsnip::set_new_model("huimori_spatial")
    parsnip::set_model_mode("huimori_spatial", "regression")
    for (engine in c("gp", "mgwr", "gam")) {
      parsnip::set_model_engine("huimori_spatial", "regression", engine)
      parsnip::set_dependency("huimori_spatial", engine, "parsnip", mode = "regression")
      parsnip::set_fit("huimori_spatial", "regression", engine, list(
        interface = "data.frame", protect = c("x", "y"),
        func = c(fun = paste0("ensemble_fit_", engine)), defaults = list()
      ))
      parsnip::set_encoding("huimori_spatial", "regression", engine, list(
        predictor_indicators = "none", compute_intercept = FALSE,
        remove_intercept = FALSE, allow_sparse_x = FALSE
      ))
      parsnip::set_pred("huimori_spatial", "regression", engine, "numeric", list(
        pre = NULL, post = function(results, object) as.numeric(results),
        func = c(fun = "predict"),
        args = list(object = quote(object$fit), newdata = quote(new_data), type = "response")
      ))
    }
  }
}

ensemble_spatial_spec <- function(engine, settings) {
  spec <- parsnip::new_model_spec("huimori_spatial", args = list(), eng_args = NULL,
                                 mode = "regression", method = NULL, engine = NULL)
  do.call(parsnip::set_engine, c(list(object = spec, engine = engine), settings))
}

ensemble_coordinates <- function(data) {
  if (all(c(".coord_x", ".coord_y") %in% names(data))) return(data)
  if (!"coords_google" %in% names(data)) stop("Need coords_google as 'latitude, longitude'.")
  coordinates <- strsplit(as.character(data$coords_google), ",", fixed = TRUE)
  if (any(lengths(coordinates) != 2L)) stop("Invalid coords_google values.")
  coordinates <- matrix(as.numeric(trimws(unlist(coordinates))), ncol = 2L, byrow = TRUE)
  if (any(!is.finite(coordinates)) || any(abs(coordinates[, 1]) > 90) ||
      any(abs(coordinates[, 2]) > 180)) stop("Invalid latitude/longitude.")
  points <- sf::st_as_sf(data.frame(lon = coordinates[, 2], lat = coordinates[, 1]),
                         coords = c("lon", "lat"), crs = 4326)
  xy <- sf::st_coordinates(sf::st_transform(points, 5179)) / 1000
  data$.coord_x <- xy[, 1]
  data$.coord_y <- xy[, 2]
  data
}

ensemble_station_table <- function(data) {
  assignments <- unique(data[c("TMSID", "year", "site_type")])
  if (anyNA(assignments)) stop("TMSID, year and site_type must be present.")
  if (anyDuplicated(assignments[c("TMSID", "year")])) stop("Conflicting station-year site types.")
  assignments <- assignments[order(assignments$year, decreasing = TRUE), ]
  assignments[!duplicated(assignments$TMSID), c("TMSID", "site_type"), drop = FALSE]
}

# Expanding windows: all training years strictly precede the assessment year.
# Warm-up rows have no out-of-fold prediction and never enter the blender.
ensemble_folds <- function(data, v, min_train_years = 2L) {
  years <- sort(unique(data$year))
  if (length(years) <= min_train_years) stop("Too few years for forward-only CV.")
  assessment_years <- tail(years[seq_along(years) > min_train_years], v)
  fold <- match(data$year, assessment_years)
  splits <- lapply(assessment_years, function(year) rsample::make_splits(
    list(analysis = which(data$year < year), assessment = which(data$year == year)), data))
  audit <- do.call(rbind, lapply(seq_along(splits), function(i) {
    train <- rsample::analysis(splits[[i]])
    test <- rsample::assessment(splits[[i]])
    stopifnot(max(train$year) < min(test$year))
    data.frame(fold = i, train_start = min(train$year), train_end = max(train$year),
               assessment_year = assessment_years[i], n_train = nrow(train), n_test = nrow(test))
  }))
  list(resamples = rsample::manual_rset(splits, paste0("Year", assessment_years)),
       fold = fold, audit = audit,
       rows = cbind(data[c("TMSID", "year", "site_type")], assessment_fold = fold))
}

# Select ridge penalties using only earlier OOF years. A historical cascade
# prediction also selects its penalty without using that year's outcomes.
ensemble_blend <- function(oof, response, years) {
  lambdas <- 10^seq(3, -5, length.out = 80L)
  validation_years <- sort(unique(years))[-1L]
  if (!length(validation_years)) stop("Need at least two OOF years for temporal blending.")
  errors <- matrix(NA_real_, length(validation_years), length(lambdas))
  counts <- integer(length(validation_years))
  cascade <- rep(NA_real_, length(response))
  chosen <- rep(NA_real_, length(response))
  audit <- list()
  for (i in seq_along(validation_years)) {
    train <- years < validation_years[i]
    test <- years == validation_years[i]
    model <- glmnet::glmnet(oof[train, , drop = FALSE], response[train], alpha = 0,
      lower.limits = 0, lambda = lambdas, standardize = TRUE)
    predictions <- as.matrix(predict(model, newx = oof[test, , drop = FALSE], s = lambdas))
    # Fixed initial penalty when no earlier meta-validation year exists.
    best <- if (i == 1L) which.min(abs(log10(lambdas))) else
      which.min(colSums(errors[seq_len(i - 1L), , drop = FALSE]))
    cascade[test] <- predictions[, best]
    chosen[test] <- lambdas[best]
    errors[i, ] <- colSums((predictions - response[test])^2)
    counts[i] <- sum(test)
    audit[[i]] <- data.frame(train_end = max(years[train]), assessment_year = validation_years[i])
  }
  mse <- colSums(errors) / sum(counts)
  model <- glmnet::glmnet(oof, response, alpha = 0, lower.limits = 0,
                          lambda = lambdas, standardize = TRUE)
  list(fit = model, lambda = lambdas[which.min(mse)],
       cv = data.frame(lambda = lambdas, mse = mse),
       cascade = cascade, cascade_lambda = chosen, audit = do.call(rbind, audit))
}

ensemble_candidates <- function(smoke, seed) {
  set.seed(seed)
  counts <- if (smoke) c(xgboost = 4L, lightgbm = 4L, gam = 4L, gp = 4L) else
    c(xgboost = 80L, lightgbm = 80L, gam = 24L, gp = 48L)
  candidates <- list()
  for (family in names(counts)) {
    candidates[[family]] <- lapply(seq_len(counts[[family]]), function(i) {
      components <- if (smoke) 2L else sample(c(5L, 10L, 20L), 1L)
      settings <- switch(family,
        xgboost = list(trees = if (smoke) 20L else sample(c(500L, 1500L, 3000L), 1),
          tree_depth = if (smoke) 2L + i else sample(4:10, 1),
          min_n = if (smoke) 2L else sample(3:40, 1), learn_rate = 10^runif(1, -3, -0.5),
          sample_size = runif(1, 0.65, 1), loss_reduction = 10^runif(1, -5, 1)),
        lightgbm = list(trees = if (smoke) 20L else sample(c(500L, 1500L, 3000L), 1),
          tree_depth = if (smoke) 2L + i else sample(3:12, 1),
          min_n = if (smoke) 2L else sample(3:40, 1), learn_rate = 10^runif(1, -3, -0.5),
          sample_size = runif(1, 0.65, 1)),
        # mgwr = list(bandwidths = if (smoke) c(30, 100, 500) else
        #               c(5, 10, 20, 40, 80, 160, 320, 640, 2000),
        #             ridge = if (smoke) c(0.01, 0.1, 0.3, 1)[i] else 10^runif(1, -4, 0),
        #             max_iter = if (smoke) 100L else 200L),
        gam = list(basis_size = if (smoke) 3L else sample(c(4L, 6L, 10L), 1),
                   spatial_basis = if (smoke) 6L else sample(c(20L, 40L, 80L), 1),
                   gamma = if (smoke) c(1, 1.2, 1.4, 1.6)[i] else runif(1, 0.8, 1.5)),
        gp = list(feature_scale = if (smoke) c(1, 3, 5, 10)[i] else 10^runif(1, -0.5, 1.5),
                  spatial_scale = if (smoke) 100 else 10^runif(1, 1, 3),
                  time_scale = if (smoke) 3 else 10^runif(1, -0.3, 1.3),
                  noise = if (smoke) 0.1 else 10^runif(1, -4, 0),
                  kernel = if (i %% 2L) "rbf" else "matern")
      )
      list(id = paste0(family, "_", i), family = family, settings = settings,
           components = if (family %in% c("xgboost", "lightgbm")) NULL else components)
    })
  }
  candidates
}

ensemble_workflow <- function(candidate, data, predictors, nthread) {
  inputs <- c(predictors, "year", ".coord_x", ".coord_y")
  recipe <- recipes::recipe(stats::reformulate(inputs, ".response"), data = data) |>
    recipes::step_impute_median(recipes::all_numeric_predictors(), -year, -.coord_x, -.coord_y) |>
    recipes::step_zv(recipes::all_numeric_predictors(), -year, -.coord_x, -.coord_y)
  if (!is.null(candidate$components)) {
    recipe <- recipe |>
      recipes::step_normalize(recipes::all_numeric_predictors(), -year, -.coord_x, -.coord_y) |>
      recipes::step_pca(recipes::all_numeric_predictors(), -year, -.coord_x, -.coord_y,
                        num_comp = candidate$components)
  }
  if (candidate$family %in% c("xgboost", "lightgbm")) {
    spec <- do.call(parsnip::boost_tree, c(list(mode = "regression"), candidate$settings))
    if (candidate$family == "xgboost") {
      spec <- parsnip::set_engine(spec, "xgboost", nthread = nthread)
    } else {
      spec <- parsnip::set_engine(spec, "lightgbm", num_threads = nthread,
                                  deterministic = TRUE, force_col_wise = TRUE, verbose = -1)
    }
  } else {
    spec <- ensemble_spatial_spec(candidate$family, candidate$settings)
  }
  workflows::workflow() |> workflows::add_recipe(recipe) |> workflows::add_model(spec)
}

# Search complete workflows on identical inner folds. A candidate must succeed
# on every fold; failed candidates are recorded, never scored on partial folds.
ensemble_tune_families <- function(data, folds, candidates, predictors, output_dir, nthread, seed, cluster) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(folds$rows, file.path(output_dir, "rows.csv"), row.names = FALSE)
  utils::write.csv(folds$audit, file.path(output_dir, "temporal_splits.csv"), row.names = FALSE)
  models <- selected <- list()
  for (family in names(candidates)) {
    message("    Tuning ", family, " (", length(candidates[[family]]), " candidates)")
    # Parallelize candidates; keep each candidate's CV sequential to avoid
    # nested worker pools. Explicit seeds make scheduling order irrelevant.
    scores <- parallel::parLapplyLB(cluster, candidates[[family]], function(candidate,
                                   data, folds, predictors, nthread, seed) {
      started <- proc.time()
      workflow <- ensemble_workflow(candidate, data, predictors, nthread)
      set.seed(seed)
      result <- tryCatch(tune::fit_resamples(workflow, resamples = folds$resamples,
        metrics = yardstick::metric_set(yardstick::rmse, yardstick::mae),
        control = tune::control_resamples(save_pred = TRUE, allow_par = FALSE)), error = identity)
      score <- Inf
      notes <- ""
      if (inherits(result, "error")) {
        notes <- conditionMessage(result)
      } else {
        diagnostics <- tune::collect_notes(result)
        notes <- paste(unique(diagnostics$note), collapse = " | ")
        metrics <- tryCatch(tune::collect_metrics(result), error = function(e) NULL)
        rmse <- if (is.null(metrics)) data.frame() else metrics[metrics$.metric == "rmse", ]
        if (nrow(rmse) == 1L && rmse$n == nrow(folds$resamples) &&
            !any(diagnostics$type == "error") && !grepl("without convergence", notes)) {
          predictions <- tune::collect_predictions(result)
          expected <- which(!is.na(folds$fold))
          if (identical(sort(predictions$.row), expected) && all(is.finite(predictions$.pred))) {
            score <- sqrt(mean((predictions$.pred - predictions$.response)^2))
          }
        }
      }
      elapsed <- proc.time() - started
      data.frame(candidate = candidate$id, rmse = score, notes = notes, worker_pid = Sys.getpid(),
                 elapsed_seconds = unname(elapsed["elapsed"]),
                 cpu_seconds = unname(elapsed["user.self"] + elapsed["sys.self"]))
    }, data = data, folds = folds, predictors = predictors, nthread = nthread, seed = seed,
    chunk.size = 1L)
    scores <- do.call(rbind, scores)
    utils::write.csv(scores, file.path(output_dir, paste0(family, "_tuning.csv")), row.names = FALSE)
    if (!any(is.finite(scores$rmse))) stop("All ", family, " candidates failed; see ", output_dir)
    best <- candidates[[family]][[which.min(scores$rmse)]]
    selected[[family]] <- best
  }
  message("    Refitting selected families in parallel")
  models <- parallel::parLapply(cluster, selected, function(candidate, data, predictors, nthread, seed) {
    set.seed(seed)
    model <- generics::fit(ensemble_workflow(candidate, data, predictors, nthread), data = data)
    if (candidate$family == "mgwr" && !workflows::extract_fit_engine(model)$converged) {
      stop("Selected MGWR did not converge during refitting; increase max_iter in ensemble_candidates().")
    }
    model
  }, data = data, predictors = predictors, nthread = nthread, seed = seed)
  saveRDS(selected, file.path(output_dir, "selected_candidates.rds"))
  list(models = models, selected = selected)
}

ensemble_base_predictions <- function(models, data) {
  result <- vapply(models, function(model) as.numeric(predict(model, new_data = data)$.pred),
                   numeric(nrow(data)))
  if (any(!is.finite(result))) stop("Non-finite component predictions.")
  result
}

predict.huimori_ensemble <- function(object, new_data, ...) {
  if (is.null(object$training_end_year)) stop("Model has no temporal cutoff; refit with the temporal pipeline.")
  data <- ensemble_coordinates(as.data.frame(new_data))
  if (!"year" %in% names(data) || any(!is.finite(data$year)) ||
      any(data$year <= object$training_end_year)) {
    stop("Prediction years must be strictly later than training_end_year = ", object$training_end_year)
  }
  for (term in object$predictors) data[[term]][!is.finite(data[[term]])] <- NA_real_
  base <- ensemble_base_predictions(object$models, data)
  prediction <- as.numeric(predict(object$blender, newx = base, s = object$lambda))
  data.frame(.pred = prediction, base, check.names = FALSE)
}

run_ensemble_models <- function(
  parquet_file = "/Users/isong/Documents/df_feat_correct_merged.parquet",
  output_dir = file.path("outputs", "ensemble_models"),
  outcomes = c("PM10", "PM25"), predictors = NULL, years = NULL,
  outer_v = 5L, inner_v = 5L, test_prop = 0.2, nthread = 1L,
  seed = 20260927L, smoke = FALSE, data = NULL, workers = 4L, holdout_years = 1L
) {
  if (length(workers) != 1L || !is.finite(workers) || workers < 4L || workers != as.integer(workers)) {
    stop("workers must be an integer >= 4.")
  }
  if (length(holdout_years) != 1L || !is.finite(holdout_years) ||
      holdout_years < 1L || holdout_years != as.integer(holdout_years)) stop("holdout_years must be a positive integer.")
  ensemble_dependencies()
  ensemble_register()
  if (is.null(data)) data <- as.data.frame(nanoparquet::read_parquet(parquet_file))
  if (!all(c("TMSID", "year", "site_type", outcomes) %in% names(data))) stop("Missing required columns.")
  data <- data[!data$site_type %in% "Port", , drop = FALSE]
  if (!is.null(years)) data <- data[data$year %in% years, , drop = FALSE]
  data <- ensemble_coordinates(data)
  if (is.null(predictors)) {
    predictors <- unique(c(intersect(c("dem", "dsm", "d_road", "mtpi", "mtpi_1km", "gw_emission"), names(data)),
                            grep("^(aod|blh)($|_)|^(landuse_frac_|lc_)", names(data), value = TRUE)))
  }
  forbidden <- c(outcomes, "PM10", "PM25", "PM10_elt", "PM25_elt", "TMSID", "TMSID2",
                 "site_type", "site_type_elt", "branch_id", "ndays", "year", ".coord_x", ".coord_y")
  if (!length(predictors) || any(!predictors %in% names(data)) || any(predictors %in% forbidden)) {
    stop("Supply valid environmental predictors, excluding outcomes and metadata.")
  }
  if (!all(vapply(data[c(predictors, "year", ".coord_x", ".coord_y")], is.numeric, logical(1)))) {
    stop("Predictors, year, and projected coordinates must be numeric.")
  }
  if (any(!is.finite(as.matrix(data[c("year", ".coord_x", ".coord_y")])))) stop("Invalid year or coordinates.")
  for (term in predictors) data[[term]][!is.finite(data[[term]])] <- NA_real_
  if (any(vapply(data[predictors], function(x) all(is.na(x)), logical(1)))) stop("A predictor is entirely missing.")
  if (outer_v < 3L || inner_v < 2L || test_prop <= 0 || test_prop >= 1) stop("Use outer_v >= 3, inner_v >= 2 and 0 < test_prop < 1.")
  if (dir.exists(output_dir) && length(list.files(output_dir))) stop("Use an empty output directory to protect previous results.")
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  # Set thread limits BEFORE launching fresh R processes (BLAS may initialize at startup).
  cluster <- local({
    limits <- c(OMP_NUM_THREADS = as.character(nthread), OPENBLAS_NUM_THREADS = "1",
                MKL_NUM_THREADS = "1", VECLIB_MAXIMUM_THREADS = "1")
    previous <- setNames(Sys.getenv(names(limits), unset = NA_character_), names(limits))
    on.exit({
      Sys.unsetenv(names(previous)[is.na(previous)])
      if (any(!is.na(previous))) do.call(Sys.setenv, as.list(previous[!is.na(previous)]))
    })
    do.call(Sys.setenv, as.list(limits))
    parallel::makePSOCKcluster(workers, outfile = file.path(output_dir, "workers.log"))
  })
  on.exit(parallel::stopCluster(cluster), add = TRUE)
  functions <- ls(envir = environment(ensemble_register), pattern = "^ensemble_|^predict\\.huimori_")
  parallel::clusterExport(cluster, functions, envir = environment(ensemble_register))
  parallel::clusterCall(cluster, function(libraries, threads) {
    .libPaths(libraries)
    Sys.setenv(OMP_NUM_THREADS = threads, OPENBLAS_NUM_THREADS = "1",
               MKL_NUM_THREADS = "1", VECLIB_MAXIMUM_THREADS = "1")
    ensemble_dependencies()
    ensemble_register()
    NULL
  }, .libPaths(), as.character(nthread))
  message("Hyperparameter tuning: ", workers, " parallel workers; ", nthread, " thread(s) per tree fit")
  candidates <- ensemble_candidates(smoke, seed)
  saveRDS(list(predictors = predictors, candidates = candidates, outcomes = outcomes,
    years = years, outer_v = outer_v, inner_v = inner_v, test_prop = test_prop,
    seed = seed, nthread = nthread, workers = workers, holdout_years = holdout_years,
    smoke = smoke, input = parquet_file), file.path(output_dir, "configuration.rds"))
  results <- list()
  for (outcome in outcomes) {
    message("Outcome: ", outcome)
    current <- data[is.finite(data[[outcome]]), , drop = FALSE]
    current$.response <- current[[outcome]]
    stations <- ensemble_station_table(current)
    set.seed(seed)
    stations$holdout <- FALSE
    for (type in unique(stations$site_type)) {
      indices <- which(stations$site_type == type)
      if (length(indices) < 2L) stop("Need >= 2 stations per latest site type: ", type)
      count <- max(1L, min(length(indices) - 1L, floor(length(indices) * test_prop)))
      stations$holdout[indices[sample.int(length(indices), count)]] <- TRUE
    }
    holdout <- stations$holdout[match(current$TMSID, stations$TMSID)]
    observed_years <- sort(unique(current$year))
    if (length(observed_years) < holdout_years + 5L) {
      stop("Need at least five development years plus holdout_years for nested temporal CV.")
    }
    test_start <- min(tail(observed_years, holdout_years))
    development <- current[!holdout & current$year < test_start, , drop = FALSE]
    test <- current[holdout & current$year >= test_start, , drop = FALSE]
    if (!nrow(test)) stop("No reserved stations have observations in the future holdout years.")
    stopifnot(max(development$year) < min(test$year))
    folder <- file.path(output_dir, outcome)
    dir.create(folder)
    utils::write.csv(stations, file.path(folder, "holdout_stations.csv"), row.names = FALSE)
    outer <- ensemble_folds(development, outer_v, min_train_years = 3L)
    utils::write.csv(outer$rows, file.path(folder, "outer_rows.csv"), row.names = FALSE)
    utils::write.csv(outer$audit, file.path(folder, "outer_temporal_splits.csv"), row.names = FALSE)
    utils::write.csv(data.frame(train_end = max(development$year), test_start = min(test$year)),
                      file.path(folder, "holdout_temporal_split.csv"), row.names = FALSE)
    oof <- matrix(NA_real_, nrow(development), length(candidates), dimnames = list(NULL, names(candidates)))
    for (i in seq_len(nrow(outer$resamples))) {
      message("  Outer temporal fold ", i, "/", nrow(outer$resamples))
      train <- rsample::analysis(outer$resamples$splits[[i]])
      assessment <- rsample::assessment(outer$resamples$splits[[i]])
      inner <- ensemble_folds(train, inner_v)
      fitted <- ensemble_tune_families(train, inner, candidates, predictors,
        file.path(folder, paste0("outer_", i)), nthread, seed + i, cluster)
      oof[which(outer$fold == i), ] <- ensemble_base_predictions(fitted$models, assessment)
    }
    eligible <- which(!is.na(outer$fold))
    oof <- oof[eligible, , drop = FALSE]
    stopifnot(all(is.finite(oof)))
    oof_data <- development[eligible, , drop = FALSE]
    blender <- ensemble_blend(oof, oof_data$.response, oof_data$year)
    oof_table <- cbind(oof_data[c("TMSID", "year", "site_type", ".response")],
                       fold = outer$fold[eligible], as.data.frame(oof),
                       cascade_prediction = blender$cascade, cascade_lambda = blender$cascade_lambda)
    utils::write.csv(oof_table, file.path(folder, "nested_oof_predictions.csv"), row.names = FALSE)
    utils::write.csv(blender$cv, file.path(folder, "blender_cv.csv"), row.names = FALSE)
    utils::write.csv(blender$audit, file.path(folder, "blender_temporal_splits.csv"), row.names = FALSE)
    message("  Final tuning on all development stations")
    final <- ensemble_tune_families(development, ensemble_folds(development, inner_v),
      candidates, predictors, file.path(folder, "final_tuning"), nthread, seed, cluster)
    bundle <- structure(list(models = final$models, selected = final$selected,
      blender = blender$fit, lambda = blender$lambda, predictors = predictors,
      training_end_year = max(development$year), test_start_year = min(test$year),
      outcome = outcome, seed = seed, holdout_stations = stations), class = "huimori_ensemble")
    prediction <- predict(bundle, new_data = test)
    metrics <- do.call(rbind, lapply(names(prediction), function(model) {
      data.frame(model = if (model == ".pred") "ensemble" else model, n = nrow(test),
        rmse = yardstick::rmse_vec(test$.response, prediction[[model]]),
        mae = yardstick::mae_vec(test$.response, prediction[[model]]))
    }))
    utils::write.csv(cbind(test[c("TMSID", "year", "site_type", ".response")], prediction),
                      file.path(folder, "holdout_predictions.csv"), row.names = FALSE)
    utils::write.csv(metrics, file.path(folder, "holdout_metrics.csv"), row.names = FALSE)
    weights <- as.matrix(stats::coef(bundle$blender, s = bundle$lambda))
    utils::write.csv(data.frame(term = rownames(weights), coefficient = weights[, 1]),
                      file.path(folder, "ensemble_weights.csv"), row.names = FALSE)
    saveRDS(bundle, file.path(folder, "ensemble.rds"))
    # Verify persistence, including the LightGBM booster, before reporting success.
    restored <- readRDS(file.path(folder, "ensemble.rds"))
    stopifnot(isTRUE(all.equal(prediction, predict(restored, new_data = test), tolerance = 1e-8)))
    results[[outcome]] <- metrics
  }
  writeLines(capture.output(sessionInfo()), file.path(output_dir, "session_info.txt"))
  invisible(results)
}

ensemble_smoke_test <- function(output_dir = tempfile("ensemble_smoke_", tmpdir = "outputs")) {
  dir.create("outputs", showWarnings = FALSE)
  set.seed(71)
  stations <- data.frame(TMSID = sprintf("S%03d", 1:24),
    site_type = rep(c("Urban", "Suburban", "Island"), each = 8),
    .coord_x = runif(24, 900, 1150), .coord_y = runif(24, 1700, 1950))
  data <- stations[rep(seq_len(24), each = 8), ]
  data$year <- rep(2015:2022, 24)
  data$dem <- runif(192, 0, 500)
  data$aod <- runif(192)
  data$blh <- runif(192, 100, 1000)
  signal <- 10 + 8 * sin(data$aod * 2) + 0.01 * data$dem +
    0.02 * (data$.coord_x - 1000) + 0.5 * (data$year - 2015)
  data$PM10 <- signal + rnorm(192, sd = 0.8)
  data$PM25 <- 0.6 * signal + rnorm(192, sd = 0.5)
  data$aod[c(5, 31)] <- NA_real_
  data$site_type[1:8] <- "Port"
  results <- run_ensemble_models(output_dir = output_dir, data = data,
    predictors = c("dem", "aod", "blh"), smoke = TRUE, outer_v = 3L, inner_v = 2L)
  for (outcome in names(results)) {
    folder <- file.path(output_dir, outcome)
    bundle <- readRDS(file.path(folder, "ensemble.rds"))
    stopifnot(length(bundle$models) == length(ensemble_candidates(TRUE, 1L)),
              nrow(results[[outcome]]) == length(bundle$models) + 1L,
              all(is.finite(results[[outcome]]$rmse)))
    for (family in names(bundle$models)) {
      tuning <- utils::read.csv(file.path(folder, "final_tuning", paste0(family, "_tuning.csv")))
      stopifnot(length(unique(tuning$worker_pid)) >= 4L)
    }
    outer <- utils::read.csv(file.path(folder, "outer_rows.csv"))
    holdout <- bundle$holdout_stations$TMSID[bundle$holdout_stations$holdout]
    stopifnot(!any(outer$TMSID %in% holdout))
    audits <- list.files(folder, pattern = "temporal_splits.csv$", recursive = TRUE, full.names = TRUE)
    for (path in audits) {
      audit <- utils::read.csv(path)
      stopifnot(all(audit$train_end < audit$assessment_year))
    }
    for (i in 1:3) {
      inner <- utils::read.csv(file.path(folder, paste0("outer_", i), "rows.csv"))
      stopifnot(!any(inner$TMSID %in% holdout),
                max(inner$year) < min(outer$year[which(outer$assessment_fold == i)]))
    }
    oof <- utils::read.csv(file.path(folder, "nested_oof_predictions.csv"))
    stopifnot(!any(oof$site_type == "Port"), !anyDuplicated(oof[c("TMSID", "year")]))
    # Prediction must not depend on supplied response values.
    future <- data[data$year > bundle$training_end_year, ]
    altered <- future
    altered$PM10 <- altered$PM25 <- -999
    stopifnot(isTRUE(all.equal(predict(bundle, future), predict(bundle, altered))))
    stopifnot(inherits(try(predict(bundle, data), silent = TRUE), "try-error"))
    # Future outcome perturbations must not change earlier cascade predictions.
    base <- as.matrix(oof[names(bundle$models)])
    changed <- oof$.response
    changed[oof$year == max(oof$year)] <- changed[oof$year == max(oof$year)] + 1000
    perturbed <- ensemble_blend(base, changed, oof$year)
    stopifnot(isTRUE(all.equal(perturbed$cascade, oof$cascade_prediction, tolerance = 1e-8)))
  }
  message("SMOKE TEST PASSED: four tuning workers, all enabled families, both pollutants, temporal cascading, and persistence. Outputs: ", output_dir)
  invisible(output_dir)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 1L && args[[1]] == "--smoke") {
    ensemble_smoke_test()
  } else if (length(args) == 3L && args[[1]] == "--full") {
    run_ensemble_models(parquet_file = args[[2]], output_dir = args[[3]])
  } else {
    stop("Usage: Rscript tools/run_models/ensemble_models.R --smoke OR --full input.parquet empty_output_dir")
  }
}
