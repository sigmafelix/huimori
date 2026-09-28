# Standalone, tidymodels-free temporal prediction experiment.
# Rscript tools/run_models/generative_models.R --smoke
# Rscript tools/run_models/generative_models.R --full input.parquet empty_output_dir
# See generative_models.md for model definitions and evaluation protocol.

gen_dependencies <- function() {
  packages <- c("bnlearn", "torch", "glmnet", "nanoparquet", "sf")
  missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing)) stop("Install: ", paste(missing, collapse = ", "))
  if (!torch::torch_is_installed()) stop("Install the torch runtime with torch::install_torch().")
}

gen_coordinates <- function(data) {
  if (all(c(".coord_x", ".coord_y") %in% names(data))) return(data)
  if (!"coords_google" %in% names(data)) stop("Need coords_google: latitude, longitude.")
  values <- strsplit(as.character(data$coords_google), ",", fixed = TRUE)
  if (any(lengths(values) != 2L)) stop("Invalid coords_google.")
  values <- matrix(as.numeric(trimws(unlist(values))), ncol = 2, byrow = TRUE)
  if (any(!is.finite(values)) || any(abs(values[, 1]) > 90) || any(abs(values[, 2]) > 180)) {
    stop("Invalid latitude/longitude.")
  }
  points <- sf::st_as_sf(data.frame(lon = values[, 2], lat = values[, 1]),
                         coords = c("lon", "lat"), crs = 4326)
  coordinates <- sf::st_coordinates(sf::st_transform(points, 5179)) / 1000
  data$.coord_x <- coordinates[, 1]
  data$.coord_y <- coordinates[, 2]
  data
}

gen_folds <- function(data, v, warmup = 2L) {
  years <- sort(unique(data$year))
  if (length(years) <= warmup) stop("Insufficient years for temporal validation.")
  assessment <- tail(years[seq_along(years) > warmup], v)
  splits <- lapply(assessment, function(year) list(train = which(data$year < year),
                                                  test = which(data$year == year)))
  audit <- do.call(rbind, lapply(seq_along(splits), function(i) {
    data.frame(fold = i, train_start = min(data$year[splits[[i]]$train]),
      train_end = max(data$year[splits[[i]]$train]), assessment_year = assessment[i],
      n_train = length(splits[[i]]$train), n_test = length(splits[[i]]$test))
  }))
  stopifnot(all(audit$train_end < audit$assessment_year))
  list(splits = splits, audit = audit)
}

# All preprocessing is estimated on the current analysis window only.
gen_preprocess <- function(data, predictors, components) {
  x <- as.matrix(data[predictors])
  x[!is.finite(x)] <- NA_real_
  median <- apply(x, 2, stats::median, na.rm = TRUE)
  for (j in seq_len(ncol(x))) x[is.na(x[, j]), j] <- median[j]
  center <- colMeans(x)
  scale <- apply(x, 2, stats::sd)
  # A feature first observed in a later year cannot be imputed from that future
  # information. Omit it in this window, just like a constant feature.
  keep <- is.finite(median) & is.finite(scale) & scale > 1e-10
  if (!any(keep)) stop("No nonconstant predictors.")
  x <- sweep(sweep(x[, keep, drop = FALSE], 2, center[keep]), 2, scale[keep], "/")
  rotation <- NULL
  if (components > 0L) {
    rank <- min(components, ncol(x), nrow(x) - 1L)
    rotation <- stats::prcomp(x, center = FALSE, scale. = FALSE, rank. = rank)$rotation
    x <- x %*% rotation
  }
  colnames(x) <- paste0("X", seq_len(ncol(x)))
  list(x = x, state = list(predictors = predictors, median = median, center = center,
                           scale = scale, keep = keep, rotation = rotation))
}

gen_bake <- function(state, data) {
  if (!all(state$predictors %in% names(data))) stop("Missing prediction covariates.")
  x <- as.matrix(data[state$predictors])
  x[!is.finite(x)] <- NA_real_
  for (j in seq_len(ncol(x))) x[is.na(x[, j]), j] <- state$median[j]
  x <- sweep(sweep(x[, state$keep, drop = FALSE], 2, state$center[state$keep]),
             2, state$scale[state$keep], "/")
  if (!is.null(state$rotation)) x <- x %*% state$rotation
  colnames(x) <- paste0("X", seq_len(ncol(x)))
  x
}

gen_network <- function(inputs, hidden, outputs) {
  torch::nn_sequential(torch::nn_linear(inputs, hidden), torch::nn_relu(),
    torch::nn_linear(hidden, hidden), torch::nn_relu(), torch::nn_linear(hidden, outputs))
}

# Save ordinary R arrays instead of external torch pointers: safe across PSOCK
# workers, saveRDS/readRDS, and fresh R sessions. Training stays on the CPU.
gen_state <- function(network) lapply(network$state_dict(), torch::as_array)

gen_train_neural <- function(x, y, candidate, seed) {
  set.seed(seed)
  torch::torch_manual_seed(seed)
  torch::torch_set_num_threads(1L)
  latent <- candidate$latent
  generator <- gen_network(ncol(x) + latent, candidate$hidden, 1L)
  auxiliary <- gen_network(ncol(x) + 1L, candidate$hidden,
                           if (candidate$family == "vae") 2L * latent else 1L)
  if (candidate$family == "vae") {
    optimizer <- torch::optim_adam(c(generator$parameters, auxiliary$parameters),
                                   lr = candidate$learning_rate, weight_decay = candidate$weight_decay)
  } else {
    optimizer <- torch::optim_adam(generator$parameters, lr = candidate$learning_rate,
                                   weight_decay = candidate$weight_decay)
    discriminator_optimizer <- torch::optim_adam(auxiliary$parameters,
                                                  lr = candidate$learning_rate)
  }
  history <- vector("list", candidate$epochs)
  for (epoch in seq_len(candidate$epochs)) {
    order <- sample.int(nrow(x))
    losses <- reconstruction <- regularization <- numeric()
    for (start in seq.int(1L, nrow(x), by = candidate$batch_size)) {
      indices <- order[start:min(nrow(x), start + candidate$batch_size - 1L)]
      tx <- torch::torch_tensor(x[indices, , drop = FALSE], dtype = torch::torch_float32())
      ty <- torch::torch_tensor(matrix(y[indices], ncol = 1), dtype = torch::torch_float32())
      if (candidate$family == "vae") {
        # q(z|x,y) during training, N(0,I) during prediction. Decoder p(y|x,z).
        encoded <- auxiliary(torch::torch_cat(list(tx, ty), dim = 2))
        mean <- encoded[, seq_len(latent), drop = FALSE]
        log_variance <- encoded[, latent + seq_len(latent), drop = FALSE]$clamp(-10, 10)
        z <- mean + torch::torch_randn_like(mean) * (0.5 * log_variance)$exp()
        prediction <- generator(torch::torch_cat(list(tx, z), dim = 2))
        mse <- torch::nnf_mse_loss(prediction, ty)
        kl <- (-0.5 * (1 + log_variance - mean$pow(2) - log_variance$exp()))$sum(dim = 2)$mean()
        loss <- mse + candidate$beta * kl
        optimizer$zero_grad()
        loss$backward()
        torch::nn_utils_clip_grad_norm_(c(generator$parameters, auxiliary$parameters), 5)
        optimizer$step()
        regularization <- c(regularization, kl$item())
      } else {
        # Conditional GAN: D(x,y) distinguishes observed and generated outcomes.
        noise <- torch::torch_randn(c(length(indices), latent))
        generated <- generator(torch::torch_cat(list(tx, noise), dim = 2))
        real <- auxiliary(torch::torch_cat(list(tx, ty), dim = 2))
        fake <- auxiliary(torch::torch_cat(list(tx, generated$detach()), dim = 2))
        discriminator_loss <- torch::nnf_binary_cross_entropy_with_logits(real, torch::torch_ones_like(real)) +
          torch::nnf_binary_cross_entropy_with_logits(fake, torch::torch_zeros_like(fake))
        discriminator_optimizer$zero_grad()
        discriminator_loss$backward()
        torch::nn_utils_clip_grad_norm_(auxiliary$parameters, 5)
        discriminator_optimizer$step()
        logits <- auxiliary(torch::torch_cat(list(tx, generated), dim = 2))
        adversarial <- torch::nnf_binary_cross_entropy_with_logits(logits, torch::torch_ones_like(logits))
        mse <- torch::nnf_mse_loss(generated, ty)
        loss <- adversarial + candidate$regression_weight * mse
        optimizer$zero_grad()
        loss$backward()
        torch::nn_utils_clip_grad_norm_(generator$parameters, 5)
        optimizer$step()
        regularization <- c(regularization, discriminator_loss$item())
      }
      losses <- c(losses, loss$item())
      reconstruction <- c(reconstruction, mse$item())
    }
    if (any(!is.finite(c(losses, reconstruction, regularization)))) stop("Non-finite neural training loss.")
    history[[epoch]] <- data.frame(epoch = epoch, objective = mean(losses),
                                    mse = mean(reconstruction), kl_or_discriminator = mean(regularization))
  }
  list(state = gen_state(generator), history = do.call(rbind, history))
}

gen_fit <- function(data, predictors, candidate, seed, repeats) {
  prepared <- gen_preprocess(data, predictors, candidate$components)
  x <- prepared$x
  center <- mean(data$.response)
  scale <- stats::sd(data$.response)
  if (!is.finite(scale) || scale < 1e-8) scale <- 1
  y <- (data$.response - center) / scale
  if (candidate$family == "bn") {
    set.seed(seed)
    training <- as.data.frame(x)
    training$Y <- y
    # Y is a sink: with every covariate observed, E[Y|X] depends on Y's parents.
    # This is a predictive Gaussian BN, not a causal discovery claim or a BNN.
    graph <- bnlearn::hc(training, score = candidate$score, maxp = candidate$max_parents,
      restart = candidate$restarts, perturb = 1,
      blacklist = data.frame(from = "Y", to = colnames(x)))
    models <- list(bnlearn::bn.fit(graph, training, method = "mle-g"))
  } else {
    models <- lapply(seq_len(repeats), function(i) gen_train_neural(x, y, candidate, seed + i))
  }
  list(preprocessing = prepared$state, center = center, scale = scale, models = models,
       candidate = candidate, dimensions = ncol(x), seed = seed, training_end_year = max(data$year))
}

gen_predict_component <- function(object, data, draws) {
  if (any(data$year <= object$training_end_year)) stop("Component cannot predict its training year or the past.")
  x <- gen_bake(object$preprocessing, data)
  if (object$candidate$family == "bn") {
    result <- as.numeric(predict(object$models[[1]], node = "Y", data = as.data.frame(x), method = "parents"))
  } else {
    predictions <- lapply(object$models, function(model) {
      network <- gen_network(object$dimensions + object$candidate$latent, object$candidate$hidden, 1L)
      network$load_state_dict(lapply(model$state, torch::torch_tensor))
      network$eval()
      torch::torch_manual_seed(object$seed + 10000L)
      # Common noise draws across rows make predictions invariant to row order
      # and prediction batch sizes. No response or encoder is used at inference.
      noise <- torch::as_array(torch::torch_randn(c(draws, object$candidate$latent)))
      samples <- torch::with_no_grad({
        vapply(seq_len(draws), function(i) {
          z <- matrix(rep(noise[i, ], each = nrow(x)), nrow = nrow(x))
          input <- torch::torch_tensor(cbind(x, z), dtype = torch::torch_float32())
          as.numeric(torch::as_array(network(input)))
        }, numeric(nrow(x)))
      })
      rowMeans(matrix(samples, nrow = nrow(x)))
    })
    result <- Reduce(`+`, predictions) / length(predictions)
  }
  result <- object$center + object$scale * result
  if (any(!is.finite(result))) stop("Non-finite predictions.")
  result
}

gen_candidates <- function(smoke, seed) {
  set.seed(seed)
  counts <- if (smoke) c(bn = 4L, vae = 4L, gan = 4L) else c(bn = 24L, vae = 48L, gan = 48L)
  lapply(setNames(names(counts), names(counts)), function(family) {
    lapply(seq_len(counts[[family]]), function(i) {
      candidate <- list(id = paste0(family, "_", i), family = family,
        components = if (smoke) c(0L, 2L, 3L, 4L)[i] else sample(c(0L, 5L, 10L, 20L), 1))
      if (family == "bn") {
        c(candidate, list(score = if (i %% 2L) "bic-g" else "aic-g",
          max_parents = if (smoke) i else sample(c(2L, 4L, 8L), 1), restarts = if (smoke) 0L else sample(c(0L, 5L, 10L), 1)))
      } else {
        c(candidate, list(hidden = if (smoke) 8L else sample(c(32L, 64L, 128L), 1),
          latent = if (smoke) 2L else sample(c(2L, 4L, 8L), 1),
          epochs = if (smoke) 3L else sample(c(100L, 250L, 500L), 1),
          batch_size = if (smoke) 64L else sample(c(32L, 64L, 128L), 1),
          learning_rate = if (smoke) 0.005 else 10^runif(1, -4, -2.3),
          weight_decay = if (smoke) 1e-4 else 10^runif(1, -6, -2),
          beta = if (smoke) 0.1 else 10^runif(1, -3, 0),
          regression_weight = if (smoke) 5 else 10^runif(1, -1, 2)))
      }
    })
  })
}

gen_score_candidate <- function(candidate, data, predictors, folds, seed, repeats, draws) {
  started <- proc.time()
  warnings <- character()
  result <- tryCatch(withCallingHandlers({
    scores <- lapply(seq_along(folds$splits), function(i) {
      split <- folds$splits[[i]]
      train <- data[split$train, , drop = FALSE]
      test <- data[split$test, , drop = FALSE]
      model <- gen_fit(train, predictors, candidate, seed + i, repeats)
      prediction <- gen_predict_component(model, test, draws)
      error <- prediction - test$.response
      data.frame(year = min(test$year), n = length(error), sse = sum(error^2), sae = sum(abs(error)))
    })
    do.call(rbind, scores)
  }, warning = function(w) { warnings <<- c(warnings, conditionMessage(w)); invokeRestart("muffleWarning") }),
  error = identity)
  failed <- inherits(result, "error")
  elapsed <- proc.time() - started
  list(summary = data.frame(candidate = candidate$id,
    rmse = if (failed) Inf else sqrt(sum(result$sse) / sum(result$n)),
    mae = if (failed) Inf else sum(result$sae) / sum(result$n),
    notes = paste(c(warnings, if (failed) conditionMessage(result)), collapse = " | "),
    worker_pid = Sys.getpid(), elapsed_seconds = unname(elapsed["elapsed"])),
    folds = if (failed) NULL else cbind(candidate = candidate$id, result))
}

gen_tune <- function(data, predictors, candidates, folds, directory, cluster, seed, repeats, draws) {
  dir.create(directory, recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(folds$audit, file.path(directory, "temporal_splits.csv"), row.names = FALSE)
  selected <- list()
  for (family in names(candidates)) {
    message("    Tuning ", family, ": ", length(candidates[[family]]), " candidates")
    results <- parallel::parLapplyLB(cluster, candidates[[family]], gen_score_candidate,
      data = data, predictors = predictors, folds = folds, seed = seed,
      repeats = repeats, draws = draws, chunk.size = 1L)
    scores <- do.call(rbind, lapply(results, `[[`, "summary"))
    utils::write.csv(scores, file.path(directory, paste0(family, "_tuning.csv")), row.names = FALSE)
    utils::write.csv(do.call(rbind, lapply(results, `[[`, "folds")),
                      file.path(directory, paste0(family, "_fold_scores.csv")), row.names = FALSE)
    if (!any(is.finite(scores$rmse))) stop("All ", family, " candidates failed; inspect ", directory)
    selected[[family]] <- candidates[[family]][[which.min(scores$rmse)]]
  }
  models <- parallel::parLapply(cluster, selected, function(candidate, data, predictors, seed, repeats) {
    gen_fit(data, predictors, candidate, seed, repeats)
  }, data = data, predictors = predictors, seed = seed, repeats = repeats)
  saveRDS(selected, file.path(directory, "selected_candidates.rds"))
  for (family in c("vae", "gan")) {
    history <- do.call(rbind, lapply(seq_along(models[[family]]$models), function(i) {
      cbind(replicate = i, models[[family]]$models[[i]]$history)
    }))
    utils::write.csv(history, file.path(directory, paste0(family, "_training_history.csv")), row.names = FALSE)
  }
  models
}

gen_base_predictions <- function(models, data, draws) {
  result <- lapply(models, gen_predict_component, data = data, draws = draws)
  do.call(cbind, result)
}

# Forward-only ridge stacking; the first OOF year is blender warm-up only.
gen_blend <- function(x, y, years) {
  lambdas <- 10^seq(3, -5, length.out = 80L)
  assessment <- sort(unique(years))[-1L]
  if (!length(assessment)) stop("Need two OOF years for blending.")
  errors <- matrix(NA_real_, length(assessment), length(lambdas))
  counts <- numeric(length(assessment))
  cascade <- rep(NA_real_, length(y))
  audit <- list()
  for (i in seq_along(assessment)) {
    train <- years < assessment[i]
    test <- years == assessment[i]
    model <- glmnet::glmnet(x[train, , drop = FALSE], y[train], alpha = 0, lower.limits = 0, lambda = lambdas)
    prediction <- as.matrix(predict(model, newx = x[test, , drop = FALSE], s = lambdas))
    best <- if (i == 1L) which.min(abs(log10(lambdas))) else
      which.min(colSums(errors[seq_len(i - 1L), , drop = FALSE]))
    cascade[test] <- prediction[, best]
    errors[i, ] <- colSums((prediction - y[test])^2)
    counts[i] <- sum(test)
    audit[[i]] <- data.frame(train_end = max(years[train]), assessment_year = assessment[i])
  }
  mse <- colSums(errors) / sum(counts)
  list(model = glmnet::glmnet(x, y, alpha = 0, lower.limits = 0, lambda = lambdas),
    lambda = lambdas[which.min(mse)], cascade = cascade,
    scores = data.frame(lambda = lambdas, mse = mse), audit = do.call(rbind, audit))
}

gen_report <- function(truth, predictions, data, split) {
  groups <- c(list(all = rep(TRUE, nrow(data))),
    setNames(lapply(sort(unique(data$year)), function(year) data$year == year), paste0("year:", sort(unique(data$year)))),
    setNames(lapply(unique(data$site_type), function(type) data$site_type == type), paste0("type:", unique(data$site_type))))
  do.call(rbind, lapply(names(groups), function(group) {
    do.call(rbind, lapply(names(predictions), function(model) {
      keep <- groups[[group]] & is.finite(predictions[[model]]) & is.finite(truth)
      error <- predictions[[model]][keep] - truth[keep]
      denominator <- sum((truth[keep] - mean(truth[keep]))^2)
      data.frame(split = split, group = group, model = model, n = sum(keep),
        rmse = if (length(error)) sqrt(mean(error^2)) else NA_real_,
        mae = if (length(error)) mean(abs(error)) else NA_real_,
        r_squared = if (denominator > 0) 1 - sum(error^2) / denominator else NA_real_,
        bias = if (length(error)) mean(error) else NA_real_)
    }))
  }))
}

predict.huimori_generative <- function(object, new_data, ...) {
  if (!"year" %in% names(new_data) || any(!is.finite(new_data$year)) ||
      any(new_data$year <= object$training_end_year)) stop("Prediction years must follow the training cutoff.")
  data <- gen_coordinates(as.data.frame(new_data))
  base <- gen_base_predictions(object$models, data, object$draws)
  prediction <- as.numeric(predict(object$blender, newx = base, s = object$lambda))
  data.frame(ensemble = prediction, base, training_mean = object$training_mean, check.names = FALSE)
}

run_generative_models <- function(
  parquet_file = "/Users/isong/Documents/df_feat_correct_merged.parquet",
  output_dir = "outputs/generative_models", outcomes = c("PM10", "PM25"),
  predictors = NULL, years = NULL, workers = 4L, outer_v = 5L, inner_v = 5L,
  holdout_years = 1L, test_prop = 0.2, seed = 20260928L, smoke = FALSE, data = NULL
) {
  gen_dependencies()
  for (value in list(workers, outer_v, inner_v, holdout_years)) {
    if (length(value) != 1L || !is.finite(value) || value != as.integer(value)) stop("Integer settings required.")
  }
  if (workers < 4L || outer_v < 2L || inner_v < 1L || holdout_years < 1L || test_prop <= 0 || test_prop >= 1) {
    stop("Use workers >= 4, outer_v >= 2, inner_v >= 1, positive holdout_years and 0 < test_prop < 1.")
  }
  if (is.null(data)) data <- as.data.frame(nanoparquet::read_parquet(parquet_file))
  if (!all(c("TMSID", "year", "site_type", outcomes) %in% names(data))) stop("Missing required columns.")
  data <- data[!data$site_type %in% "Port", , drop = FALSE]
  if (!is.null(years)) data <- data[data$year %in% years, , drop = FALSE]
  data <- gen_coordinates(data)
  assignments <- unique(data[c("TMSID", "year", "site_type")])
  if (anyNA(assignments) || anyDuplicated(assignments[c("TMSID", "year")])) stop("Missing or conflicting station-year types.")
  if (is.null(predictors)) predictors <- unique(c(
    intersect(c("dem", "dsm", "mtpi", "mtpi_1km", "d_road", "gw_emission"), names(data)),
    grep("^(aod|blh)($|_)|^(landuse_frac_|lc_)", names(data), value = TRUE)))
  forbidden <- c(outcomes, "PM10", "PM25", "PM10_elt", "PM25_elt", "TMSID", "TMSID2", "site_type", "site_type_elt", "ndays", "branch_id")
  if (!length(predictors) || any(predictors %in% forbidden)) stop("Invalid predictor selection.")
  predictors <- unique(c(predictors, "year", ".coord_x", ".coord_y"))
  if (!all(predictors %in% names(data)) || !all(vapply(data[predictors], is.numeric, logical(1)))) stop("Numeric covariates required.")
  if (any(!is.finite(as.matrix(data[c("year", ".coord_x", ".coord_y")])))) stop("Invalid year/coordinates.")
  if (dir.exists(output_dir) && length(list.files(output_dir))) stop("Use an empty output directory.")
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  cluster <- local({
    limits <- c(OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1", VECLIB_MAXIMUM_THREADS = "1")
    old <- setNames(Sys.getenv(names(limits), unset = NA_character_), names(limits))
    on.exit({ Sys.unsetenv(names(old)[is.na(old)]); if (any(!is.na(old))) do.call(Sys.setenv, as.list(old[!is.na(old)])) })
    do.call(Sys.setenv, as.list(limits))
    parallel::makePSOCKcluster(workers, outfile = file.path(output_dir, "workers.log"))
  })
  on.exit(parallel::stopCluster(cluster), add = TRUE)
  functions <- ls(envir = environment(gen_dependencies), pattern = "^gen_")
  parallel::clusterExport(cluster, functions, envir = environment(gen_dependencies))
  parallel::clusterCall(cluster, function(libraries) {
    .libPaths(libraries)
    gen_dependencies()
    torch::torch_set_num_threads(1L)
    torch::torch_set_num_interop_threads(1L)
    NULL
  }, .libPaths())
  torch::torch_set_num_threads(1L)
  candidates <- gen_candidates(smoke, seed)
  repeats <- if (smoke) 1L else 3L
  draws <- if (smoke) 8L else 64L
  saveRDS(list(input = parquet_file, outcomes = outcomes, predictors = predictors, years = years,
    candidates = candidates, repeats = repeats, draws = draws, seed = seed, workers = workers,
    outer_v = outer_v, inner_v = inner_v, holdout_years = holdout_years, test_prop = test_prop,
    smoke = smoke), file.path(output_dir, "configuration.rds"))
  all_reports <- list()
  for (outcome in outcomes) {
    message("Outcome: ", outcome)
    current <- data[is.finite(data[[outcome]]), , drop = FALSE]
    current$.response <- current[[outcome]]
    observed <- sort(unique(current$year))
    if (length(observed) < holdout_years + 5L) stop("Need >= five development years plus holdout years.")
    cutoff <- min(tail(observed, holdout_years))
    stations <- current[order(current$year, decreasing = TRUE), c("TMSID", "site_type")]
    stations <- stations[!duplicated(stations$TMSID), ]
    set.seed(seed)
    stations$holdout <- FALSE
    for (type in unique(stations$site_type)) {
      indices <- which(stations$site_type == type)
      if (length(indices) < 2L) stop("Need two stations per site type.")
      count <- min(length(indices) - 1L, max(1L, floor(length(indices) * test_prop)))
      stations$holdout[indices[sample.int(length(indices), count)]] <- TRUE
    }
    reserved <- stations$holdout[match(current$TMSID, stations$TMSID)]
    development <- current[!reserved & current$year < cutoff, , drop = FALSE]
    test <- current[reserved & current$year >= cutoff, , drop = FALSE]
    if (!nrow(test)) stop("No future holdout observations.")
    folder <- file.path(output_dir, outcome)
    dir.create(folder)
    utils::write.csv(stations, file.path(folder, "holdout_stations.csv"), row.names = FALSE)
    outer <- gen_folds(development, outer_v, warmup = 3L)
    utils::write.csv(outer$audit, file.path(folder, "outer_temporal_splits.csv"), row.names = FALSE)
    utils::write.csv(data.frame(train_end = max(development$year), assessment_year = min(test$year)),
                      file.path(folder, "holdout_temporal_splits.csv"), row.names = FALSE)
    oof <- list()
    for (i in seq_along(outer$splits)) {
      message("  Outer temporal fold ", i, "/", length(outer$splits))
      train <- development[outer$splits[[i]]$train, , drop = FALSE]
      assessment <- development[outer$splits[[i]]$test, , drop = FALSE]
      directory <- file.path(folder, paste0("outer_", i))
      fitted <- gen_tune(train, predictors, candidates, gen_folds(train, inner_v), directory,
                         cluster, seed + i, repeats, draws)
      prediction <- gen_base_predictions(fitted, assessment, draws)
      oof[[i]] <- cbind(assessment[c("TMSID", "year", "site_type", ".response")],
                        as.data.frame(prediction), training_mean = mean(train$.response))
    }
    oof <- do.call(rbind, oof)
    blend <- gen_blend(as.matrix(oof[names(candidates)]), oof$.response, oof$year)
    oof$ensemble <- blend$cascade
    utils::write.csv(oof, file.path(folder, "cascade_predictions.csv"), row.names = FALSE)
    utils::write.csv(blend$scores, file.path(folder, "blender_tuning.csv"), row.names = FALSE)
    utils::write.csv(blend$audit, file.path(folder, "blender_temporal_splits.csv"), row.names = FALSE)
    final <- gen_tune(development, predictors, candidates, gen_folds(development, inner_v),
      file.path(folder, "final_tuning"), cluster, seed, repeats, draws)
    bundle <- structure(list(models = final, blender = blend$model, lambda = blend$lambda,
      predictors = predictors, draws = draws, outcome = outcome, seed = seed,
      training_end_year = max(development$year), training_mean = mean(development$.response)),
      class = "huimori_generative")
    prediction <- predict(bundle, new_data = test)
    report <- rbind(gen_report(oof$.response, oof[c("ensemble", names(candidates), "training_mean")], oof, "cascade"),
                    gen_report(test$.response, prediction, test, "holdout"))
    report$outcome <- outcome
    utils::write.csv(report, file.path(folder, "performance.csv"), row.names = FALSE)
    utils::write.csv(cbind(test[c("TMSID", "year", "site_type", ".response")], prediction),
                      file.path(folder, "holdout_predictions.csv"), row.names = FALSE)
    weights <- as.matrix(stats::coef(bundle$blender, s = bundle$lambda))
    utils::write.csv(data.frame(term = rownames(weights), coefficient = weights[, 1]),
                      file.path(folder, "ensemble_weights.csv"), row.names = FALSE)
    saveRDS(bundle, file.path(folder, "models.rds"))
    restored <- readRDS(file.path(folder, "models.rds"))
    stopifnot(isTRUE(all.equal(prediction, predict(restored, test), tolerance = 1e-6)))
    all_reports[[outcome]] <- report
  }
  report <- do.call(rbind, all_reports)
  utils::write.csv(report, file.path(output_dir, "performance.csv"), row.names = FALSE)
  writeLines(capture.output(sessionInfo()), file.path(output_dir, "session_info.txt"))
  invisible(report)
}

gen_smoke_test <- function(output_dir = tempfile("generative_smoke_", tmpdir = "outputs")) {
  dir.create("outputs", showWarnings = FALSE)
  set.seed(91)
  sites <- data.frame(TMSID = sprintf("S%02d", 1:24), site_type = rep(c("Urban", "Island", "Suburban"), each = 8),
    .coord_x = runif(24, 900, 1150), .coord_y = runif(24, 1700, 1950))
  data <- sites[rep(1:24, each = 8), ]
  data$year <- rep(2015:2022, 24)
  data$dem <- runif(192, 0, 500)
  data$aod <- runif(192)
  data$blh <- runif(192, 100, 1000)
  signal <- 10 + 5 * data$aod + 0.01 * data$dem + 0.3 * (data$year - 2015)
  data$PM10 <- signal + rnorm(192, sd = 0.5)
  data$PM25 <- 0.6 * signal + rnorm(192, sd = 0.3)
  data$late_feature <- ifelse(data$year <= 2017, NA_real_, runif(192))
  data$aod[c(4, 19)] <- NA_real_
  data$site_type[1:8] <- "Port"
  initial <- gen_preprocess(data[data$year <= 2016, ], c("dem", "aod", "blh", "late_feature"), 0L)
  stopifnot(!initial$state$keep[["late_feature"]], all(is.finite(initial$x)))
  run_generative_models(data = data, predictors = c("dem", "aod", "blh", "late_feature"), smoke = TRUE,
                         outer_v = 3L, inner_v = 2L, output_dir = output_dir)
  for (outcome in c("PM10", "PM25")) {
    folder <- file.path(output_dir, outcome)
    audits <- list.files(folder, pattern = "temporal_splits.csv$", recursive = TRUE, full.names = TRUE)
    for (path in audits) {
      audit <- utils::read.csv(path)
      stopifnot(all(audit$train_end < audit$assessment_year))
    }
    bundle <- readRDS(file.path(folder, "models.rds"))
    for (family in c("bn", "vae", "gan")) {
      scores <- utils::read.csv(file.path(folder, "final_tuning", paste0(family, "_tuning.csv")))
      stopifnot(length(unique(scores$worker_pid)) == 4L, any(is.finite(scores$rmse)))
    }
    stations <- utils::read.csv(file.path(folder, "holdout_stations.csv"))
    oof <- utils::read.csv(file.path(folder, "cascade_predictions.csv"))
    stopifnot(!any(oof$TMSID %in% stations$TMSID[stations$holdout]), !any(oof$site_type == "Port"))
    changed <- oof$.response
    changed[oof$year == max(oof$year)] <- changed[oof$year == max(oof$year)] + 1000
    perturbed <- gen_blend(as.matrix(oof[c("bn", "vae", "gan")]), changed, oof$year)
    stopifnot(isTRUE(all.equal(perturbed$cascade, oof$ensemble, tolerance = 1e-6)))
    future <- data[data$year > bundle$training_end_year, ]
    altered <- future
    altered$PM10 <- altered$PM25 <- -999
    stopifnot(isTRUE(all.equal(predict(bundle, future), predict(bundle, altered))),
              inherits(try(predict(bundle, data), silent = TRUE), "try-error"))
    prediction <- predict(bundle, future)
    stopifnot(all(is.finite(as.matrix(prediction))), ncol(prediction) == 5L)
    stopifnot(isTRUE(all.equal(prediction[1, ], predict(bundle, future[1, , drop = FALSE]),
                              check.attributes = FALSE, tolerance = 1e-6)))
  }
  stopifnot(!any(c("tidymodels", "parsnip", "recipes", "tune", "rsample", "workflows") %in% loadedNamespaces()))
  message("SMOKE TEST PASSED: BN, conditional VAE/GAN, four workers, temporal tuning/stacking, persistence. Outputs: ", output_dir)
  invisible(output_dir)
}

if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 1L && args[[1]] == "--smoke") {
    gen_smoke_test()
  } else if (length(args) == 3L && args[[1]] == "--full") {
    run_generative_models(parquet_file = args[[2]], output_dir = args[[3]])
  } else stop("Usage: --smoke OR --full input.parquet empty_output_dir")
}
