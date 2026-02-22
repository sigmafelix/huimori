library(pacman)
p_load(data.table, torch, arrow, Metrics)

set.seed(2026)
torch_manual_seed(2026)

basepath <- file.path(Sys.getenv("HOME"), "Documents")
filename <- "dt_daily.parquet"

default_config <- list(
	year_max = 2012L,
	seq_len = 48L,
	batch_size = 64L,
	epochs = 100L,
	learning_rate = 5e-4,
	weight_decay = 1e-4,
	d_model = 64L,
	d_inner = 128L,
	d_state = 64L,
	n_layers = 3L,
	conv_kernel = 3L,
	dropout = 0.1,
	train_prop = 0.8,
	min_obs_per_target = 365L,
	save_artifacts = TRUE,
	artifact_dir = file.path("tools", "run_models", "artifacts"),
	device = if (cuda_is_available()) torch_device("cuda") else torch_device("cpu")
)

ensure_dir <- function(path) {
	if (!dir.exists(path)) {
		dir.create(path, recursive = TRUE, showWarnings = FALSE)
	}
	path
}

coerce_numeric_columns <- function(dt, columns, context = "data") {
	for (col_name in columns) {
		raw <- dt[[col_name]]
		num <- suppressWarnings(as.numeric(raw))
		bad <- is.na(num) & !is.na(raw)
		if (any(bad)) {
			stop(sprintf("Non-numeric values found in %s column '%s'.", context, col_name))
		}
		set(dt, j = col_name, value = num)
	}
	dt
}

add_weekday_features <- function(dt) {
	dt[
		,
		`:=`(
			weekday_mon = fifelse(weekday == 1L, 1, 0),
			weekday_tue = fifelse(weekday == 2L, 1, 0),
			weekday_wed = fifelse(weekday == 3L, 1, 0),
			weekday_thu = fifelse(weekday == 4L, 1, 0),
			weekday_fri = fifelse(weekday == 5L, 1, 0),
			weekday_sat = fifelse(weekday == 6L, 1, 0),
			weekday_sun = fifelse(weekday == 7L, 1, 0),
			diff_dsm_dem = dsm - dem
		)
	]
	dt
}

choose_tmsid2 <- function(dt, min_obs = 365L, seq_len = 30L) {
	score <- dt[
		,
		.(
			n_pm10 = sum(!is.na(PM10)),
			n_pm25 = sum(!is.na(PM25))
		),
		by = TMSID2
	]

	strict <- score[n_pm10 >= min_obs & n_pm25 >= min_obs]
	fallback <- score[n_pm10 >= (seq_len * 2L) & n_pm25 >= (seq_len * 2L)]

	selected <- if (nrow(strict) > 0L) strict else fallback

	if (nrow(selected) == 0L) {
		stop("No TMSID2 has enough observations for both PM10 and PM25.")
	}

	if (nrow(strict) == 0L) {
		message("No TMSID2 met strict min_obs; falling back to sequence-length threshold.")
	}

	selected[, total := n_pm10 + n_pm25]
	selected[order(-total)][1, TMSID2]
}

prepare_target_table <- function(dt, target, cfg) {
	other_target <- if (target == "PM10") "PM25" else "PM10"

	target_dt <- copy(dt)[!is.na(get(target))]
	target_dt[, (other_target) := NULL]

	covars <- setdiff(names(target_dt), c(target, "date", "TMSID2", "year", "weekday"))
	target_dt <- coerce_numeric_columns(target_dt, covars, context = sprintf("target '%s'", target))
	target_dt[, (covars) := lapply(.SD, function(x) frollmean(x, 7, fill = mean(x, na.rm = TRUE))), .SDcols = covars]

	target_dt <- target_dt[order(date)]
	split_idx <- floor(nrow(target_dt) * cfg$train_prop)

	if (split_idx <= cfg$seq_len || (nrow(target_dt) - split_idx) <= cfg$seq_len) {
		stop(sprintf("Not enough rows for %s after split/sequence windowing.", target))
	}

	train_dt <- target_dt[1:split_idx]
	test_dt <- target_dt[(split_idx + 1L):.N]

	numeric_covars <- covars
	mu <- train_dt[, lapply(.SD, mean, na.rm = TRUE), .SDcols = numeric_covars]
	sigma <- train_dt[, lapply(.SD, sd, na.rm = TRUE), .SDcols = numeric_covars]

	mu_vec <- unlist(mu, use.names = TRUE)
	sigma_vec <- unlist(sigma, use.names = TRUE)
	sigma_vec[sigma_vec == 0 | is.na(sigma_vec)] <- 1

	train_dt[, (numeric_covars) := Map(function(col, m, s) (col - m) / s, .SD, mu_vec[numeric_covars], sigma_vec[numeric_covars]), .SDcols = numeric_covars]
	test_dt[, (numeric_covars) := Map(function(col, m, s) (col - m) / s, .SD, mu_vec[numeric_covars], sigma_vec[numeric_covars]), .SDcols = numeric_covars]

	list(
		train_dt = train_dt,
		test_dt = test_dt,
		features = covars,
		target = target
	)
}

make_windows <- function(dt, features, target, seq_len) {
	n <- nrow(dt)
	p <- length(features)
	n_samples <- n - seq_len + 1L

	x <- array(0, dim = c(n_samples, seq_len, p))
	y <- numeric(n_samples)

	for (i in seq_len:n) {
		idx <- i - seq_len + 1L
		x[idx, , ] <- as.matrix(dt[(i - seq_len + 1L):i, ..features])
		y[idx] <- dt[[target]][i]
	}

	list(x = x, y = y)
}

time_series_dataset <- dataset(
	name = "time_series_dataset",
	initialize = function(x, y) {
		self$x <- torch_tensor(x, dtype = torch_float32())
		self$y <- torch_tensor(y, dtype = torch_float32())$unsqueeze(2)
	},
	.getitem = function(i) {
		list(x = self$x[i, , ], y = self$y[i, ])
	},
	.length = function() {
		self$x$size()[1]
	}
)

mamba_block <- nn_module(
	"mamba_block",
	initialize = function(d_model, d_inner, d_state, conv_kernel = 3L, dropout = 0.1) {
		self$d_inner <- d_inner
		self$norm <- nn_layer_norm(normalized_shape = d_model)
		self$in_proj <- nn_linear(d_model, d_inner * 2L)
		self$conv <- nn_conv1d(
			in_channels = d_inner,
			out_channels = d_inner,
			kernel_size = conv_kernel,
			groups = d_inner,
			padding = conv_kernel - 1L
		)
		self$to_state <- nn_linear(d_inner, d_state)
		self$from_state <- nn_linear(d_state, d_inner)
		self$out_proj <- nn_linear(d_inner, d_model)
		self$dropout <- nn_dropout(p = dropout)

		self$A_log <- nn_parameter(torch_randn(d_state))
		self$B <- nn_parameter(torch_randn(d_state))
		self$C <- nn_parameter(torch_randn(d_state))
		self$D <- nn_parameter(torch_ones(d_state))
	},
	forward = function(x) {
		b <- x$size(1)
		s <- x$size(2)

		residual <- x
		x <- self$norm(x)
		proj <- self$in_proj(x)

		u <- proj[, , 1:self$d_inner]
		g <- proj[, , (self$d_inner + 1):(2L * self$d_inner)]

		u <- u$transpose(2, 3)
		u <- self$conv(u)
		u <- u[, , 1:s]
		u <- nnf_silu(u)
		u <- u$transpose(2, 3)

		ssm_in <- self$to_state(u)

		state <- torch_zeros(c(b, self$A_log$size(1)), device = x$device)
		decay <- torch_exp(-torch_exp(self$A_log))
		outs <- vector("list", s)

		for (t in seq_len(s)) {
			u_t <- ssm_in[, t, ]
			state <- state * decay + u_t * self$B
			y_t <- state * self$C + u_t * self$D
			outs[[t]] <- y_t$unsqueeze(2)
		}

		y <- torch_cat(outs, dim = 2)
		y <- self$from_state(y)
		y <- y * torch_sigmoid(g)
		y <- self$out_proj(y)
		y <- self$dropout(y)

		residual + y
	}
)

mamba_regressor <- nn_module(
	"mamba_regressor",
	initialize = function(input_dim, d_model = 64L, d_inner = 128L, d_state = 64L, n_layers = 3L, conv_kernel = 3L, dropout = 0.1) {
		self$embed <- nn_linear(input_dim, d_model)
		self$blocks <- nn_module_list(lapply(
			seq_len(n_layers),
			function(i) mamba_block(d_model, d_inner, d_state, conv_kernel, dropout)
		))
		self$head_norm <- nn_layer_norm(d_model)
		self$head <- nn_linear(d_model, 1L)
	},
	forward = function(x) {
		h <- self$embed(x)
		for (i in seq_len(length(self$blocks))) {
			h <- self$blocks[[i]](h)
		}
		h <- self$head_norm(h)
		h_last <- h[, h$size(2), ]
		self$head(h_last)
	}
)

fit_one_target <- function(train_dt, test_dt, features, target, cfg) {
	train_seq <- make_windows(train_dt, features, target, cfg$seq_len)
	test_seq <- make_windows(test_dt, features, target, cfg$seq_len)

	train_ds <- time_series_dataset(train_seq$x, train_seq$y)
	test_ds <- time_series_dataset(test_seq$x, test_seq$y)

	train_dl <- dataloader(train_ds, batch_size = cfg$batch_size, shuffle = TRUE)
	test_dl <- dataloader(test_ds, batch_size = cfg$batch_size, shuffle = FALSE)

	model <- mamba_regressor(
		input_dim = length(features),
		d_model = cfg$d_model,
		d_inner = cfg$d_inner,
		d_state = cfg$d_state,
		n_layers = cfg$n_layers,
		conv_kernel = cfg$conv_kernel,
		dropout = cfg$dropout
	)

	model <- model$to(device = cfg$device)
	optimizer <- optim_adamw(model$parameters, lr = cfg$learning_rate, weight_decay = cfg$weight_decay)
	criterion <- nn_mse_loss()

	best_val <- Inf
	best_state <- NULL

	for (epoch in seq_len(cfg$epochs)) {
		model$train()
		train_loss <- c()

		coro::loop(for (batch in train_dl) {
			x <- batch$x$to(device = cfg$device)
			y <- batch$y$to(device = cfg$device)

			optimizer$zero_grad()
			pred <- model(x)
			loss <- criterion(pred, y)
			loss$backward()
			optimizer$step()

			train_loss <- c(train_loss, loss$item())
		})

		model$eval()
		val_loss <- c()

		with_no_grad({
			coro::loop(for (batch in test_dl) {
				x <- batch$x$to(device = cfg$device)
				y <- batch$y$to(device = cfg$device)
				pred <- model(x)
				loss <- criterion(pred, y)
				val_loss <- c(val_loss, loss$item())
			})
		})

		mean_train <- mean(train_loss)
		mean_val <- mean(val_loss)

		if (mean_val < best_val) {
			best_val <- mean_val
			best_state <- model$state_dict()
		}

		if (epoch %% 10L == 0L || epoch == 1L || epoch == cfg$epochs) {
			cat(sprintf("[%s] epoch %03d | train_mse=%.5f | test_mse=%.5f\n", target, epoch, mean_train, mean_val))
		}
	}

	if (!is.null(best_state)) {
		model$load_state_dict(best_state)
	}

	model$eval()
	preds <- c()
	trues <- c()

	with_no_grad({
		coro::loop(for (batch in test_dl) {
			x <- batch$x$to(device = cfg$device)
			y <- batch$y$to(device = cfg$device)
			pred <- model(x)
			preds <- c(preds, as.numeric(pred$to(device = "cpu")$squeeze(2)))
			trues <- c(trues, as.numeric(y$to(device = "cpu")$squeeze(2)))
		})
	})

	metrics <- list(
		rmse = Metrics::rmse(trues, preds),
		mae = Metrics::mae(trues, preds),
		rsq = cor(trues, preds)^2
	)

	list(model = model, metrics = metrics, preds = preds, trues = trues)
}

save_model_bundle <- function(model, file_path, model_hparams, features, target, tmsid2, metrics) {
	bundle <- list(
		model_state = model$state_dict(),
		model_hparams = model_hparams,
		features = features,
		target = target,
		tmsid2 = tmsid2,
		metrics = metrics,
		saved_at = as.character(Sys.time())
	)
	torch_save(bundle, file_path)
	invisible(file_path)
}

load_model_bundle <- function(file_path, device = if (cuda_is_available()) torch_device("cuda") else torch_device("cpu")) {
	bundle <- torch_load(file_path)
	hp <- bundle$model_hparams
	model <- mamba_regressor(
		input_dim = as.integer(hp$input_dim),
		d_model = as.integer(hp$d_model),
		d_inner = as.integer(hp$d_inner),
		d_state = as.integer(hp$d_state),
		n_layers = as.integer(hp$n_layers),
		conv_kernel = as.integer(hp$conv_kernel),
		dropout = as.numeric(hp$dropout)
	)
	model$load_state_dict(bundle$model_state)
	model <- model$to(device = device)

	list(
		model = model,
		metadata = bundle
	)
}

prepare_sequence_input <- function(sequence_matrix, expected_features = NULL) {
	if (inherits(sequence_matrix, "data.table") || is.data.frame(sequence_matrix)) {
		seq_dt <- as.data.table(sequence_matrix)
		if (!is.null(expected_features)) {
			missing <- setdiff(expected_features, names(seq_dt))
			if (length(missing) > 0L) {
				stop(sprintf("Missing expected feature columns: %s", paste(missing, collapse = ", ")))
			}
			seq_dt <- seq_dt[, ..expected_features]
			seq_dt <- coerce_numeric_columns(seq_dt, expected_features, context = "inference sequence")
			sequence_matrix <- as.matrix(seq_dt)
		} else {
			cols <- names(seq_dt)
			seq_dt <- coerce_numeric_columns(seq_dt, cols, context = "inference sequence")
			sequence_matrix <- as.matrix(seq_dt)
		}
	}

	if (!is.matrix(sequence_matrix)) {
		stop("sequence_matrix must be a matrix/data.frame/data.table with shape [seq_len, n_features].")
	}

	storage.mode(sequence_matrix) <- "double"
	if (anyNA(sequence_matrix)) {
		stop("sequence_matrix contains NA values. Please impute or remove NA before inference.")
	}

	sequence_matrix
}

predict_from_loaded_bundle <- function(loaded, sequence_matrix, device = NULL) {
	if (is.null(device)) {
		device <- default_config$device
	}

	expected_features <- loaded$metadata$features
	x_mat <- prepare_sequence_input(sequence_matrix, expected_features = expected_features)

	n_features <- ncol(x_mat)
	expected_input_dim <- as.integer(loaded$metadata$model_hparams$input_dim)
	if (n_features != expected_input_dim) {
		stop(sprintf("Feature mismatch: got %d columns, expected %d.", n_features, expected_input_dim))
	}

	x <- torch_tensor(x_mat, dtype = torch_float32())$unsqueeze(1)$to(device = device)

	loaded$model$eval()
	pred <- with_no_grad({
		loaded$model(x)$to(device = "cpu")$squeeze()$item()
	})

	as.numeric(pred)
}

predict_from_checkpoint <- function(checkpoint_path, sequence_matrix, device = NULL) {
	loaded <- load_model_bundle(checkpoint_path, device = if (is.null(device)) default_config$device else device)
	predict_from_loaded_bundle(loaded, sequence_matrix, device = if (is.null(device)) default_config$device else device)
}

export_predictions <- function(file_path, target, preds, trues, tmsid2) {
	dt_pred <- data.table(
		TMSID2 = as.character(tmsid2),
		target = target,
		y_true = as.numeric(trues),
		y_pred = as.numeric(preds),
		residual = as.numeric(trues) - as.numeric(preds)
	)
	arrow::write_parquet(dt_pred, file_path)
	invisible(file_path)
}

run_experiment <- function(chosen_tmsid2 = NULL, cfg = default_config) {
	dt <- as.data.table(arrow::read_parquet(file.path(basepath, filename), as_data_frame = TRUE))
	year_max <- as.integer(cfg$year_max)
	build_dtx <- function(use_all_years = FALSE) {
		x <- if (use_all_years) dt[, ..cols_needed] else dt[year <= year_max, ..cols_needed]
		x <- add_weekday_features(x)
		x[, date := as.Date(date)]
		x
	}

	cols_needed <- c(
		"TMSID2", "year", "date", "PM10", "PM25", "weekday", "is_weekend",
		"d_road", "dsm", "dem", "mtpi", "mtpi_1km", "lc_CRP", "lc_FST", "lc_WET", "lc_IMP", "lc_WTR"
	)

	if (!all(cols_needed %in% names(dt))) {
		missing <- setdiff(cols_needed, names(dt))
		stop(sprintf("Missing required columns: %s", paste(missing, collapse = ", ")))
	}

	dtx <- build_dtx(FALSE)
	used_year_fallback <- FALSE

	if (is.null(chosen_tmsid2)) {
		chosen_tmsid2 <- tryCatch(
			choose_tmsid2(dtx, min_obs = cfg$min_obs_per_target, seq_len = cfg$seq_len),
			error = function(e) {
				message("Year-filtered data does not support dual-target training; retrying with all years.")
				used_year_fallback <<- TRUE
				dtx <<- build_dtx(TRUE)
				dtx_all <- dtx
				choose_tmsid2(dtx_all, min_obs = cfg$min_obs_per_target, seq_len = cfg$seq_len)
			}
		)
	}

	dts <- dtx[TMSID2 == chosen_tmsid2][order(date)]

	if (!is.null(chosen_tmsid2) && !used_year_fallback) {
		has_both <- dts[, sum(!is.na(PM10)) > 0 && sum(!is.na(PM25)) > 0]
		if (!has_both) {
			message("Selected TMSID2 has no dual-target overlap in year-filtered data; retrying with all years.")
			dtx <- build_dtx(TRUE)
			dts <- dtx[TMSID2 == chosen_tmsid2][order(date)]
			used_year_fallback <- TRUE
		}
	}

	if (nrow(dts) < (cfg$seq_len * 3L)) {
		stop("Selected TMSID2 has too few rows for sequence modeling.")
	}

	cat(sprintf("Selected TMSID2: %s\n", as.character(chosen_tmsid2)))
	if (used_year_fallback) {
		cat("Using all available years for dual-target training.\n")
	}
	cat(sprintf("Device: %s\n", as.character(cfg$device)))

	pm10_data <- prepare_target_table(dts, "PM10", cfg)
	pm25_data <- prepare_target_table(dts, "PM25", cfg)

	pm10_fit <- fit_one_target(
		train_dt = pm10_data$train_dt,
		test_dt = pm10_data$test_dt,
		features = pm10_data$features,
		target = "PM10",
		cfg = cfg
	)

	pm25_fit <- fit_one_target(
		train_dt = pm25_data$train_dt,
		test_dt = pm25_data$test_dt,
		features = pm25_data$features,
		target = "PM25",
		cfg = cfg
	)

	cat(sprintf("PM10 metrics | RMSE: %.4f, MAE: %.4f, R2: %.4f\n", pm10_fit$metrics$rmse, pm10_fit$metrics$mae, pm10_fit$metrics$rsq))
	cat(sprintf("PM25 metrics | RMSE: %.4f, MAE: %.4f, R2: %.4f\n", pm25_fit$metrics$rmse, pm25_fit$metrics$mae, pm25_fit$metrics$rsq))

	artifacts <- list()
	if (isTRUE(cfg$save_artifacts)) {
		artifact_root <- ensure_dir(file.path(cfg$artifact_dir, as.character(chosen_tmsid2)))
		model_hparams <- list(
			input_dim = length(pm10_data$features),
			d_model = cfg$d_model,
			d_inner = cfg$d_inner,
			d_state = cfg$d_state,
			n_layers = cfg$n_layers,
			conv_kernel = cfg$conv_kernel,
			dropout = cfg$dropout
		)

		pm10_ckpt <- file.path(artifact_root, "mamba_pm10.pt")
		pm25_ckpt <- file.path(artifact_root, "mamba_pm25.pt")
		pm10_pred_file <- file.path(artifact_root, "predictions_pm10.parquet")
		pm25_pred_file <- file.path(artifact_root, "predictions_pm25.parquet")

		save_model_bundle(pm10_fit$model, pm10_ckpt, model_hparams, pm10_data$features, "PM10", chosen_tmsid2, pm10_fit$metrics)
		save_model_bundle(pm25_fit$model, pm25_ckpt, model_hparams, pm25_data$features, "PM25", chosen_tmsid2, pm25_fit$metrics)

		export_predictions(pm10_pred_file, "PM10", pm10_fit$preds, pm10_fit$trues, chosen_tmsid2)
		export_predictions(pm25_pred_file, "PM25", pm25_fit$preds, pm25_fit$trues, chosen_tmsid2)

		cat(sprintf("Saved PM10 checkpoint: %s\n", pm10_ckpt))
		cat(sprintf("Saved PM25 checkpoint: %s\n", pm25_ckpt))
		cat(sprintf("Saved PM10 predictions: %s\n", pm10_pred_file))
		cat(sprintf("Saved PM25 predictions: %s\n", pm25_pred_file))

		artifacts <- list(
			checkpoint_pm10 = pm10_ckpt,
			checkpoint_pm25 = pm25_ckpt,
			predictions_pm10 = pm10_pred_file,
			predictions_pm25 = pm25_pred_file
		)
	}

	list(
		tmsid2 = chosen_tmsid2,
		config = cfg,
		pm10 = pm10_fit,
		pm25 = pm25_fit,
		artifacts = artifacts
	)
}

# Usage:
# result <- run_experiment(chosen_tmsid2 = 111123456)
# result <- run_experiment()  # auto-select TMSID2 with enough PM10/PM25 data
# loaded <- load_model_bundle(result$artifacts$checkpoint_pm10, device = default_config$device)
result <- run_experiment(chosen_tmsid2 = "111142A")
