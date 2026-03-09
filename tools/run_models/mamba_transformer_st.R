###############################################################################
# Mamba-Transformer Spatiotemporal Model for PM10/PM2.5 Prediction
#
# Architecture: Shared Mamba temporal encoder + Transformer cross-attention
# over K nearest neighbor stations with distance-aware attention bias,
# gated fusion with static features, joint PM10/PM25 prediction head.
#
# See mamba_transformer_st_design.md for full design rationale.
###############################################################################

library(pacman)
p_load(data.table, torch, arrow, sf, Metrics, coro)

set.seed(2026)
torch_manual_seed(2026)

# ============================================================================
# Configuration
# ============================================================================

default_st_config <- list(
    # Data
    seq_len             = 24L,
    K_neighbors         = 5L,
    min_obs_per_station = 100L,

    # Model
    d_model             = 32L,
    d_inner             = 32L,
    d_state             = 32L,
    n_mamba_layers      = 3L,
    conv_kernel         = 3L,
    n_cross_attn_layers = 2L,
    n_heads             = 4L,
    dropout             = 0.3,

    # Training
    batch_size          = 64L,
    max_epochs          = 300L,
    learning_rate       = 5e-3,
    weight_decay        = 1e-2,
    patience            = 15L,
    min_delta           = 1e-5,
    grad_clip_norm      = 1.0,
    lr_factor           = 0.5,
    lr_patience         = 5L,
    use_amp             = TRUE,
    amp_dtype           = "float16",
    use_vectorized_ssm  = TRUE,
    num_workers         = 2L,
    pin_memory          = TRUE,
    cache_dataset_tensors = NULL,

    # Split proportions (train / val / test)
    train_prop          = 0.6,
    val_prop            = 0.3,

    # I/O
    basepath            = file.path(Sys.getenv("HOME"), "Documents"),
    filename            = "dt_daily.parquet",
    save_artifacts      = TRUE,
    artifact_dir        = file.path("tools", "run_models", "artifacts_st"),
    device              = if (cuda_is_available()) torch_device("cuda") else torch_device("cpu")
)

resolve_amp_dtype <- function(dtype_name) {
    if (identical(dtype_name, "bfloat16")) {
        torch_bfloat16()
    } else {
        torch_float16()
    }
}

ensure_dir <- function(path) {
    if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
    path
}

# ============================================================================
# Data Loading & Preprocessing
# ============================================================================

load_and_prepare_data <- function(cfg = default_st_config) {
    cat("Loading data...\n")
    dt <- as.data.table(arrow::read_parquet(
        file.path(cfg$basepath, cfg$filename),
        as_data_frame = TRUE
    ))

    # Use existing projected coordinates (EPSG:5179, meters)
    if ("coords_x" %in% names(dt) && "coords_y" %in% names(dt)) {
        setnames(dt, c("coords_x", "coords_y"), c("coord_x", "coord_y"),
            skip_absent = TRUE
        )
    } else if ("geometry" %in% names(dt)) {
        # Fallback: parse geometry if coordinate columns are absent
        geo_sf <- sf::st_as_sf(dt, sf_column_name = "geometry")
        coords <- sf::st_coordinates(geo_sf)
        dt[, `:=`(coord_x = coords[, 1], coord_y = coords[, 2])]
    }

    # Drop non-feature columns
    drop_cols <- intersect(names(dt), c(
        "geometry", "TMSID", "site_type", "date_start", "date_end",
        "coords_google", "lon2", "lat2", "dist_m", "ndays",
        "month", "day",
        # Lagged/smoothed PM variants (use raw PM10/PM25 only)
        "PM10_A1", "PM10_A3", "PM10_A7", "PM10_B1", "PM10_B3", "PM10_B7",
        "PM10_C1", "PM10_C3", "PM10_C7",
        "PM25_A1", "PM25_A3", "PM25_A7", "PM25_B1", "PM25_B3", "PM25_B7",
        "PM25_C1", "PM25_C3", "PM25_C7"
    ))
    if (length(drop_cols) > 0) dt[, (drop_cols) := NULL]

    dt[, date := as.Date(date)]

    # Coerce integer columns to double to avoid truncation warnings from frollmean
    int_to_dbl <- c("is_weekend", "weekday")
    int_to_dbl <- intersect(int_to_dbl, names(dt))
    for (col_name in int_to_dbl) {
        set(dt, j = col_name, value = as.double(dt[[col_name]]))
    }

    # Feature engineering: one-hot weekday + diff_dsm_dem
    dt[, `:=`(
        weekday_mon = fifelse(weekday == 1L, 1, 0),
        weekday_tue = fifelse(weekday == 2L, 1, 0),
        weekday_wed = fifelse(weekday == 3L, 1, 0),
        weekday_thu = fifelse(weekday == 4L, 1, 0),
        weekday_fri = fifelse(weekday == 5L, 1, 0),
        weekday_sat = fifelse(weekday == 6L, 1, 0),
        weekday_sun = fifelse(weekday == 7L, 1, 0),
        diff_dsm_dem = dsm - dem
    )]

    dt
}

define_feature_sets <- function() {
    # Static/slow-changing covariates (used as both temporal sequence features
    # and static features at the target location)
    static_covars <- c(
        "is_weekend", "d_road", "dsm", "dem", "mtpi", "mtpi_1km",
        "lc_CRP", "lc_FST", "lc_WET", "lc_IMP", "lc_WTR",
        "weekday_mon", "weekday_tue", "weekday_wed", "weekday_thu",
        "weekday_fri", "weekday_sat", "weekday_sun", "diff_dsm_dem"
    )
    # Temporal features for neighbor sequences include PM observations
    neighbor_temporal <- c(static_covars, "PM10", "PM25")
    # Target's own temporal features (exclude PM targets at prediction time)
    target_temporal <- static_covars

    list(
        static_covars      = static_covars,
        neighbor_temporal  = neighbor_temporal,
        target_temporal    = target_temporal
    )
}

build_neighbor_index <- function(dt, K) {
    # Unique station locations (use median coords for relocated stations)
    station_locs <- dt[, .(
        coord_x = median(coord_x, na.rm = TRUE),
        coord_y = median(coord_y, na.rm = TRUE),
        dem_med = median(dem, na.rm = TRUE)
    ), by = TMSID2]
    setkey(station_locs, TMSID2)

    n_stations <- nrow(station_locs)
    K_actual <- min(K, n_stations - 1L)

    # Pairwise distance matrix (EPSG:5179 = meters)
    xy <- as.matrix(station_locs[, .(coord_x, coord_y)])
    dist_mat <- as.matrix(dist(xy))

    # For each station: indices, distances, bearings to K nearest neighbors
    nn_idx <- matrix(0L, nrow = n_stations, ncol = K_actual)
    nn_dist <- matrix(0, nrow = n_stations, ncol = K_actual)

    for (i in seq_len(n_stations)) {
        ord <- order(dist_mat[i, ])
        # Exclude self (index 1 in sorted order)
        nn_idx[i, ] <- ord[2:(K_actual + 1L)]
        nn_dist[i, ] <- dist_mat[i, nn_idx[i, ]]
    }

    # Compute bearings (azimuth from target to neighbor)
    nn_bearing_sin <- matrix(0, nrow = n_stations, ncol = K_actual)
    nn_bearing_cos <- matrix(0, nrow = n_stations, ncol = K_actual)
    nn_delta_elev <- matrix(0, nrow = n_stations, ncol = K_actual)

    for (i in seq_len(n_stations)) {
        for (k in seq_len(K_actual)) {
            j <- nn_idx[i, k]
            dx <- xy[j, 1] - xy[i, 1]
            dy <- xy[j, 2] - xy[i, 2]
            bearing <- atan2(dx, dy)
            nn_bearing_sin[i, k] <- sin(bearing)
            nn_bearing_cos[i, k] <- cos(bearing)
            nn_delta_elev[i, k] <- station_locs$dem_med[j] - station_locs$dem_med[i]
        }
    }

    list(
        station_locs   = station_locs,
        nn_idx         = nn_idx,
        nn_dist_m      = nn_dist,
        nn_bearing_sin = nn_bearing_sin,
        nn_bearing_cos = nn_bearing_cos,
        nn_delta_elev  = nn_delta_elev,
        K              = K_actual
    )
}

compute_spatial_rel_features <- function(nn_info, station_idx) {
    # Returns [K, 6] matrix of relative spatial features
    K <- nn_info$K
    dist_km <- nn_info$nn_dist_m[station_idx, ] / 1000
    cbind(
        dist_km     = dist_km,
        inv_dist    = 1 / (dist_km + 0.1),
        log_dist    = log(dist_km + 1),
        bearing_sin = nn_info$nn_bearing_sin[station_idx, ],
        bearing_cos = nn_info$nn_bearing_cos[station_idx, ],
        delta_elev  = nn_info$nn_delta_elev[station_idx, ]
    )
}

apply_rolling_mean <- function(dt, covars, window = 7L) {
    dt[, (covars) := lapply(.SD, function(x) {
        frollmean(x, window, fill = mean(x, na.rm = TRUE))
    }), .SDcols = covars, by = TMSID2]
    dt
}

normalize_features <- function(train_dt, val_dt, test_dt, cols) {
    mu <- train_dt[, lapply(.SD, mean, na.rm = TRUE), .SDcols = cols]
    sigma <- train_dt[, lapply(.SD, sd, na.rm = TRUE), .SDcols = cols]
    mu_vec <- unlist(mu, use.names = TRUE)
    sigma_vec <- unlist(sigma, use.names = TRUE)
    sigma_vec[sigma_vec == 0 | is.na(sigma_vec)] <- 1

    norm_fn <- function(dt) {
        dt[, (cols) := Map(
            function(col, m, s) (col - m) / s,
            .SD, mu_vec[cols], sigma_vec[cols]
        ), .SDcols = cols]
        dt
    }
    norm_fn(train_dt)
    norm_fn(val_dt)
    norm_fn(test_dt)

    list(mu = mu_vec, sigma = sigma_vec)
}

split_by_time <- function(dt, train_prop, val_prop) {
    dates <- sort(unique(dt$date))
    n <- length(dates)
    train_end <- dates[floor(n * train_prop)]
    val_end <- dates[floor(n * (train_prop + val_prop))]

    list(
        train = dt[date <= train_end],
        val   = dt[date > train_end & date <= val_end],
        test  = dt[date > val_end]
    )
}

# ============================================================================
# Dataset
# ============================================================================

spatiotemporal_dataset <- dataset(
    name = "spatiotemporal_dataset",
    initialize = function(dt, nn_info, feat_sets, seq_len, cfg) {
        cfg_num_workers <- suppressWarnings(as.integer(cfg$num_workers))
        if (length(cfg_num_workers) != 1L || is.na(cfg_num_workers)) {
            cfg_num_workers <- 0L
        }
        cfg_cache_tensors <- cfg$cache_dataset_tensors
        if (length(cfg_cache_tensors) != 1L) cfg_cache_tensors <- NULL

        self$seq_len <- seq_len
        self$K <- nn_info$K
        self$nn_info <- nn_info
        self$target_feats <- feat_sets$target_temporal
        self$neighbor_feats <- feat_sets$neighbor_temporal
        self$static_feats <- feat_sets$static_covars
        self$cache_tensors <- isTRUE(cfg_cache_tensors)
        if (is.null(cfg_cache_tensors)) {
            self$cache_tensors <- cfg_num_workers == 0L
        }
        if (self$cache_tensors && cfg_num_workers > 0L) {
            self$cache_tensors <- FALSE
        }

        station_locs <- nn_info$station_locs
        station_ids <- station_locs$TMSID2

        # Build per-station ordered data matrices
        self$station_data <- list()
        self$station_targets <- list()
        self$station_dates <- list()
        self$station_idx_map <- list()
        self$x_rel_cache <- vector("list", length = nrow(station_locs))

        for (idx in seq_along(station_ids)) {
            sid <- station_ids[idx]
            sdt <- dt[TMSID2 == sid][order(date)]
            if (nrow(sdt) < seq_len) next

            # Neighbor temporal features matrix (includes PM10/PM25)
            nb_cols <- intersect(self$neighbor_feats, names(sdt))
            mat_nb <- as.matrix(sdt[, ..nb_cols])
            storage.mode(mat_nb) <- "double"
            mat_nb[is.na(mat_nb)] <- 0

            # Target temporal features matrix (excludes PM targets)
            tgt_cols <- intersect(self$target_feats, names(sdt))
            mat_tgt <- as.matrix(sdt[, ..tgt_cols])
            storage.mode(mat_tgt) <- "double"
            mat_tgt[is.na(mat_tgt)] <- 0

            # Static features (from last row as representative)
            static_cols <- intersect(self$static_feats, names(sdt))
            static_vec <- as.numeric(sdt[.N, ..static_cols])
            static_vec[is.na(static_vec)] <- 0

            # Targets
            pm10 <- as.numeric(sdt$PM10)
            pm25 <- as.numeric(sdt$PM25)

            pm10_mask <- !is.na(pm10)
            pm25_mask <- !is.na(pm25)
            pm10[!pm10_mask] <- 0
            pm25[!pm25_mask] <- 0

            station_entry <- list(
                nb_mat       = mat_nb,
                tgt_mat      = mat_tgt,
                static_vec   = static_vec,
                pm10         = pm10,
                pm25         = pm25,
                pm10_mask    = pm10_mask,
                pm25_mask    = pm25_mask,
                n_rows       = nrow(sdt)
            )
            if (self$cache_tensors) {
                station_entry$nb_tensor <- torch_tensor(mat_nb, dtype = torch_float32())
                station_entry$tgt_tensor <- torch_tensor(mat_tgt, dtype = torch_float32())
                station_entry$static_tensor <- torch_tensor(static_vec, dtype = torch_float32())
                station_entry$pm10_tensor <- torch_tensor(pm10, dtype = torch_float32())
                station_entry$pm25_tensor <- torch_tensor(pm25, dtype = torch_float32())
                station_entry$pm10_mask_tensor <- torch_tensor(pm10_mask, dtype = torch_bool())
                station_entry$pm25_mask_tensor <- torch_tensor(pm25_mask, dtype = torch_bool())
            }
            self$station_data[[as.character(idx)]] <- station_entry
            self$station_dates[[as.character(idx)]] <- sdt$date
            self$station_idx_map[[sid]] <- idx
            x_rel_i <- compute_spatial_rel_features(self$nn_info, idx)
            if (self$cache_tensors) {
                self$x_rel_cache[[idx]] <- torch_tensor(x_rel_i, dtype = torch_float32())
            } else {
                self$x_rel_cache[[idx]] <- x_rel_i
            }
        }

        # Enumerate valid (station_idx, time_idx) samples
        # A sample is valid if target station and all K neighbors have seq_len
        # consecutive rows ending at time_idx
        self$samples <- self$.enumerate_samples()
        cat(sprintf(
            "Dataset: %d valid samples from %d stations\n",
            length(self$samples), length(self$station_data)
        ))
    },
    .enumerate_samples = function() {
        samples <- list()
        station_locs <- self$nn_info$station_locs
        n_stations <- nrow(station_locs)

        for (i in seq_len(n_stations)) {
            si <- as.character(i)
            if (is.null(self$station_data[[si]])) next

            sdata <- self$station_data[[si]]
            n_rows <- sdata$n_rows

            # Check that all K neighbors have data
            neighbor_indices <- self$nn_info$nn_idx[i, ]
            neighbors_ok <- all(vapply(neighbor_indices, function(j) {
                !is.null(self$station_data[[as.character(j)]])
            }, logical(1)))
            if (!neighbors_ok) next

            # Get neighbor min rows
            nb_min_rows <- min(vapply(neighbor_indices, function(j) {
                self$station_data[[as.character(j)]]$n_rows
            }, integer(1)))

            # Valid time indices: seq_len to min(n_rows, nb_min_rows)
            max_t <- min(n_rows, nb_min_rows)
            if (max_t < self$seq_len) next

            for (t_idx in seq(self$seq_len, max_t)) {
                # Check target has at least one non-NA PM value
                pm10_val <- sdata$pm10[t_idx]
                pm25_val <- sdata$pm25[t_idx]
                if (is.na(pm10_val) && is.na(pm25_val)) next

                samples[[length(samples) + 1L]] <- list(
                    station_idx = i,
                    time_idx    = t_idx
                )
            }
        }
        samples
    },
    .getitem = function(i) {
        s <- self$samples[[i]]
        si <- as.character(s$station_idx)
        t_end <- s$time_idx
        t_start <- t_end - self$seq_len + 1L
        sdata <- self$station_data[[si]]

        # Target temporal sequence: [seq_len, n_target_feats]
        if (self$cache_tensors) {
            x_target <- sdata$tgt_tensor[t_start:t_end, ]
            x_static <- sdata$static_tensor
        } else {
            x_target <- torch_tensor(sdata$tgt_mat[t_start:t_end, , drop = FALSE], dtype = torch_float32())
            x_static <- torch_tensor(sdata$static_vec, dtype = torch_float32())
        }

        # Neighbor temporal sequences: [K, seq_len, n_neighbor_feats]
        n_nb_feats <- ncol(self$station_data[[as.character(
            self$nn_info$nn_idx[s$station_idx, 1]
        )]]$nb_mat)
        x_neighbors <- torch_zeros(c(self$K, self$seq_len, n_nb_feats), dtype = torch_float32())

        for (k in seq_len(self$K)) {
            nb_idx <- self$nn_info$nn_idx[s$station_idx, k]
            nb_si <- as.character(nb_idx)
            nb_data <- self$station_data[[nb_si]]
            # Use same time window (aligned by row index)
            nb_t_end <- min(t_end, nb_data$n_rows)
            nb_t_start <- nb_t_end - self$seq_len + 1L
            if (nb_t_start < 1L) {
                nb_t_start <- 1L
                nb_t_end <- self$seq_len
            }
            if (self$cache_tensors) {
                x_neighbors[k, , ] <- nb_data$nb_tensor[nb_t_start:nb_t_end, ]
            } else {
                x_neighbors[k, , ] <- torch_tensor(
                    nb_data$nb_mat[nb_t_start:nb_t_end, , drop = FALSE],
                    dtype = torch_float32()
                )
            }
        }

        # Spatial relation features: [K, 6]
        x_rel <- if (self$cache_tensors) {
            self$x_rel_cache[[s$station_idx]]
        } else {
            torch_tensor(self$x_rel_cache[[s$station_idx]], dtype = torch_float32())
        }

        # Targets: [2] (PM10, PM25) with mask
        if (self$cache_tensors) {
            y <- torch_stack(list(sdata$pm10_tensor[t_end], sdata$pm25_tensor[t_end]))
            target_mask <- torch_stack(list(
                sdata$pm10_mask_tensor[t_end],
                sdata$pm25_mask_tensor[t_end]
            ))
        } else {
            y <- torch_tensor(c(sdata$pm10[t_end], sdata$pm25[t_end]), dtype = torch_float32())
            target_mask <- torch_tensor(
                c(sdata$pm10_mask[t_end], sdata$pm25_mask[t_end]),
                dtype = torch_bool()
            )
        }

        list(
            x_target    = x_target,
            x_static    = x_static,
            x_neighbors = x_neighbors,
            x_rel       = x_rel,
            y           = y,
            target_mask = target_mask
        )
    },
    .length = function() {
        length(self$samples)
    }
)

# ============================================================================
# Model Modules
# ============================================================================

# --- Mamba Block (reused from experiment_rnn.R) ---
mamba_block <- nn_module(
    "mamba_block",
    initialize = function(d_model, d_inner, d_state, conv_kernel = 3L, dropout = 0.1,
                          vectorized_ssm = TRUE) {
        self$d_inner <- d_inner
        self$vectorized_ssm <- isTRUE(vectorized_ssm)
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

        decay <- torch_exp(-torch_exp(self$A_log))
        if (self$vectorized_ssm && exists("torch_einsum", where = asNamespace("torch"), inherits = FALSE)) {
            # state[t] = sum_{i<=t} (decay^(t-i) * u[i] * B), fully vectorized on device.
            time_idx <- torch_arange(
                start = 0, end = as.integer(s - 1L),
                device = x$device, dtype = torch_float32()
            )
            dt <- time_idx$unsqueeze(2) - time_idx$unsqueeze(1)  # [S, S]
            causal <- (dt >= 0)$to(dtype = ssm_in$dtype)
            dt <- dt$clamp(min = 0)$to(dtype = ssm_in$dtype)

            decay_grid <- torch_pow(decay$view(c(1, 1, -1)), dt$unsqueeze(-1))
            kernel <- decay_grid * causal$unsqueeze(-1)  # [S, S, D]
            state <- torch_einsum("bsd,std->btd", list(ssm_in, kernel))
            state <- state * self$B$view(c(1, 1, -1))
            y <- state * self$C$view(c(1, 1, -1)) + ssm_in * self$D$view(c(1, 1, -1))
        } else {
            state <- torch_zeros(c(b, self$A_log$size(1)), device = x$device)
            outs <- vector("list", s)
            for (t in seq_len(s)) {
                u_t <- ssm_in[, t, ]
                state <- state * decay + u_t * self$B
                y_t <- state * self$C + u_t * self$D
                outs[[t]] <- y_t$unsqueeze(2)
            }
            y <- torch_cat(outs, dim = 2)
        }
        y <- self$from_state(y)
        y <- y * torch_sigmoid(g)
        y <- self$out_proj(y)
        y <- self$dropout(y)

        residual + y
    }
)

# --- Temporal Encoder: embedding + Mamba blocks + last-step pooling ---
temporal_encoder <- nn_module(
    "temporal_encoder",
    initialize = function(input_dim, d_model, d_inner, d_state,
                          n_layers, conv_kernel, dropout, vectorized_ssm = TRUE) {
        self$embed <- nn_linear(input_dim, d_model)
        self$blocks <- nn_module_list(lapply(
            seq_len(n_layers),
            function(i) mamba_block(
                d_model, d_inner, d_state, conv_kernel, dropout,
                vectorized_ssm = vectorized_ssm
            )
        ))
        self$norm <- nn_layer_norm(d_model)
    },
    forward = function(x) {
        # x: [B, T, F]
        h <- self$embed(x)
        for (i in seq_len(length(self$blocks))) {
            h <- self$blocks[[i]](h)
        }
        h <- self$norm(h)
        h[, h$size(2), ] # [B, D] last time step
    }
)

# --- Spatial Relation Encoder ---
spatial_rel_encoder <- nn_module(
    "spatial_rel_encoder",
    initialize = function(input_dim, d_model) {
        self$mlp <- nn_sequential(
            nn_linear(input_dim, d_model),
            nn_silu(),
            nn_linear(d_model, d_model)
        )
    },
    forward = function(x_rel) {
        # x_rel: [B, K, F_r]
        self$mlp(x_rel) # [B, K, D]
    }
)

# --- Spatial Cross-Attention with Distance Bias ---
spatial_cross_attention <- nn_module(
    "spatial_cross_attention",
    initialize = function(d_model, n_heads, dropout) {
        if ((d_model %% n_heads) != 0L) {
            stop(sprintf(
                "Invalid attention config: d_model (%d) must be divisible by n_heads (%d).",
                d_model, n_heads
            ))
        }
        self$n_heads <- n_heads
        self$head_dim <- as.integer(d_model / n_heads)
        self$scale <- 1.0 / sqrt(self$head_dim)

        self$q_proj <- nn_linear(d_model, d_model)
        self$k_proj <- nn_linear(d_model, d_model)
        self$v_proj <- nn_linear(d_model, d_model)
        self$dist_bias_proj <- nn_linear(d_model, n_heads)
        self$out_proj <- nn_linear(d_model, d_model)

        self$norm1 <- nn_layer_norm(d_model)
        self$norm2 <- nn_layer_norm(d_model)
        self$ffn <- nn_sequential(
            nn_linear(d_model, d_model * 4L),
            nn_silu(),
            nn_dropout(p = dropout),
            nn_linear(d_model * 4L, d_model)
        )
        self$dropout <- nn_dropout(p = dropout)
    },
    forward = function(h_target, h_neighbors, s_rel) {
        # h_target:    [B, D]
        # h_neighbors: [B, K, D]
        # s_rel:       [B, K, D]  (spatial relation embeddings)
        B <- h_target$size(1)
        K <- h_neighbors$size(2)
        H <- self$n_heads
        Dh <- self$head_dim

        # Add spatial conditioning to neighbor representations
        h_nb_cond <- h_neighbors + s_rel

        # Q from target, K/V from conditioned neighbors
        Q <- self$q_proj(h_target)$view(c(B, 1L, H, Dh))$permute(c(1, 3, 2, 4))
        Kt <- self$k_proj(h_nb_cond)$view(c(B, K, H, Dh))$permute(c(1, 3, 2, 4))
        V <- self$v_proj(h_nb_cond)$view(c(B, K, H, Dh))$permute(c(1, 3, 2, 4))

        # Attention scores + distance bias
        attn <- torch_matmul(Q, Kt$transpose(-2, -1)) * self$scale
        dist_bias <- self$dist_bias_proj(s_rel)$permute(c(1, 3, 2))$unsqueeze(3)
        attn <- attn + dist_bias
        attn <- nnf_softmax(attn, dim = -1)
        attn <- self$dropout(attn)

        # Aggregate
        context <- torch_matmul(attn, V)
        context <- context$permute(c(1, 3, 2, 4))$reshape(c(B, -1))
        context <- self$out_proj(context)

        # Residual + FFN
        h <- self$norm1(h_target + self$dropout(context))
        h <- self$norm2(h + self$dropout(self$ffn(h)))
        h
    }
)

# --- Gated Fusion Module ---
fusion_module <- nn_module(
    "fusion_module",
    initialize = function(n_static_features, d_model) {
        self$static_proj <- nn_linear(n_static_features, d_model)
        self$gate_net <- nn_sequential(
            nn_linear(d_model * 2L, d_model),
            nn_sigmoid()
        )
        self$norm <- nn_layer_norm(d_model)
    },
    forward = function(h_spatiotemporal, x_static) {
        h_static <- self$static_proj(x_static)
        h_cat <- torch_cat(list(h_spatiotemporal, h_static), dim = 2)
        gate <- self$gate_net(h_cat)
        h <- gate * h_spatiotemporal + (1 - gate) * h_static
        self$norm(h)
    }
)

# --- Top-Level Mamba-Transformer Spatiotemporal Model ---
mamba_transformer_st <- nn_module(
    "mamba_transformer_st",
    initialize = function(n_target_temporal_features,
                          n_neighbor_temporal_features,
                          n_static_features,
                          n_spatial_rel_features = 6L,
                          d_model = 64L,
                          d_inner = 128L,
                          d_state = 64L,
                          n_mamba_layers = 3L,
                          conv_kernel = 3L,
                          n_cross_attn_layers = 2L,
                          n_heads = 4L,
                          dropout = 0.1,
                          n_targets = 2L,
                          vectorized_ssm = TRUE) {
        # Separate temporal encoders for target vs neighbors
        # (different input dims: target has no PM cols, neighbors have PM cols)
        self$target_encoder <- temporal_encoder(
            n_target_temporal_features, d_model, d_inner, d_state,
            n_mamba_layers, conv_kernel, dropout, vectorized_ssm = vectorized_ssm
        )
        self$neighbor_encoder <- temporal_encoder(
            n_neighbor_temporal_features, d_model, d_inner, d_state,
            n_mamba_layers, conv_kernel, dropout, vectorized_ssm = vectorized_ssm
        )

        self$spatial_rel_enc <- spatial_rel_encoder(n_spatial_rel_features, d_model)

        self$cross_attn_layers <- nn_module_list(lapply(
            seq_len(n_cross_attn_layers),
            function(i) spatial_cross_attention(d_model, n_heads, dropout)
        ))

        self$fusion <- fusion_module(n_static_features, d_model)

        self$pred_head <- nn_sequential(
            nn_linear(d_model, d_model),
            nn_silu(),
            nn_dropout(p = dropout),
            nn_linear(d_model, n_targets)
        )
    },
    forward = function(x_target, x_static, x_neighbors, x_rel) {
        # x_target:    [B, T, F_target]
        # x_static:    [B, F_static]
        # x_neighbors: [B, K, T, F_neighbor]
        # x_rel:       [B, K, 6]

        B <- x_target$size(1)
        K <- x_neighbors$size(2)
        TT <- x_neighbors$size(3)

        # 1. Temporal encoding of target
        h_target <- self$target_encoder(x_target) # [B, D]

        # 2. Temporal encoding of neighbors (batched)
        x_nb_flat <- x_neighbors$reshape(c(B * K, TT, -1)) # [B*K, T, F_nb]
        h_nb_flat <- self$neighbor_encoder(x_nb_flat) # [B*K, D]
        h_neighbors <- h_nb_flat$reshape(c(B, K, -1)) # [B, K, D]

        # 3. Spatial relation encoding
        s_rel <- self$spatial_rel_enc(x_rel) # [B, K, D]

        # 4. Cross-attention layers
        h <- h_target
        for (i in seq_len(length(self$cross_attn_layers))) {
            h <- self$cross_attn_layers[[i]](h, h_neighbors, s_rel)
        }

        # 5. Fuse with static features
        h <- self$fusion(h, x_static) # [B, D]

        # 6. Predict PM10 + PM25
        self$pred_head(h) # [B, 2]
    }
)

# ============================================================================
# Training
# ============================================================================

masked_mse_loss <- function(pred, y, mask) {
    # pred: [B, 2], y: [B, 2], mask: [B, 2] (bool)
    mask_f <- mask$to(dtype = torch_float32())
    sq_err <- (pred - y)^2 * mask_f

    n_pm10 <- mask[, 1]$sum()$clamp(min = 1)
    n_pm25 <- mask[, 2]$sum()$clamp(min = 1)

    loss_pm10 <- sq_err[, 1]$sum() / n_pm10
    loss_pm25 <- sq_err[, 2]$sum() / n_pm25

    loss_pm10 + loss_pm25
}

train_model <- function(model, train_dl, val_dl, cfg) {
    device <- cfg$device
    model <- model$to(device = device)
    optimizer <- optim_adamw(
        model$parameters,
        lr = cfg$learning_rate,
        weight_decay = cfg$weight_decay
    )
    scheduler <- lr_reduce_on_plateau(
        optimizer,
        factor = cfg$lr_factor,
        patience = cfg$lr_patience
    )

    best_val_loss <- Inf
    best_state <- NULL
    epochs_no_improve <- 0L
    use_cuda <- grepl("cuda", as.character(device), fixed = TRUE)
    use_amp <- isTRUE(cfg$use_amp) && use_cuda
    amp_dtype <- resolve_amp_dtype(cfg$amp_dtype)
    scaler <- if (use_amp) cuda_amp_grad_scaler(enabled = TRUE) else NULL

    for (epoch in seq_len(cfg$max_epochs)) {
        # --- Train ---
        model$train()
        train_loss_sum <- 0
        train_batches <- 0L
        train_se_sum <- 0
        train_ae_sum <- 0
        train_n_obs <- 0

        coro::loop(for (batch in train_dl) {
            x_target <- batch$x_target$to(device = device)
            x_static <- batch$x_static$to(device = device)
            x_neighbors <- batch$x_neighbors$to(device = device)
            x_rel <- batch$x_rel$to(device = device)
            y <- batch$y$to(device = device)
            mask <- batch$target_mask$to(device = device)

            optimizer$zero_grad()
            if (use_amp) {
                out <- with_autocast(
                    {
                        pred <- model(x_target, x_static, x_neighbors, x_rel)
                        list(
                            pred = pred,
                            loss = masked_mse_loss(pred, y, mask)
                        )
                    },
                    device_type = "cuda",
                    dtype = amp_dtype,
                    enabled = TRUE
                )
                pred <- out$pred
                loss <- out$loss
                scaler$scale(loss)$backward()
                scaler$unscale_(optimizer)
                nn_utils_clip_grad_norm_(model$parameters, max_norm = cfg$grad_clip_norm)
                scaler$step(optimizer)
                scaler$update()
            } else {
                pred <- model(x_target, x_static, x_neighbors, x_rel)
                loss <- masked_mse_loss(pred, y, mask)
                loss$backward()
                nn_utils_clip_grad_norm_(model$parameters, max_norm = cfg$grad_clip_norm)
                optimizer$step()
            }

            train_loss_sum <- train_loss_sum + loss$item()
            train_batches <- train_batches + 1L
            mask_f <- mask$to(dtype = torch_float32())
            err <- (pred - y) * mask_f
            train_se_sum <- train_se_sum + ((err^2)$sum())$item()
            train_ae_sum <- train_ae_sum + ((err$abs())$sum())$item()
            train_n_obs <- train_n_obs + (mask_f$sum())$item()
        })

        # --- Validate ---
        model$eval()
        val_loss_sum <- 0
        val_batches <- 0L
        val_se_sum <- 0
        val_ae_sum <- 0
        val_n_obs <- 0

        with_no_grad({
            coro::loop(for (batch in val_dl) {
                x_target <- batch$x_target$to(device = device)
                x_static <- batch$x_static$to(device = device)
                x_neighbors <- batch$x_neighbors$to(device = device)
                x_rel <- batch$x_rel$to(device = device)
                y <- batch$y$to(device = device)
                mask <- batch$target_mask$to(device = device)

                if (use_amp) {
                    out <- with_autocast(
                        {
                            pred <- model(x_target, x_static, x_neighbors, x_rel)
                            list(
                                pred = pred,
                                loss = masked_mse_loss(pred, y, mask)
                            )
                        },
                        device_type = "cuda",
                        dtype = amp_dtype,
                        enabled = TRUE
                    )
                    pred <- out$pred
                    loss <- out$loss
                } else {
                    pred <- model(x_target, x_static, x_neighbors, x_rel)
                    loss <- masked_mse_loss(pred, y, mask)
                }
                val_loss_sum <- val_loss_sum + loss$item()
                val_batches <- val_batches + 1L
                mask_f <- mask$to(dtype = torch_float32())
                err <- (pred - y) * mask_f
                val_se_sum <- val_se_sum + ((err^2)$sum())$item()
                val_ae_sum <- val_ae_sum + ((err$abs())$sum())$item()
                val_n_obs <- val_n_obs + (mask_f$sum())$item()
            })
        })

        mean_train <- train_loss_sum / max(train_batches, 1L)
        mean_val <- val_loss_sum / max(val_batches, 1L)
        train_rmse <- sqrt(train_se_sum / max(train_n_obs, 1))
        train_mae <- train_ae_sum / max(train_n_obs, 1)
        val_rmse <- sqrt(val_se_sum / max(val_n_obs, 1))
        val_mae <- val_ae_sum / max(val_n_obs, 1)

        # LR scheduler step
        scheduler$step(mean_val)

        # Early stopping check
        if (mean_val < best_val_loss - cfg$min_delta) {
            best_val_loss <- mean_val
            best_state <- model$state_dict()
            epochs_no_improve <- 0L
        } else {
            epochs_no_improve <- epochs_no_improve + 1L
        }

        if (epoch %% 5L == 0L || epoch == 1L || epochs_no_improve >= cfg$patience) {
            cat(sprintf(
                "[Epoch %03d] train_loss=%.5f rmse=%.4f mae=%.4f | val_loss=%.5f rmse=%.4f mae=%.4f | best=%.5f | patience=%d/%d\n",
                epoch, mean_train, train_rmse, train_mae, mean_val, val_rmse, val_mae, best_val_loss,
                epochs_no_improve, cfg$patience
            ))
        }

        if (epochs_no_improve >= cfg$patience) {
            cat(sprintf("Early stopping at epoch %d\n", epoch))
            break
        }
    }

    # Restore best model
    if (!is.null(best_state)) {
        model$load_state_dict(best_state)
    }

    list(model = model, best_val_loss = best_val_loss, final_epoch = epoch)
}

# ============================================================================
# Evaluation
# ============================================================================

evaluate_model <- function(model, test_dl, device) {
    model$eval()
    all_preds <- list()
    all_trues <- list()
    all_masks <- list()

    with_no_grad({
        coro::loop(for (batch in test_dl) {
            x_target <- batch$x_target$to(device = device)
            x_static <- batch$x_static$to(device = device)
            x_neighbors <- batch$x_neighbors$to(device = device)
            x_rel <- batch$x_rel$to(device = device)
            y <- batch$y
            mask <- batch$target_mask

            pred <- model(x_target, x_static, x_neighbors, x_rel)
            pred <- pred$to(device = "cpu")

            all_preds[[length(all_preds) + 1L]] <- as.matrix(pred)
            all_trues[[length(all_trues) + 1L]] <- as.matrix(y)
            all_masks[[length(all_masks) + 1L]] <- as.matrix(mask)
        })
    })

    preds <- do.call(rbind, all_preds)
    trues <- do.call(rbind, all_trues)
    masks <- do.call(rbind, all_masks)

    metrics <- list()
    for (col_idx in 1:2) {
        target_name <- c("PM10", "PM25")[col_idx]
        valid <- masks[, col_idx] == 1
        if (sum(valid) == 0) {
            metrics[[target_name]] <- list(rmse = NA, mae = NA, rsq = NA, n = 0)
            next
        }
        p <- preds[valid, col_idx]
        t <- trues[valid, col_idx]
        metrics[[target_name]] <- list(
            rmse = Metrics::rmse(t, p),
            mae  = Metrics::mae(t, p),
            rsq  = cor(t, p)^2,
            n    = sum(valid)
        )
    }
    metrics
}

# ============================================================================
# Checkpoint Save/Load
# ============================================================================

save_st_model_bundle <- function(model, file_path, model_hparams,
                                 feat_sets, norm_stats, nn_info_summary,
                                 metrics, cfg) {
    bundle <- list(
        model_state     = model$state_dict(),
        model_hparams   = model_hparams,
        feat_sets       = feat_sets,
        norm_stats      = norm_stats,
        nn_info_summary = nn_info_summary,
        metrics         = metrics,
        config          = cfg[setdiff(names(cfg), "device")],
        saved_at        = as.character(Sys.time())
    )
    torch_save(bundle, file_path)
    cat(sprintf("Model saved: %s\n", file_path))
    invisible(file_path)
}

load_st_model_bundle <- function(file_path, device = NULL) {
    if (is.null(device)) {
        device <- if (cuda_is_available()) torch_device("cuda") else torch_device("cpu")
    }
    bundle <- torch_load(file_path)
    hp <- bundle$model_hparams

    vectorized_ssm_hp <- if (!is.null(hp$vectorized_ssm)) isTRUE(hp$vectorized_ssm) else TRUE
    model <- mamba_transformer_st(
        n_target_temporal_features   = as.integer(hp$n_target_temporal_features),
        n_neighbor_temporal_features = as.integer(hp$n_neighbor_temporal_features),
        n_static_features            = as.integer(hp$n_static_features),
        n_spatial_rel_features       = as.integer(hp$n_spatial_rel_features),
        d_model                      = as.integer(hp$d_model),
        d_inner                      = as.integer(hp$d_inner),
        d_state                      = as.integer(hp$d_state),
        n_mamba_layers               = as.integer(hp$n_mamba_layers),
        conv_kernel                  = as.integer(hp$conv_kernel),
        n_cross_attn_layers          = as.integer(hp$n_cross_attn_layers),
        n_heads                      = as.integer(hp$n_heads),
        dropout                      = as.numeric(hp$dropout),
        vectorized_ssm               = vectorized_ssm_hp
    )
    model$load_state_dict(bundle$model_state)
    model <- model$to(device = device)

    list(model = model, metadata = bundle)
}

# ============================================================================
# Main Experiment Runner
# ============================================================================

run_st_experiment <- function(cfg = default_st_config,
                              subset_stations = NULL,
                              subset_max_date = NULL,
                              subset_date_range = NULL) {
    report_pm_ranges <- function(dt_in, label = "Input") {
        pm10_min <- suppressWarnings(min(dt_in$PM10, na.rm = TRUE))
        pm10_max <- suppressWarnings(max(dt_in$PM10, na.rm = TRUE))
        pm25_min <- suppressWarnings(min(dt_in$PM25, na.rm = TRUE))
        pm25_max <- suppressWarnings(max(dt_in$PM25, na.rm = TRUE))

        if (!is.finite(pm10_min)) pm10_min <- NA_real_
        if (!is.finite(pm10_max)) pm10_max <- NA_real_
        if (!is.finite(pm25_min)) pm25_min <- NA_real_
        if (!is.finite(pm25_max)) pm25_max <- NA_real_

        cat(sprintf(
            "%s PM ranges | PM10: [%.3f, %.3f] | PM2.5: [%.3f, %.3f]\n",
            label, pm10_min, pm10_max, pm25_min, pm25_max
        ))
    }

    cfg_num_workers <- suppressWarnings(as.integer(cfg$num_workers))
    if (length(cfg_num_workers) != 1L || is.na(cfg_num_workers)) {
        cfg_num_workers <- 0L
        cfg$num_workers <- 0L
    }
    cfg_cache_tensors <- cfg$cache_dataset_tensors
    if (length(cfg_cache_tensors) != 1L) {
        cfg_cache_tensors <- NULL
        cfg$cache_dataset_tensors <- NULL
    }

    if (is.null(cfg_cache_tensors)) {
        cfg$cache_dataset_tensors <- cfg_num_workers == 0L
    }
    if (isTRUE(cfg$cache_dataset_tensors) && cfg_num_workers > 0L) {
        cat("Disabling dataset tensor cache because num_workers > 0 (worker-safe mode).\n")
        cfg$cache_dataset_tensors <- FALSE
    }
    if ((as.integer(cfg$d_model) %% as.integer(cfg$n_heads)) != 0L) {
        stop(sprintf(
            "Invalid model config: d_model (%d) must be divisible by n_heads (%d). For d_model=%d, use n_heads such as 1, 2, 4, 8, 16, or %d.",
            as.integer(cfg$d_model), as.integer(cfg$n_heads),
            as.integer(cfg$d_model), as.integer(cfg$d_model)
        ))
    }

    # --- Load and prepare ---
    dt <- load_and_prepare_data(cfg)

    # Optional subsetting for testing
    if (!is.null(subset_stations)) {
        all_stations <- unique(dt$TMSID2)
        if (is.numeric(subset_stations) && length(subset_stations) == 1) {
            subset_stations <- all_stations[seq_len(min(subset_stations, length(all_stations)))]
        }
        dt <- dt[TMSID2 %in% subset_stations]
        cat(sprintf("Subset: %d stations\n", length(subset_stations)))
    }
    if (!is.null(subset_date_range)) {
        if (length(subset_date_range) == 1L) {
            start_date <- as.Date(subset_date_range[[1]])
            end_date <- as.Date(subset_date_range[[1]])
        } else if (length(subset_date_range) >= 2L) {
            start_date <- as.Date(subset_date_range[[1]])
            end_date <- as.Date(subset_date_range[[2]])
        } else {
            stop("subset_date_range must have length 1 or 2.")
        }
        if (is.na(start_date) || is.na(end_date)) {
            stop("subset_date_range contains invalid date(s).")
        }
        if (start_date > end_date) {
            tmp <- start_date
            start_date <- end_date
            end_date <- tmp
        }
        dt <- dt[date >= start_date & date <= end_date]
        cat(sprintf("Subset: date range %s to %s\n", start_date, end_date))
    } else if (!is.null(subset_max_date)) {
        dt <- dt[date <= as.Date(subset_max_date)]
        cat(sprintf("Subset: dates up to %s\n", subset_max_date))
    }
    report_pm_ranges(dt, label = "Subsetted input")

    feat_sets <- define_feature_sets()

    # Filter stations with minimum observations
    station_obs <- dt[, .(
        n_pm10 = sum(!is.na(PM10)),
        n_pm25 = sum(!is.na(PM25))
    ), by = TMSID2]
    good_stations <- station_obs[
        n_pm10 >= cfg$min_obs_per_station | n_pm25 >= cfg$min_obs_per_station,
        TMSID2
    ]
    dt <- dt[TMSID2 %in% good_stations]
    cat(sprintf("Stations after filtering: %d\n", length(good_stations)))
    report_pm_ranges(dt, label = "Filtered input")

    if (length(good_stations) < cfg$K_neighbors + 1L) {
        stop(sprintf(
            "Need at least %d stations, got %d",
            cfg$K_neighbors + 1L, length(good_stations)
        ))
    }

    # --- Build neighbor index ---
    cat("Building neighbor index...\n")
    nn_info <- build_neighbor_index(dt, cfg$K_neighbors)

    # --- Rolling mean ---
    all_covars <- union(
        feat_sets$static_covars,
        setdiff(feat_sets$neighbor_temporal, c("PM10", "PM25"))
    )
    dt <- apply_rolling_mean(dt, all_covars, window = 7L)

    # --- Split ---
    splits <- split_by_time(dt, cfg$train_prop, cfg$val_prop)
    cat(sprintf(
        "Train: %d rows | Val: %d rows | Test: %d rows\n",
        nrow(splits$train), nrow(splits$val), nrow(splits$test)
    ))

    # --- Normalize ---
    norm_cols <- c(all_covars, "PM10", "PM25")
    norm_cols <- intersect(norm_cols, names(splits$train))
    norm_stats <- normalize_features(splits$train, splits$val, splits$test, norm_cols)

    # --- Build datasets ---
    cat("Building training dataset...\n")
    train_ds <- spatiotemporal_dataset(splits$train, nn_info, feat_sets, cfg$seq_len, cfg)
    cat("Building validation dataset...\n")
    val_ds <- spatiotemporal_dataset(splits$val, nn_info, feat_sets, cfg$seq_len, cfg)
    cat("Building test dataset...\n")
    test_ds <- spatiotemporal_dataset(splits$test, nn_info, feat_sets, cfg$seq_len, cfg)

    if (length(train_ds) == 0 || length(val_ds) == 0) {
        stop("Not enough valid samples. Try fewer K neighbors or a longer date range.")
    }

    dl_num_workers <- as.integer(cfg$num_workers)
    dl_pin_memory <- isTRUE(cfg$pin_memory) && grepl("cuda", as.character(cfg$device), fixed = TRUE)

    train_dl <- dataloader(
        train_ds,
        batch_size = cfg$batch_size,
        shuffle = TRUE,
        drop_last = TRUE,
        num_workers = dl_num_workers,
        pin_memory = dl_pin_memory
    )
    val_dl <- dataloader(
        val_ds,
        batch_size = cfg$batch_size,
        shuffle = FALSE,
        drop_last = FALSE,
        num_workers = dl_num_workers,
        pin_memory = dl_pin_memory
    )
    test_dl <- dataloader(
        test_ds,
        batch_size = cfg$batch_size,
        shuffle = FALSE,
        drop_last = FALSE,
        num_workers = dl_num_workers,
        pin_memory = dl_pin_memory
    )

    # --- Determine feature dimensions from first batch ---
    first_batch <- train_ds$.getitem(1)
    n_target_feats <- first_batch$x_target$size(2)
    n_neighbor_feats <- first_batch$x_neighbors$size(3)
    n_static_feats <- first_batch$x_static$size(1)

    cat(sprintf(
        "Feature dims: target_temporal=%d, neighbor_temporal=%d, static=%d\n",
        n_target_feats, n_neighbor_feats, n_static_feats
    ))

    # --- Build model ---
    model <- mamba_transformer_st(
        n_target_temporal_features   = n_target_feats,
        n_neighbor_temporal_features = n_neighbor_feats,
        n_static_features            = n_static_feats,
        n_spatial_rel_features       = 6L,
        d_model                      = cfg$d_model,
        d_inner                      = cfg$d_inner,
        d_state                      = cfg$d_state,
        n_mamba_layers               = cfg$n_mamba_layers,
        conv_kernel                  = cfg$conv_kernel,
        n_cross_attn_layers          = cfg$n_cross_attn_layers,
        n_heads                      = cfg$n_heads,
        dropout                      = cfg$dropout,
        vectorized_ssm               = cfg$use_vectorized_ssm
    )

    n_params <- sum(sapply(model$parameters, function(p) p$numel()))
    cat(sprintf("Model parameters: %s\n", format(n_params, big.mark = ",")))
    cat(sprintf("Device: %s\n", as.character(cfg$device)))

    # --- Train ---
    cat("Training...\n")
    train_result <- train_model(model, train_dl, val_dl, cfg)
    model <- train_result$model

    # --- Evaluate ---
    cat("Evaluating on test set...\n")
    metrics <- evaluate_model(model, test_dl, cfg$device)

    for (tgt in names(metrics)) {
        m <- metrics[[tgt]]
        cat(sprintf(
            "%s | RMSE: %.4f | MAE: %.4f | R2: %.4f | n=%d\n",
            tgt, m$rmse, m$mae, m$rsq, m$n
        ))
    }

    # --- Save artifacts ---
    artifacts <- list()
    if (isTRUE(cfg$save_artifacts)) {
        artifact_root <- ensure_dir(cfg$artifact_dir)
        ckpt_path <- file.path(artifact_root, "mamba_transformer_st.pt")

        model_hparams <- list(
            n_target_temporal_features = n_target_feats,
            n_neighbor_temporal_features = n_neighbor_feats,
            n_static_features = n_static_feats,
            n_spatial_rel_features = 6L,
            d_model = cfg$d_model,
            d_inner = cfg$d_inner,
            d_state = cfg$d_state,
            n_mamba_layers = cfg$n_mamba_layers,
            conv_kernel = cfg$conv_kernel,
            n_cross_attn_layers = cfg$n_cross_attn_layers,
            n_heads = cfg$n_heads,
            dropout = cfg$dropout,
            vectorized_ssm = cfg$use_vectorized_ssm
        )

        nn_info_summary <- list(
            station_ids = nn_info$station_locs$TMSID2,
            station_xy  = as.matrix(nn_info$station_locs[, .(coord_x, coord_y)]),
            K           = nn_info$K
        )

        save_st_model_bundle(
            model, ckpt_path, model_hparams,
            feat_sets, norm_stats, nn_info_summary,
            metrics, cfg
        )
        artifacts$checkpoint <- ckpt_path
    }

    list(
        model = model,
        metrics = metrics,
        config = cfg,
        artifacts = artifacts,
        train_result = train_result
    )
}

# ============================================================================
# Optional Small-Subset Test Run
# ============================================================================
# Set HUIMORI_RUN_ST_EXAMPLE=1 to run this block interactively.
if (TRUE) {#interactive() && identical(Sys.getenv("HUIMORI_RUN_ST_EXAMPLE"), "1")) {
    test_cfg <- default_st_config
    test_cfg$max_epochs <- 100L
    test_cfg$patience <- 5L
    test_cfg$batch_size   <- 64L
    test_cfg$K_neighbors  <- 5L
    # test_cfg$seq_len      <- 10L
    test_cfg$min_obs_per_station <- 72L
    # test_cfg$save_artifacts <- FALSE
    test_cfg$num_workers <- 0L  # use this with cache_dataset_tensors = TRUE
    test_cfg$cache_dataset_tensors <- TRUE
    test_cfg$use_amp <- TRUE # set to TRUE if using GPU and want faster training with mixed precision

    result <- run_st_experiment(
        cfg = test_cfg,
        subset_stations = 500,
        subset_date_range = c("2017-01-01", "2023-12-31")
    )
}
