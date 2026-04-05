###############################################################################
# Reusable inference utilities for Mamba-Transformer ST model checkpoints
# saved by mamba_transformer_st.R (save_st_model_bundle -> *.pt)
###############################################################################

library(torch)

resolve_amp_dtype <- function(dtype_name) {
  if (identical(dtype_name, "bfloat16")) {
    torch_bfloat16()
  } else {
    torch_float16()
  }
}

# -----------------------------------------------------------------------------
# Model definitions (kept compatible with checkpoint architecture)
# -----------------------------------------------------------------------------

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
      time_idx <- torch_arange(
        start = 0, end = as.integer(s - 1L),
        device = x$device, dtype = torch_float32()
      )
      dt <- time_idx$unsqueeze(2) - time_idx$unsqueeze(1)
      causal <- (dt >= 0)$to(dtype = ssm_in$dtype)
      dt <- dt$clamp(min = 0)$to(dtype = ssm_in$dtype)

      decay_grid <- torch_pow(decay$view(c(1, 1, -1)), dt$unsqueeze(-1))
      kernel <- decay_grid * causal$unsqueeze(-1)
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
    h <- self$embed(x)
    for (i in seq_len(length(self$blocks))) {
      h <- self$blocks[[i]](h)
    }
    h <- self$norm(h)
    h[, h$size(2), ]
  }
)

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
    self$mlp(x_rel)
  }
)

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
    B <- h_target$size(1)
    K <- h_neighbors$size(2)
    H <- self$n_heads
    Dh <- self$head_dim

    h_nb_cond <- h_neighbors + s_rel

    Q <- self$q_proj(h_target)$view(c(B, 1L, H, Dh))$permute(c(1, 3, 2, 4))
    Kt <- self$k_proj(h_nb_cond)$view(c(B, K, H, Dh))$permute(c(1, 3, 2, 4))
    V <- self$v_proj(h_nb_cond)$view(c(B, K, H, Dh))$permute(c(1, 3, 2, 4))

    attn <- torch_matmul(Q, Kt$transpose(-2, -1)) * self$scale
    dist_bias <- self$dist_bias_proj(s_rel)$permute(c(1, 3, 2))$unsqueeze(3)
    attn <- nnf_softmax(attn + dist_bias, dim = -1)
    attn <- self$dropout(attn)

    context <- torch_matmul(attn, V)
    context <- context$permute(c(1, 3, 2, 4))$reshape(c(B, -1))
    context <- self$out_proj(context)

    h <- self$norm1(h_target + self$dropout(context))
    h <- self$norm2(h + self$dropout(self$ffn(h)))
    h
  }
)

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
    B <- x_target$size(1)
    K <- x_neighbors$size(2)
    TT <- x_neighbors$size(3)

    h_target <- self$target_encoder(x_target)

    x_nb_flat <- x_neighbors$reshape(c(B * K, TT, -1))
    h_nb_flat <- self$neighbor_encoder(x_nb_flat)
    h_neighbors <- h_nb_flat$reshape(c(B, K, -1))

    s_rel <- self$spatial_rel_enc(x_rel)

    h <- h_target
    for (i in seq_len(length(self$cross_attn_layers))) {
      h <- self$cross_attn_layers[[i]](h, h_neighbors, s_rel)
    }

    h <- self$fusion(h, x_static)
    self$pred_head(h)
  }
)

# -----------------------------------------------------------------------------
# Checkpoint loading + optional tensor compilation (JIT trace)
# -----------------------------------------------------------------------------

load_mamba_st_predictor <- function(pt_file,
                                    device = NULL,
                                    use_jit_trace = TRUE,
                                    use_amp = TRUE,
                                    amp_dtype = "float16") {
  if (is.null(device)) {
    device <- if (cuda_is_available()) torch_device("cuda") else torch_device("cpu")
  }

  bundle <- torch_load(pt_file)
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
  model$eval()

  list(
    model = model,
    compiled_model = NULL,
    bundle = bundle,
    device = device,
    use_jit_trace = isTRUE(use_jit_trace),
    use_amp = isTRUE(use_amp),
    amp_dtype = amp_dtype,
    compiled_signature = NULL
  )
}

compile_predictor_once <- function(predictor, x_target, x_static, x_neighbors, x_rel) {
  if (!isTRUE(predictor$use_jit_trace)) return(predictor)
  if (!exists("jit_trace", where = asNamespace("torch"), inherits = FALSE)) return(predictor)
  if (!is.null(predictor$compiled_model)) return(predictor)

  traced <- tryCatch(
    {
      jit_trace(
        predictor$model,
        x_target,
        x_static,
        x_neighbors,
        x_rel,
        strict = FALSE
      )
    },
    error = function(e) {
      message("JIT trace skipped: ", conditionMessage(e))
      NULL
    }
  )

  if (!is.null(traced)) {
    predictor$compiled_model <- traced
    predictor$compiled_signature <- c(
      B = x_target$size(1),
      T = x_target$size(2),
      F_target = x_target$size(3),
      K = x_neighbors$size(2),
      F_neighbor = x_neighbors$size(4),
      F_static = x_static$size(2)
    )
    message("Predictor compiled with jit_trace().")
  }

  predictor
}

as_float_tensor <- function(x) {
  if (inherits(x, "torch_tensor")) {
    x$to(dtype = torch_float32())
  } else {
    torch_tensor(x, dtype = torch_float32())
  }
}

predict_mamba_st <- function(predictor,
                             x_target,
                             x_static,
                             x_neighbors,
                             x_rel,
                             batch_size = 1024L) {
  x_target <- as_float_tensor(x_target)
  x_static <- as_float_tensor(x_static)
  x_neighbors <- as_float_tensor(x_neighbors)
  x_rel <- as_float_tensor(x_rel)

  n <- x_target$size(1)
  if (x_static$size(1) != n || x_neighbors$size(1) != n || x_rel$size(1) != n) {
    stop("Batch dimension mismatch among inputs.")
  }

  device <- predictor$device
  use_cuda <- grepl("cuda", as.character(device), fixed = TRUE)
  use_amp_now <- isTRUE(predictor$use_amp) && use_cuda
  amp_dtype <- resolve_amp_dtype(predictor$amp_dtype)
  predictor_local <- predictor

  outs <- vector("list", ceiling(n / batch_size))
  j <- 1L

  with_no_grad({
    for (start in seq.int(1L, n, by = batch_size)) {
      end <- min(start + batch_size - 1L, n)
      idx <- start:end

      xb_t <- x_target[idx, , ]$to(device = device)
      xb_s <- x_static[idx, ]$to(device = device)
      xb_n <- x_neighbors[idx, , , ]$to(device = device)
      xb_r <- x_rel[idx, , ]$to(device = device)

      if (is.null(predictor_local$compiled_model)) {
        predictor_local <- compile_predictor_once(predictor_local, xb_t, xb_s, xb_n, xb_r)
      }

      use_compiled <- FALSE
      if (!is.null(predictor_local$compiled_model) && !is.null(predictor_local$compiled_signature)) {
        sig <- predictor_local$compiled_signature
        use_compiled <- (xb_t$size(1) == sig[["B"]] &&
          xb_t$size(2) == sig[["T"]] &&
          xb_t$size(3) == sig[["F_target"]] &&
          xb_n$size(2) == sig[["K"]] &&
          xb_n$size(4) == sig[["F_neighbor"]] &&
          xb_s$size(2) == sig[["F_static"]])
      }
      model_infer <- if (use_compiled) predictor_local$compiled_model else predictor_local$model

      pred <- if (use_amp_now) {
        with_autocast(
          {
            model_infer(xb_t, xb_s, xb_n, xb_r)
          },
          device_type = "cuda",
          dtype = amp_dtype,
          enabled = TRUE
        )
      } else {
        model_infer(xb_t, xb_s, xb_n, xb_r)
      }

      outs[[j]] <- as.matrix(pred$to(device = "cpu"))
      j <- j + 1L
    }
  })

  out <- do.call(rbind, outs)
  colnames(out) <- c("PM10_pred", "PM25_pred")
  out
}

# -----------------------------------------------------------------------------
# Minimal usage
# -----------------------------------------------------------------------------
# predictor <- load_mamba_st_predictor("tools/run_models/artifacts_st/mamba_transformer_st.pt")
# preds <- predict_mamba_st(
#   predictor,
#   x_target = array(runif(100 * 24 * 19), dim = c(100, 24, 19)),
#   x_static = matrix(runif(100 * 19), nrow = 100, ncol = 19),
#   x_neighbors = array(runif(100 * 5 * 24 * 21), dim = c(100, 5, 24, 21)),
#   x_rel = array(runif(100 * 5 * 6), dim = c(100, 5, 6)),
#   batch_size = 256L
# )
