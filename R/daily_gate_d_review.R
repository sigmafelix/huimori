gate_d_hist_add <- function(histogram, values) {
  if (any(!is.finite(values))) stop("Nonfinite stored prediction.")
  bins <- floor(values * 100) + 1000001L
  if (any(bins < 1 | bins > length(histogram))) stop("Prediction outside bounded quantile index; review required.")
  lo <- min(bins); hi <- max(bins)
  histogram[lo:hi] <- histogram[lo:hi] + tabulate(bins - lo + 1L, hi - lo + 1L)
  histogram
}

gate_d_quantile_targets <- function(histogram) {
  probs <- c(.01, .5, .99); n <- sum(histogram); h <- 1 + (n - 1) * probs
  cumulative <- cumsum(histogram)
  ranks <- as.vector(rbind(floor(h), ceiling(h)))
  bins <- vapply(ranks, function(r) which(cumulative >= r)[1L], integer(1))
  before <- c(0, head(cumulative, -1))[bins]
  data.frame(p = rep(probs, each = 2), side = rep(c("lo", "hi"), 3), bin = bins,
             local_rank = ranks - before, weight = rep(h - floor(h), each = 2))
}

gate_d_frequency_add <- function(freq, values) {
  if (!length(values)) return(freq)
  u <- sort(unique(values)); counts <- as.double(tabulate(match(values, u), length(u)))
  if (is.null(freq)) return(list(value = u, n = counts))
  combined <- sort(unique(c(freq$value, u)))
  if (length(combined) > 2000000L) stop("Exact-quantile refinement exceeds bounded unique-value budget.")
  n <- numeric(length(combined)); n[match(freq$value, combined)] <- freq$n
  idx <- match(u, combined); n[idx] <- n[idx] + counts
  list(value = combined, n = n)
}

gate_d_refine_add <- function(frequencies, values, targets) {
  bins <- floor(values * 100) + 1000001L
  for (bin in unique(targets$bin)) {
    key <- as.character(bin)
    frequencies[[key]] <- gate_d_frequency_add(frequencies[[key]], values[bins == bin])
  }
  frequencies
}

gate_d_refined_quantiles <- function(frequencies, targets) {
  values <- vapply(seq_len(nrow(targets)), function(i) {
    f <- frequencies[[as.character(targets$bin[i])]]
    if (is.null(f) || sum(f$n) < targets$local_rank[i]) stop("Incomplete exact quantile refinement.")
    f$value[which(cumsum(f$n) >= targets$local_rank[i])[1L]]
  }, numeric(1))
  i <- c(1L,3L,5L); values[i] + targets$weight[i] * (values[i + 1L] - values[i])
}
