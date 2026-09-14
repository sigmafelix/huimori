normalize_daily_model_missing_values <- function(data, chunk_id, annual_branch,
                                                 terms = daily_static_predictor_terms()) {
  if (!identical(class(data), "data.frame")) stop("Expected a temporary plain data.frame.")
  if (!identical(terms, daily_static_predictor_terms()) || anyDuplicated(terms)) stop("Static schema mismatch.")
  daily_assert_columns(data, c("gid", "x", "y", "layer", "year", terms))
  metadata <- setdiff(names(data), terms)
  allowed <- c("gid", "x", "y", "layer", "year", "chunk_id", "grid_cell_id", "grid_template_id", "date")
  if (any(!metadata %in% allowed) || !identical(names(data)[names(data) %in% terms], terms)) stop("Static schema/metadata mismatch.")
  if (length(chunk_id) != 1L || is.na(chunk_id) || !nzchar(chunk_id) ||
      length(annual_branch) != 1L || is.na(annual_branch) ||
      !grepl("^df_feat_grid_merged_[a-z0-9]+$", annual_branch)) stop("Missing/invalid branch or chunk identity.")
  if ("chunk_id" %in% metadata && (anyNA(data$chunk_id) || any(data$chunk_id != chunk_id))) stop("Chunk identity mismatch.")
  daily_assert_keys(data, c("gid", "year"))
  if (any(data$year != 2023L)) stop("Static import requires year 2023.")
  if (!all(vapply(data[terms], is.numeric, logical(1)))) stop("Static predictors must be numeric.")
  counts <- vapply(data[terms], function(v) c(na = sum(is.na(v) & !is.nan(v)),
    nan = sum(is.nan(v)), inf = sum(v == Inf, na.rm = TRUE), neginf = sum(v == -Inf, na.rm = TRUE)), numeric(4))
  if (sum(counts[c("inf", "neginf"), ]) != 0) stop("Static predictors contain infinite values; normalization aborted.")
  # Plain R data frames/vectors have copy-on-modify semantics, not reference updates.
  out <- data
  affected <- rep(FALSE, nrow(data))
  for (term in terms[counts["nan", ] > 0]) {
    mask <- is.nan(data[[term]])
    affected <- affected | mask
    out[[term]][mask] <- NA_real_
    if (!identical(out[[term]][!mask], data[[term]][!mask], num.eq = FALSE) ||
        !identical(is.na(out[[term]]), is.na(data[[term]]))) stop("Normalization changed non-NaN values.")
  }
  unchanged <- c(metadata, terms[counts["nan", ] == 0])
  if (!identical(out[unchanged], data[unchanged], num.eq = FALSE) ||
      !identical(attributes(out), attributes(data))) stop("Normalization changed structure/metadata.")
  daily_assert_numeric(out, terms)
  schema <- daily_model_schema()
  audit <- list(chunk_id = chunk_id, annual_branch = annual_branch, n_rows = nrow(data),
    nan_converted = sum(counts["nan", ]), affected_rows = sum(affected),
    affected_predictors = sum(counts["nan", ] > 0), per_predictor = counts["nan", ],
    raw_na = sum(counts["na", ]), raw_nan = sum(counts["nan", ]),
    positive_inf = sum(counts["inf", ]), negative_inf = sum(counts["neginf", ]),
    normalized_na = sum(is.na(out[terms])), normalized_nan = 0L,
    finite_values_unchanged = TRUE, metadata_unchanged = TRUE,
    schema_version = schema$version, schema_hash = schema$hash)
  if (audit$normalized_na != audit$raw_na + audit$nan_converted) stop("Missing-value accounting mismatch.")
  list(data = out, audit = audit)
}

static_normalization_audit_row <- function(audit) {
  scalar <- audit[setdiff(names(audit), "per_predictor")]
  cbind(as.data.frame(scalar, stringsAsFactors = FALSE),
        as.data.frame(as.list(stats::setNames(audit$per_predictor,
          paste0("nan_", names(audit$per_predictor))))))
}

assert_static_normalization_reconciliation <- function(records, expected) {
  daily_assert_keys(records, "chunk_id")
  daily_assert_keys(records, "annual_branch")
  daily_assert_keys(expected, "chunk_id")
  idx <- match(expected$chunk_id, records$chunk_id)
  if (nrow(records) != 593L || nrow(expected) != 593L || anyNA(idx)) stop("Expected exactly 593 normalization records.")
  x <- records[idx, ]
  if (!identical(x$annual_branch, expected$annual_branch) || any(x$n_rows != expected$n_rows) ||
      any(x$nan_converted != expected$n_static_nan) || any(x$affected_rows != expected$rows_nan) ||
      any(x$raw_na != expected$n_static_na)) stop("Normalization provenance/count drift.")
  totals <- c(branches = sum(x$nan_converted > 0), rows = sum(x$affected_rows),
    conversions = sum(x$nan_converted), cells = sum(x$n_rows))
  if (!identical(unname(as.numeric(totals)), c(4, 605, 1586, 111637615)) ||
      any(x$positive_inf != 0 | x$negative_inf != 0 | x$raw_na != 0 | x$normalized_nan != 0) ||
      any(x$normalized_na != x$nan_converted) ||
      any(!x$finite_values_unchanged | !x$metadata_unchanged) ||
      any(x$schema_hash != daily_model_schema()$hash | x$schema_version != daily_model_schema()$version)) {
    stop("Normalization acceptance accounting differs from authorized provenance.")
  }
  terms <- daily_static_predictor_terms()
  for (i in seq_len(nrow(x))) {
    expected_counts <- stats::setNames(rep(0, length(terms)), terms)
    nonzero <- unlist(jsonlite::fromJSON(expected$nan_by_predictor[i]))
    if (length(nonzero)) expected_counts[names(nonzero)] <- nonzero
    if (!identical(as.numeric(x[i, paste0("nan_", terms)]), as.numeric(expected_counts))) stop("Per-predictor normalization drift.")
  }
  list(status = "PASS", totals = as.list(totals),
    per_predictor = as.list(stats::setNames(colSums(x[paste0("nan_", terms)]), terms)),
    raw_na = sum(x$raw_na), normalized_na = sum(x$normalized_na), normalized_nan = 0L,
    positive_inf = 0L, negative_inf = 0L, schema = daily_model_schema())
}
