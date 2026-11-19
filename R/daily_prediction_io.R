materialize_daily_smoke_contract <- function(contract, date, chunk_id, expected_cells) {
  if (!inherits(contract, "daily_grid_month_contract")) stop("Invalid daily grid contract.")
  if (!identical(contract$chunk_id, chunk_id)) stop("Wrong chunk identity in daily contract.")
  if (!inherits(date, "Date") || length(date) != 1L || is.na(date)) stop("Expected one smoke date.")
  daily_check_dates(data.frame(date = date), month = contract$month)
  if (!identical(contract$predictor_terms, daily_predictor_terms()) ||
      !identical(contract$schema_version, daily_native_schema_version())) stop("Daily contract schema mismatch.")
  manifest <- validate_grid_daily_cell_map(contract$cell_map_file, contract$sources)
  if (manifest$n_points != expected_cells || manifest$chunk_id != chunk_id) stop("Cell-map chunk count/identity mismatch.")
  if (expected_cells < 1L || expected_cells > 10000L) stop("Small smoke cell limit exceeded.")
  out <- materialize_grid_daily_contract(contract, dates = date)
  if (nrow(out) != expected_cells || any(out$date != date)) stop("Materialization row/date mismatch.")
  daily_assert_keys(out, c("gid", "date"))
  daily_assert_numeric(out, daily_predictor_terms())
  out$chunk_id <- chunk_id
  out
}

write_daily_smoke_summary <- function(result, output_root) {
  path <- file.path(output_root, "smoke_result.json")
  staging <- paste0(path, ".partial")
  if (!dir.exists(output_root) || any(file.exists(c(path, staging)))) stop("Summary output missing directory or already exists.")
  # Serialize before touching the destination so unsupported metadata classes
  # cannot leave a misleading partial success record.
  json <- jsonlite::toJSON(result, auto_unbox = TRUE, pretty = TRUE, digits = NA)
  writeLines(json, staging, useBytes = TRUE)
  if (!file.rename(staging, path)) stop("Could not publish smoke summary.")
  invisible(path)
}
