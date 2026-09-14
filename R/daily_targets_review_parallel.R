# A barrier separates sequential COG writing from the measured review pool.
daily_2023_review_catalog <- function(raster_files, january = FALSE) {
  paths <- unlist(raster_files, use.names = FALSE)
  results <- paths[basename(paths) == "result.json"]
  dates <- vapply(results, function(p) jsonlite::read_json(p)$date, "")
  expected <- if (january) as.character(daily_2023_dates("2023-01")) else as.character(daily_2023_dates())[32:365]
  stopifnot(length(results) == length(expected), !anyDuplicated(dates), setequal(dates, expected))
  lapply(results[order(dates)], function(p) paths[dirname(paths) == dirname(p) |
    dirname(dirname(paths)) == dirname(p)])
}

daily_2023_review_parallel <- function(raster_files, windows_file, implementation_files,
                                       reviewer_implementation) {
  result_file <- raster_files[basename(raster_files) == "result.json"]
  stopifnot(length(result_file) == 1L)
  source <- dirname(result_file)
  result <- jsonlite::read_json(result_file)
  stopifnot(result$status == "PASS")
  recorded <- jsonlite::read_json(file.path(source, "implementation.json"), simplifyVector = TRUE)
  stopifnot(identical(unname(unlist(recorded)), unname(vapply(implementation_files, daily_2023_sha, ""))))
  root <- "daehoon/data/daily_xgb_targets/year=2023/run=original_fixed300_001"
  destination <- daily_2023_attempt(root, file.path("reviews", result$date))
  pins <- as.list(stats::setNames(vapply(c(implementation_files, reviewer_implementation),
    daily_2023_sha, ""), c(implementation_files, reviewer_implementation)))
  daily_2023_atomic_json(pins, file.path(destination, "implementation.json"))
  status <- system2("/members/dhnyu/.conda/envs/rgeo/bin/python", c(
    "inst/python/daily_targets_review.py", shQuote(source), shQuote(windows_file), shQuote(destination)),
    env = c("PYTHONDONTWRITEBYTECODE=1", "OPENBLAS_NUM_THREADS=1", "MKL_NUM_THREADS=1",
      "NUMEXPR_NUM_THREADS=1", "OMP_NUM_THREADS=1", "OMP_THREAD_LIMIT=1", "GDAL_NUM_THREADS=1"))
  if (status != 0L) stop("Parallel independent review failed: ", destination)
  file.path(destination, "review.json")
}
