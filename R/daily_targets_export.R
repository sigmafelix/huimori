daily_2023_raster_date <- function(prediction_files, date, configuration, windows_file, implementation_files) {
  cfg <- daily_2023_read_config(configuration)
  daily_2023_assert_date(as.Date(date))
  paths <- unlist(prediction_files, use.names = FALSE)
  paths <- paths[basename(paths) == paste0(date, ".parquet")]
  if (length(paths) != 593L || anyDuplicated(paths)) stop("Expected 593 date partitions.")
  directory <- daily_2023_attempt(cfg$root, file.path("rasters", date))
  index <- file.path(directory, "inputs.csv")
  utils::write.csv(data.frame(path = paths), index, row.names = FALSE)
  daily_2023_atomic_json(as.list(stats::setNames(vapply(implementation_files, daily_2023_sha, ""),
    implementation_files)), file.path(directory, "implementation.json"))
  status <- system2("/members/dhnyu/.conda/envs/rgeo/bin/python", c(
    "inst/python/daily_targets_raster.py", shQuote(index), shQuote(windows_file),
    shQuote(date), shQuote(directory)), env = "PYTHONDONTWRITEBYTECODE=1")
  if (status != 0L) stop("Daily raster assembly failed; evidence retained in ", directory)
  result <- jsonlite::read_json(file.path(directory, "result.json"))
  if (result$status != "PASS") stop("Raster did not pass.")
  c(index, file.path(directory, "result.json"),
    file.path(directory, "rasters", paste0(date, "_", rep(c("PM10", "PM25"), each = 2L), rep(c(".tif", ".json"), 2L))))
}

daily_2023_review_date <- function(raster_files, windows_file, implementation_files) {
  result_file <- raster_files[basename(raster_files) == "result.json"]
  stopifnot(length(result_file) == 1L)
  directory <- dirname(result_file); result <- jsonlite::read_json(result_file)
  recorded <- jsonlite::read_json(file.path(directory, "implementation.json"), simplifyVector = TRUE)
  stopifnot(identical(unname(unlist(recorded)), unname(vapply(implementation_files, daily_2023_sha, ""))))
  status <- system2("/members/dhnyu/.conda/envs/rgeo/bin/python", c(
    "inst/python/daily_targets_raster.py", shQuote(file.path(directory, "inputs.csv")),
    shQuote(windows_file), shQuote(result$date), shQuote(directory), "--review"),
    env = "PYTHONDONTWRITEBYTECODE=1")
  if (status != 0L) stop("Independent date review failed.")
  file.path(directory, "review.json")
}

daily_2023_january_gate <- function(reviews, configuration) {
  cfg <- daily_2023_read_config(configuration)
  paths <- unlist(reviews, use.names = FALSE)
  if (length(paths) != 31L || anyDuplicated(paths)) stop("Expected 31 January reviews.")
  values <- lapply(paths, jsonlite::read_json)
  dates <- vapply(values, `[[`, "", "date")
  stopifnot(setequal(dates, as.character(daily_2023_dates("2023-01"))), !anyDuplicated(dates),
    all(vapply(values, function(x) x$status == "PASS" && isTRUE(x$january_exact) &&
      isTRUE(x$independent_review) && x$chunks == 593L && x$cells == 111637615, logical(1))))
  path <- file.path(daily_2023_attempt(cfg$root, "january_equivalence"), "result.json")
  daily_2023_atomic_json(list(status = "PASS", dates = dates, exact_predictions = TRUE,
    exact_raster_values = TRUE, geometry_mask_cog = TRUE, reviews = paths,
    scientific_release = FALSE), path)
}

daily_2023_all_rasters <- function(january, remaining) {
  paths <- unlist(c(january, remaining), use.names = FALSE)
  rasters <- paths[endsWith(paths, ".tif")]
  expected <- as.vector(outer(as.character(daily_2023_dates()), c("PM10", "PM25"),
    function(date, p) paste0(date, "_", p, ".tif")))
  stopifnot(length(rasters) == 730L, !anyDuplicated(basename(rasters)), setequal(basename(rasters), expected))
  rasters
}

daily_2023_final_validation <- function(rasters, january_reviews, remaining_reviews, configuration) {
  cfg <- daily_2023_read_config(configuration)
  paths <- unlist(c(january_reviews, remaining_reviews), use.names = FALSE)
  stopifnot(length(paths) == 365L, length(rasters) == 730L, !anyDuplicated(paths))
  result <- lapply(paths, jsonlite::read_json)
  stopifnot(setequal(vapply(result, `[[`, "", "date"), as.character(daily_2023_dates())),
    all(vapply(result, function(x) x$status == "PASS" && isTRUE(x$independent_review) &&
      x$chunks == 593L && x$cells == 111637615, logical(1))))
  daily_2023_atomic_json(list(status = "PASS", dates = 365L, rasters = 730L,
    chunks_per_date = 593L, cells_per_date = 111637615, schema = daily_model_schema(),
    label = cfg$label, estimand = cfg$estimand, release = cfg$release, scientific_release = FALSE,
    reviews = paths, raster_paths = rasters),
    file.path(daily_2023_attempt(cfg$root, "validation"), "result.json"))
}
