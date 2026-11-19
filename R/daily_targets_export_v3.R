daily_2023_resources_v3 <- function(path) {
  x <- jsonlite::read_json(path, simplifyVector = TRUE)
  stopifnot(x$version == 3L, x$year == 2023L, x$prediction_workers == 20L,
    x$prediction_xgboost_threads == 2L, x$raster_workers == 40L, x$review_workers == 40L,
    isTRUE(x$phase_separation), identical(x$scientific_release, FALSE),
    daily_2023_sha(x$base_plan) == x$base_sha256,
    daily_2023_sha(x$reuse_registry) == x$reuse_sha256,
    daily_2023_sha(x$equivalence_result) == x$equivalence_sha256,
    jsonlite::read_json(x$equivalence_result)$status == "PASS")
  daily_2023_resources(x$base_plan)
  x
}

daily_2023_raster_parallel_v3 <- function(prediction_files, date, configuration, windows_file,
                                         implementation_files, resource_plan) {
  cfg <- daily_2023_read_config(configuration)
  plan <- daily_2023_resources_v3(resource_plan)
  daily_2023_assert_date(as.Date(date))
  paths <- unlist(prediction_files, use.names = FALSE)
  paths <- paths[basename(paths) == paste0(date, ".parquet")]
  stopifnot(length(paths) == 593L, !anyDuplicated(paths))
  directory <- daily_2023_attempt(cfg$root, file.path("rasters", date))
  index <- file.path(directory, "inputs.csv")
  utils::write.csv(data.frame(path = paths), index, row.names = FALSE)
  impl <- file.path(directory, "implementation.json")
  daily_2023_atomic_json(as.list(stats::setNames(vapply(implementation_files, daily_2023_sha, ""), implementation_files)), impl)
  resource <- file.path(directory, "resource_plan.json")
  daily_2023_atomic_json(list(version = 3L, plan = resource_plan, sha256 = daily_2023_sha(resource_plan),
    effective_raster_workers = 40L, effective_review_workers = 40L,
    accepted_prediction_modes = c("8x5", "20x2"), base_configuration = configuration,
    base_configuration_sha256 = daily_2023_sha(configuration), scientific_release = FALSE), resource)
  RhpcBLASctl::blas_set_num_threads(1L)
  stopifnot(RhpcBLASctl::blas_get_num_procs() == 1L)
  status <- system2("/members/dhnyu/.conda/envs/rgeo/bin/python", c("inst/python/daily_targets_raster_v3.py",
    shQuote(index), shQuote(windows_file), shQuote(date), shQuote(directory)),
    env = c("PYTHONDONTWRITEBYTECODE=1", "OPENBLAS_NUM_THREADS=1", "MKL_NUM_THREADS=1",
      "NUMEXPR_NUM_THREADS=1", "OMP_NUM_THREADS=1", "OMP_THREAD_LIMIT=1", "GDAL_NUM_THREADS=1"))
  stopifnot(status == 0L, RhpcBLASctl::blas_get_num_procs() == 1L,
    jsonlite::read_json(file.path(directory, "result.json"))$status == "PASS")
  c(index, impl, resource, file.path(directory, "result.json"),
    file.path(directory, "rasters", paste0(date, "_", rep(c("PM10", "PM25"), each = 2L), rep(c(".tif", ".json"), 2L))))
}

daily_2023_review_parallel_v3 <- function(raster_files, windows_file, implementation_files, reviewer_implementation) {
  result_file <- raster_files[basename(raster_files) == "result.json"]
  stopifnot(length(result_file) == 1L)
  source <- dirname(result_file); result <- jsonlite::read_json(result_file)
  recorded <- jsonlite::read_json(file.path(source, "implementation.json"), simplifyVector = TRUE)
  stopifnot(result$status == "PASS",
    identical(unname(unlist(recorded)), unname(vapply(implementation_files, daily_2023_sha, ""))))
  destination <- daily_2023_attempt("daehoon/data/daily_xgb_targets/year=2023/run=original_fixed300_001",
    file.path("reviews", result$date))
  code <- c(implementation_files, reviewer_implementation)
  daily_2023_atomic_json(as.list(stats::setNames(vapply(code, daily_2023_sha, ""), code)), file.path(destination, "implementation.json"))
  status <- system2("/members/dhnyu/.conda/envs/rgeo/bin/python", c("inst/python/daily_targets_review_v3.py",
    shQuote(source), shQuote(windows_file), shQuote(destination)),
    env = c("PYTHONDONTWRITEBYTECODE=1", "OPENBLAS_NUM_THREADS=1", "MKL_NUM_THREADS=1",
      "NUMEXPR_NUM_THREADS=1", "OMP_NUM_THREADS=1", "OMP_THREAD_LIMIT=1", "GDAL_NUM_THREADS=1"))
  if (status != 0L) stop("Versioned independent review failed: ", destination)
  file.path(destination, "review.json")
}
