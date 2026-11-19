daily_2023_resources <- function(path) {
  x <- jsonlite::read_json(path, simplifyVector = TRUE)
  stopifnot(x$version == 2L, x$year == 2023L, x$fit_workers == 1L,
    x$fit_xgboost_threads == 40L, x$prediction_workers == 8L,
    x$prediction_xgboost_threads == 5L, x$raster_workers == 40L,
    x$review_workers == 40L, x$numerical_threads == 1L, x$gdal_threads == 1L,
    isTRUE(x$phase_separation), identical(x$scientific_release, FALSE))
  for (i in seq_len(nrow(x$benchmarks))) {
    p <- x$benchmarks$path[i]
    stopifnot(daily_2023_sha(p) == x$benchmarks$sha256[i],
      jsonlite::read_json(p)$status == "PASS")
  }
  x
}

daily_2023_raster_parallel <- function(prediction_files, date, configuration, windows_file,
                                        implementation_files, resource_plan) {
  effective <- daily_2023_resources(resource_plan)
  RhpcBLASctl::blas_set_num_threads(1L)
  stopifnot(RhpcBLASctl::blas_get_num_procs() == 1L)
  files <- withr::with_envvar(c(OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1",
    NUMEXPR_NUM_THREADS = "1", OMP_NUM_THREADS = "1", OMP_THREAD_LIMIT = "1", GDAL_NUM_THREADS = "1"),
    daily_2023_raster_date(prediction_files, date, configuration, windows_file, implementation_files))
  stopifnot(RhpcBLASctl::blas_get_num_procs() == 1L)
  directory <- dirname(files[basename(files) == "result.json"])
  record <- file.path(directory, "resource_plan.json")
  daily_2023_atomic_json(list(version = effective$version, plan = resource_plan,
    sha256 = daily_2023_sha(resource_plan), base_configuration = configuration,
    base_configuration_sha256 = daily_2023_sha(configuration),
    effective_raster_workers = effective$raster_workers, effective_review_workers = effective$review_workers,
    gdal_threads = 1L, blas_before_after = 1L, phase_separation = TRUE, scientific_release = FALSE), record)
  c(files, record, file.path(directory, "implementation.json"))
}
