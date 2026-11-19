new_static_audit_cluster <- function(workers, log_file) {
  if (length(workers) != 1L || !workers %in% c(2L, 40L)) stop("Expected 2 test workers or 40 audit workers.")
  vars <- c(OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1", VECLIB_MAXIMUM_THREADS = "1",
    NUMEXPR_NUM_THREADS = "1", GDAL_NUM_THREADS = "1", OMP_NUM_THREADS = "1",
    OMP_THREAD_LIMIT = "1", OMP_MAX_ACTIVE_LEVELS = "1", OMP_NESTED = "FALSE")
  old <- Sys.getenv(names(vars), unset = NA_character_)
  on.exit({
    Sys.unsetenv(names(old)[is.na(old)])
    do.call(Sys.setenv, as.list(old[!is.na(old)]))
  }, add = TRUE)
  do.call(Sys.setenv, as.list(vars))
  parallel::makePSOCKcluster(workers, outfile = log_file)
}

initialize_static_audit_worker <- function(project) {
  setwd(project)
  RhpcBLASctl::blas_set_num_threads(1L)
  foreach::registerDoSEQ()
  source("R/daily_correct.R")
  source("R/daily_native_cube.R")
  source("R/daily_xgb_contracts.R")
  source("R/daily_national_registry.R")
  assign("static_audit_catalog", read_national_target_catalog(), envir = .GlobalEnv)
  static_audit_worker_threads()
}

static_audit_worker_threads <- function() {
  state <- list(pid = Sys.getpid(), blas = RhpcBLASctl::blas_get_num_procs(),
    omp = Sys.getenv("OMP_NUM_THREADS"), omp_limit = Sys.getenv("OMP_THREAD_LIMIT"),
    gdal = Sys.getenv("GDAL_NUM_THREADS"),
    parallel_backend = foreach::getDoParRegistered() && !identical(foreach::getDoParName(), "doSEQ"))
  if (state$blas != 1L || state$omp != "1" || state$omp_limit != "1" ||
      state$gdal != "1" || state$parallel_backend || foreach::getDoParWorkers() != 1L) stop("Audit worker thread contract failed.")
  state
}

parallel_january_static_audit <- function(candidates, registry, expected, evidence_dir) {
  cl <- new_static_audit_cluster(40L, file.path(evidence_dir, "audit_workers.log"))
  on.exit(parallel::stopCluster(cl), add = TRUE)
  parallel::clusterExport(cl, c("initialize_static_audit_worker", "static_audit_worker_threads"),
                          envir = environment(parallel_january_static_audit))
  states <- parallel::clusterCall(cl, initialize_static_audit_worker, getwd())
  jsonlite::write_json(states, file.path(evidence_dir, "audit_worker_start.json"), auto_unbox = TRUE, pretty = TRUE)
  parallel::clusterExport(cl, c("registry", "expected"), envir = environment())
  results <- list()
  # Bounded waves stop scheduling further branches if any current wave fails.
  for (start in seq.int(1L, length(candidates), by = 40L)) {
    wave <- candidates[start:min(start + 39L, length(candidates))]
    rows <- parallel::parLapply(cl, wave, function(name) {
      before <- static_audit_worker_threads()
      begin <- proc.time()
      row <- audit_one_january_annual_branch(name, registry, static_audit_catalog,
                                            expected, daily_canonical_template())
      after <- static_audit_worker_threads()
      row$audit_worker_pid <- before$pid
      row$audit_blas_before <- before$blas
      row$audit_blas_after <- after$blas
      row$audit_omp_threads <- as.integer(after$omp)
      row$audit_elapsed_seconds <- unname((proc.time() - begin)["elapsed"])
      gc(FALSE)
      row
    })
    results <- c(results, rows)
    message("Parallel annual audit: ", length(results), "/", length(candidates), " (40 workers; inner threads=1)")
  }
  states <- parallel::clusterCall(cl, static_audit_worker_threads)
  jsonlite::write_json(states, file.path(evidence_dir, "audit_worker_end.json"), auto_unbox = TRUE, pretty = TRUE)
  results
}
