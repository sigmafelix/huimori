# Formal downstream targets helpers. Accepted annual and pilot helpers are immutable.
daily_2023_dates <- function(month = NULL) {
  dates <- seq(as.Date("2023-01-01"), as.Date("2023-12-31"), by = "day")
  if (!is.null(month)) {
    if (length(month) != 1L || !month %in% sprintf("2023-%02d", 1:12)) stop("2023 month required.")
    dates <- dates[format(dates, "%Y-%m") == month]
  }
  dates
}

daily_2023_assert_date <- function(date, month = NULL) {
  if (!inherits(date, "Date") || length(date) != 1L || is.na(date) ||
      !date %in% daily_2023_dates(month)) stop("Date outside declared 2023 month.")
  invisible(TRUE)
}

daily_2023_sha <- function(path) digest::digest(file = path, algo = "sha256")

daily_2023_disk_guard <- function(path, minimum_gib = 100) {
  lines <- system2("df", c("-Pk", shQuote(path)), stdout = TRUE)
  fields <- strsplit(trimws(tail(lines, 1L)), "[[:space:]]+")[[1L]]
  free <- as.numeric(fields[4L]) * 1024
  if (!is.finite(free) || free < minimum_gib * 1024^3) stop("Insufficient daily-output disk headroom.")
  free
}

daily_2023_atomic_json <- function(value, path) {
  if (any(file.exists(c(path, paste0(path, ".partial"))))) stop("Exclusive output exists: ", path)
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(value, paste0(path, ".partial"), auto_unbox = TRUE, pretty = TRUE, digits = 17)
  if (!file.rename(paste0(path, ".partial"), path)) stop("Atomic JSON installation failed.")
  path
}

daily_2023_attempt <- function(root, component) {
  if (!grepl("^daehoon/data/daily_xgb_targets/year=2023/run=[A-Za-z0-9_-]+$", root)) stop("Invalid isolated root.")
  parent <- file.path(root, component)
  dir.create(parent, recursive = TRUE, showWarnings = FALSE)
  path <- tempfile("attempt_", tmpdir = parent)
  if (!dir.create(path)) stop("Exclusive attempt creation failed.")
  path
}

daily_2023_config <- function(root, authority_files) {
  if (file.exists(root)) stop("New exclusive target run root required.")
  stopifnot(basename(getwd()) == "huimori",
    identical(system2("git", c("branch", "--show-current"), stdout = TRUE), "ver2"))
  require_gate_b_precondition(gate_b_root())
  daily_2023_disk_guard("daehoon/data", minimum_gib = 2000)
  for (path in authority_files[basename(authority_files) == "result.json"]) {
    if (jsonlite::read_json(path)$status != "PASS") stop("Accepted prerequisite failed: ", path)
  }
  baseline <- "daehoon/data/daily_xgb_2023_baseline/run=original_fixed300_001"
  refs <- file.path(baseline, paste0(c("PM10", "PM25"),
    "_ORIGINAL_DAILY_2023_BASELINE_RETROSPECTIVE_NONPRODUCTION.rds"))
  stopifnot(identical(unname(vapply(refs, daily_2023_sha, "")), c(
    "5c9a76d808c8439a09ea04545fb554e9f9b3bf1ac8569e2c0c911e89f78a3dd3",
    "ade7123b15e21fb4586c34e531f00939e841603e0bbb413eceec073026180abc")))
  config <- list(year = 2023L, root = root, schema = daily_model_schema(),
    label = "ORIGINAL_DAILY_2023_BASELINE", estimand = "RETROSPECTIVE",
    release = "NONPRODUCTION", scientific_release = FALSE,
    models_reference = refs, fixed = daily_monitor_validation_config(),
    prediction_workers = 8L, prediction_threads = 5L, raster_workers = 1L,
    authority = data.frame(path = authority_files, sha256 = vapply(authority_files, daily_2023_sha, "")))
  dir.create(root, recursive = TRUE)
  daily_2023_atomic_json(config, file.path(root, "config.json"))
}

daily_2023_read_config <- function(path) {
  x <- jsonlite::read_json(path, simplifyVector = TRUE)
  if (x$year != 2023L || x$scientific_release || x$schema$hash != daily_model_schema()$hash) stop("Configuration drift.")
  if (!identical(unname(vapply(x$authority$path, daily_2023_sha, "")), x$authority$sha256)) stop("Authority changed.")
  x
}

daily_2023_frame_list <- function(x) {
  if (is.data.frame(x)) return(list(as.data.frame(x)))
  if (!is.list(x)) stop("Feature branch is not a data frame/list.")
  unlist(lapply(x, daily_2023_frame_list), recursive = FALSE)
}

daily_2023_monitor_month <- function(data) {
  daily_check_dates(data)
  months <- unique(format(data$date, "%Y-%m"))
  if (length(months) != 1L || !months %in% sprintf("2023-%02d", 1:12) ||
      !setequal(data$date, daily_2023_dates(months))) stop("Invalid 2023 monitor month.")
  data
}

daily_2023_training <- function(annual, daily) {
  a <- daily_2023_frame_list(annual)
  if (length(a) != 1L || !identical(unique(as.integer(a[[1]]$year)), 2023L)) stop("Expected one annual 2023 monitor branch.")
  d <- daily_2023_frame_list(daily)
  if (length(d) != 12L) stop("Expected twelve daily monitor branches.")
  d <- d[order(vapply(d, function(x) as.character(min(x$date)), ""))]
  d <- do.call(rbind, d); rownames(d) <- NULL
  out <- join_correct_daily_annual_features(d, a[[1]])
  stopifnot(nrow(out) == 239052L, setequal(out$date, daily_2023_dates()),
    sum(is.finite(out$PM10)) == 225024L, sum(is.finite(out$PM25)) == 223472L)
  assert_daily_model_schema(out)
  out
}

daily_2023_spatial_fit <- function(data, configuration, reference_files, fold_file) {
  cfg <- daily_2023_read_config(configuration); setup_daily_monitor_threads()
  directory <- daily_2023_attempt(cfg$root, "spatial_oof")
  folds <- original_baseline_folds(data, fold_file)
  outputs <- character()
  for (p in c("PM10", "PM25")) {
    reference <- readRDS(reference_files[match(p, c("PM10", "PM25"))])
    engine <- xgboost::xgb.config(workflows::extract_fit_engine(reference$workflow))
    valid <- which(is.finite(data[[p]]))
    mold <- workflows::extract_mold(reference$workflow)
    original_baseline_equal(as.data.frame(mold$predictors), data[valid, ], daily_model_predictor_terms())
    stopifnot(identical(mold$outcomes[[p]], data[[p]][valid]))
    for (fold in paste0("Fold", 1:5)) {
      train <- which(is.finite(data[[p]]) & folds$rows$fold != fold)
      test <- which(is.finite(data[[p]]) & folds$rows$fold == fold)
      stopifnot(!length(intersect(data$TMSID[train], data$TMSID[test])))
      fit <- fit_original_daily_baseline(data[train, ], p, fold, engine)
      prediction <- predict(fit$workflow, new_data = data[test, daily_model_predictor_terms()])$.pred
      assert_daily_monitor_threads()
      old <- as.data.frame(nanoparquet::read_parquet(file.path(original_baseline_root(),
        paste0("oof_", p, "_", fold, ".parquet"))))
      stopifnot(identical(test, old$.row_id), identical(prediction, old$predicted), all(is.finite(prediction)))
      path <- file.path(directory, paste0(p, "_", fold, ".parquet"))
      old$predicted <- prediction
      nanoparquet::write_parquet(old, paste0(path, ".partial"), compression = "zstd")
      stopifnot(file.rename(paste0(path, ".partial"), path))
      outputs <- c(outputs, path)
      rm(fit); gc()
    }
  }
  c(outputs, daily_2023_atomic_json(list(status = "PASS", exact_m0_oof = TRUE,
    folds = 5L, outcomes = 2L, no_tuning = TRUE, no_early_stopping = TRUE,
    schema = daily_model_schema()), file.path(directory, "result.json")))
}

daily_2023_full_fit <- function(data, spatial_files, configuration, reference_files) {
  stopifnot(length(spatial_files) == 11L,
    jsonlite::read_json(spatial_files[endsWith(spatial_files, "result.json")])$status == "PASS")
  cfg <- daily_2023_read_config(configuration); setup_daily_monitor_threads()
  directory <- daily_2023_attempt(cfg$root, "models")
  paths <- character()
  for (p in c("PM10", "PM25")) {
    old <- readRDS(reference_files[match(p, c("PM10", "PM25"))])
    reference <- workflows::extract_fit_engine(old$workflow)
    fit <- fit_original_daily_baseline(data[is.finite(data[[p]]), ], p, "FULL_2023", xgboost::xgb.config(reference))
    stopifnot(identical(xgboost::xgb.dump(workflows::extract_fit_engine(fit$workflow)), xgboost::xgb.dump(reference)))
    fit$metadata$scientific_release <- FALSE
    path <- file.path(directory, paste0(p, "_ORIGINAL_DAILY_2023_BASELINE.rds"))
    saveRDS(fit, paste0(path, ".partial")); stopifnot(file.rename(paste0(path, ".partial"), path))
    paths <- c(paths, path)
    rm(fit, old, reference); gc()
  }
  paths
}
