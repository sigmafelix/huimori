daily_key_cols <- function() {
  c("TMSID", "TMSID2", "date")
}

daily_grid_key_cols <- function() {
  c("gid", "date")
}

assert_daily_unique_key <- function(x, key, label) {
  missing <- setdiff(key, names(x))
  if (length(missing) > 0L) {
    stop(label, " is missing key columns: ", paste(missing, collapse = ", "))
  }
  values <- if (inherits(x, "sf")) sf::st_drop_geometry(x) else x
  duplicate_idx <- anyDuplicated(values[, key, drop = FALSE])
  if (duplicate_idx > 0L) {
    stop(
      label, " has duplicated ", paste(key, collapse = "/"),
      " keys at row ", duplicate_idx, "."
    )
  }
  invisible(x)
}

summarize_daily_correct <- function(
  data,
  timeflag = "datehour",
  dateflag = "date",
  min_valid_hours = 18L
) {
  required_cols <- c("TMSID", timeflag, dateflag, "PM10", "PM25")
  missing_cols <- setdiff(required_cols, names(data))
  if (length(missing_cols) > 0L) {
    stop("summarize_daily_correct() is missing columns: ", paste(missing_cols, collapse = ", "))
  }
  min_valid_hours <- as.integer(min_valid_hours)
  if (length(min_valid_hours) != 1L || is.na(min_valid_hours) || min_valid_hours < 1L) {
    stop("min_valid_hours must be one positive integer.")
  }
  dt <- data.table::as.data.table(data.table::copy(data))
  dt[, TMSID := as.character(TMSID)]
  dt[, `.daily_timestamp` := get(timeflag)]
  dt[, date_s := as.Date(get(dateflag))]
  if (anyNA(dt$.daily_timestamp) || anyNA(dt$date_s)) {
    stop("Daily correct timestamps and KST dates must not be missing.")
  }
  dt[, PM10 := as.numeric(PM10)]
  dt[, PM25 := as.numeric(PM25)]
  dt[PM10 < 0, PM10 := NA_real_]
  dt[PM25 < 0, PM25 := NA_real_]
  mean_or_na <- function(x) if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE)
  hourly <- dt[, .(
    PM10 = mean_or_na(PM10),
    PM25 = mean_or_na(PM25),
    n_source_rows = .N
  ), by = .(TMSID, date_s, .daily_timestamp)]
  daily <- hourly[, .(
    n_hours = .N,
    n_valid_hours_PM10 = sum(!is.na(PM10)),
    n_valid_hours_PM25 = sum(!is.na(PM25)),
    n_duplicate_rows = sum(n_source_rows - 1L),
    PM10 = mean_or_na(PM10),
    PM25 = mean_or_na(PM25)
  ), by = .(TMSID, date_s)]
  daily[, PM10flag := as.integer(n_valid_hours_PM10 < min_valid_hours)]
  daily[, PM25flag := as.integer(n_valid_hours_PM25 < min_valid_hours)]
  daily[PM10flag == 1L, PM10 := NA_real_]
  daily[PM25flag == 1L, PM25 := NA_real_]
  data.table::setcolorder(
    daily,
    c(
      "TMSID", "date_s", "PM10flag", "PM25flag", "PM10", "PM25",
      "n_hours", "n_valid_hours_PM10", "n_valid_hours_PM25", "n_duplicate_rows"
    )
  )
  tibble::as_tibble(daily)
}

daily_month_dates <- function(month) {
  if (length(month) != 1L || is.na(month) || !grepl("^[0-9]{4}-[0-9]{2}$", month)) {
    stop("Expected one month formatted as YYYY-MM.")
  }
  start <- as.Date(paste0(month, "-01"))
  seq(start, seq(start, by = "month", length.out = 2L)[[2L]] - 1L, by = "day")
}

as_kst_date <- function(x) {
  if (inherits(x, "Date")) {
    return(as.Date(x))
  }
  as.Date(format(x, tz = "Asia/Seoul", format = "%Y-%m-%d"))
}

daily_key_frame <- function(x, id_cols = c("TMSID", "TMSID2")) {
  if (inherits(x, "sf")) {
    x <- sf::st_drop_geometry(x)
  }
  key_cols <- c(id_cols, "date")
  missing_cols <- setdiff(key_cols, names(x))
  if (length(missing_cols) > 0L) {
    stop("Daily key frame is missing columns: ", paste(missing_cols, collapse = ", "))
  }
  out <- x[, key_cols, drop = FALSE]
  out$date <- as.Date(out$date)
  out
}

assert_daily_branch <- function(
  x,
  month,
  label,
  required_cols = character(),
  id_cols = c("TMSID", "TMSID2")
) {
  key_cols <- c(id_cols, "date")
  required <- unique(c(key_cols, required_cols))
  missing_cols <- setdiff(required, names(x))
  if (length(missing_cols) > 0L) {
    stop(label, " is missing columns: ", paste(missing_cols, collapse = ", "))
  }
  keys <- daily_key_frame(x, id_cols)
  duplicate_idx <- anyDuplicated(keys)
  if (duplicate_idx > 0L) {
    stop(label, " has duplicated ", paste(key_cols, collapse = "/"), " keys at row ", duplicate_idx, ".")
  }
  bad_month <- format(keys$date, "%Y-%m") != month
  if (anyNA(keys$date) || any(bad_month)) {
    stop(label, " contains missing dates or dates outside branch month ", month, ".")
  }
  invisible(x)
}

assert_same_daily_keys <- function(
  x,
  reference,
  label,
  id_cols = c("TMSID", "TMSID2")
) {
  x_key <- daily_key_frame(x, id_cols)
  reference_key <- daily_key_frame(reference, id_cols)
  signature <- function(key) {
    values <- c(key[id_cols], list(format(key$date, "%Y-%m-%d")))
    sort(do.call(paste, c(values, sep = "\r")))
  }
  if (!identical(signature(x_key), signature(reference_key))) {
    stop(label, " key set does not match the base ", paste(c(id_cols, "date"), collapse = "/"), " key set.")
  }
  invisible(x)
}

prepare_daily_location_history <- function(site_history, invalid = c("drop", "error")) {
  invalid <- match.arg(invalid)
  required <- c(
    "TMSID", "TMSID2", "date_start", "date_end", "lon", "lat",
    "site_type", "coords_google"
  )
  missing_cols <- setdiff(required, names(site_history))
  if (length(missing_cols) > 0L) {
    stop("Daily location history is missing columns: ", paste(missing_cols, collapse = ", "))
  }
  history <- site_history |>
    sf::st_drop_geometry() |>
    dplyr::mutate(
      TMSID = as.character(TMSID),
      TMSID2 = as.character(TMSID2),
      date_start_kst = as_kst_date(date_start),
      date_end_kst = as_kst_date(date_end)
    ) |>
    dplyr::arrange(TMSID, date_start_kst, date_end_kst, TMSID2)

  if (anyNA(history[c("TMSID", "TMSID2", "date_start_kst", "date_end_kst", "lon", "lat")])) {
    stop("Daily location history contains missing key, interval, or coordinate values.")
  }
  if (anyDuplicated(history$TMSID2)) {
    stop("Daily location history TMSID2 values must be globally unique.")
  }
  invalid_rows <- which(history$date_start_kst > history$date_end_kst)
  if (length(invalid_rows) > 0L) {
    invalid_ids <- paste(history$TMSID2[invalid_rows], collapse = ", ")
    if (invalid == "error") {
      stop("Daily location history has reversed intervals: ", invalid_ids)
    }
    warning("Dropping reversed daily location intervals with no active dates: ", invalid_ids)
    history <- history[-invalid_rows, , drop = FALSE]
  }

  overlap <- history |>
    dplyr::group_by(TMSID) |>
    dplyr::mutate(previous_end = dplyr::lag(date_end_kst)) |>
    dplyr::ungroup() |>
    dplyr::filter(!is.na(previous_end), date_start_kst <= previous_end)
  if (nrow(overlap) > 0L) {
    stop(
      "Daily location history has overlapping intervals for TMSID(s): ",
      paste(unique(overlap$TMSID), collapse = ", ")
    )
  }
  attr(history, "dropped_invalid_intervals") <- length(invalid_rows)
  history
}

build_sf_monitors_correct_daily <- function(
  measurements,
  site_history,
  date_range = NULL,
  min_valid_hours = 18L
) {
  history <- prepare_daily_location_history(site_history, invalid = "drop")
  dropped_invalid_intervals <- attr(history, "dropped_invalid_intervals")
  required_measurement_cols <- c("TMSID", "date", "datehour", "PM10", "PM25")
  missing_cols <- setdiff(required_measurement_cols, names(measurements))
  if (length(missing_cols) > 0L) {
    stop("Daily measurements are missing columns: ", paste(missing_cols, collapse = ", "))
  }
  measurement_date <- as.Date(measurements[["date"]])
  if (is.null(date_range)) {
    date_range <- range(measurement_date, na.rm = TRUE)
  }
  date_range <- as.Date(date_range)
  if (length(date_range) != 2L || anyNA(date_range) || date_range[[1L]] > date_range[[2L]]) {
    stop("date_range must contain two ordered, non-missing dates.")
  }
  keep_measurements <- measurement_date >= date_range[[1L]] & measurement_date <= date_range[[2L]]
  measurement_dt <- data.table::as.data.table(
    data.table::copy(measurements[keep_measurements, ])
  )
  measurement_dt[, TMSID := as.character(TMSID)]
  measurement_dt[, `.date_kst` := as.Date(date)]

  daily_pm <- summarize_daily_correct(
    data = measurement_dt,
    timeflag = "datehour",
    dateflag = ".date_kst",
    min_valid_hours = min_valid_hours
  ) |>
    dplyr::rename(date = date_s)

  history <- history |>
    dplyr::mutate(
      clipped_start = pmax(date_start_kst, date_range[[1L]]),
      clipped_end = pmin(date_end_kst, date_range[[2L]])
    ) |>
    dplyr::filter(clipped_start <= clipped_end) |>
    dplyr::arrange(TMSID, clipped_start, TMSID2) |>
    dplyr::group_by(TMSID) |>
    dplyr::mutate(lon2 = dplyr::lag(lon), lat2 = dplyr::lag(lat)) |>
    dplyr::ungroup() |>
    dplyr::rowwise() |>
    dplyr::mutate(
      dist_m = if (is.na(lon2) || is.na(lat2)) {
        NA_real_
      } else {
        geosphere::distGeo(c(lon, lat), c(lon2, lat2))
      }
    ) |>
    dplyr::ungroup()

  daily_sites <- history |>
    dplyr::rowwise() |>
    dplyr::mutate(date = list(seq(clipped_start, clipped_end, by = "day"))) |>
    dplyr::ungroup() |>
    tidyr::unnest(date) |>
    dplyr::mutate(date = as.Date(date))
  assert_daily_unique_key(daily_sites, daily_key_cols(), "daily location skeleton")
  if (any(daily_sites$date < date_range[[1L]] | daily_sites$date > date_range[[2L]])) {
    stop("Daily location skeleton contains dates outside date_range.")
  }

  unmatched <- dplyr::anti_join(
    daily_pm |> dplyr::select(TMSID, date),
    daily_sites |> dplyr::select(TMSID, date),
    by = c("TMSID", "date")
  )
  if (nrow(unmatched) > 0L) {
    stop(
      "Daily measurements could not be assigned to an active KST location interval: ",
      nrow(unmatched), " TMSID/date rows."
    )
  }

  base_n <- nrow(daily_sites)
  out <- daily_sites |>
    dplyr::left_join(daily_pm, by = c("TMSID", "date"))
  if (nrow(out) != base_n) {
    stop("Daily monitor join changed row count: base=", base_n, ", joined=", nrow(out), ".")
  }
  out <- out |>
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326, remove = FALSE) |>
    sf::st_transform(5179) |>
    dplyr::mutate(year = lubridate::year(date)) |>
    dplyr::relocate(dplyr::any_of(c("date", "PM10", "PM25")), .after = TMSID2)
  assert_daily_unique_key(out, daily_key_cols(), "sf_monitors_correct_daily")
  attr(out, "dropped_invalid_intervals") <- dropped_invalid_intervals
  attr(out, "min_valid_hours") <- as.integer(min_valid_hours)
  out
}

subset_daily_monitor_month <- function(x, month) {
  out <- x |>
    dplyr::filter(format(date, "%Y-%m") == month)
  assert_daily_branch(out, month, "sf_monitors_correct_month")
  out
}
