daily_static_predictor_terms <- function() {
  classes <- c(0, 10, 11, 20, 51, 52, 61, 62, 71, 72, 81, 82, 91,
               120, 130, 140, 150, 181, 182, 183, 186, 187, 190, 200, 210)
  c("dsm", "dem", "d_road", "mtpi", "mtpi_1km",
    unlist(lapply(c(100, 500, 2000, 5000), function(radius) {
      paste0("landuse_frac_", classes, "_", radius)
    }), use.names = FALSE))
}

daily_annual_predictor_terms <- function() {
  c(daily_static_predictor_terms(), paste0("aod_yearly_", c(100, 500, 2000, 5000)),
    paste0("blh_yearly_", c(100, 500, 2000, 5000)))
}

daily_model_predictor_terms <- function(annual_terms = daily_annual_predictor_terms()) {
  if (!identical(annual_terms, daily_annual_predictor_terms())) {
    stop("Unexpected ordered annual schema.")
  }
  dynamic <- c("aod", "blh", "t2m", "u10", "v10", "sp", "ssr", "tp")
  if (!identical(daily_predictor_terms(), dynamic)) stop("Unexpected daily dynamic schema.")
  terms <- c(daily_static_predictor_terms(), dynamic)
  stopifnot(length(terms) == 113L, !anyDuplicated(terms),
            !any(grepl("^(aod|blh)_yearly_|^PM(10|25)$", terms)))
  terms
}

daily_model_schema <- function() {
  list(version = "daily-xgb-2023-v1", terms = daily_model_predictor_terms(),
       hash = digest::digest(daily_model_predictor_terms(), algo = "sha256"))
}

daily_assert_columns <- function(x, columns) {
  if (!is.data.frame(x) || inherits(x, "sf")) stop("Expected a plain data frame.")
  if (anyDuplicated(names(x))) stop("Duplicate column names.")
  missing <- setdiff(columns, names(x))
  if (length(missing)) stop("Missing columns: ", paste(missing, collapse = ", "))
  invisible(x)
}

daily_assert_keys <- function(x, keys) {
  daily_assert_columns(x, keys)
  if (!nrow(x)) stop("Empty daily model input.")
  if (anyNA(x[keys]) || any(vapply(x[keys], function(v) {
    is.character(v) && any(!nzchar(trimws(v)))
  }, logical(1)))) stop("Missing key values.")
  if (anyDuplicated(x[keys])) stop("Duplicate key: ", paste(keys, collapse = "/"))
  invisible(x)
}

daily_check_dates <- function(x, year = 2023L, month = NULL) {
  daily_assert_columns(x, "date")
  if (!identical(as.integer(year), 2023L)) stop("Only pilot year 2023 is supported.")
  if (!inherits(x$date, "Date") || !length(x$date) || anyNA(x$date)) {
    stop("Expected nonmissing date values of class Date.")
  }
  derived <- as.integer(format(x$date, "%Y"))
  if (any(derived != year)) stop("Dates must be in 2023.")
  if ("year" %in% names(x) && (anyNA(x$year) || any(x$year != derived))) {
    stop("year/date inconsistency.")
  }
  if (!is.null(month)) {
    if (length(month) != 1L || !grepl("^2023-(0[1-9]|1[0-2])$", month) ||
        any(format(x$date, "%Y-%m") != month)) stop("Date outside expected month.")
  }
  x$year <- derived
  x
}

daily_assert_numeric <- function(x, terms) {
  daily_assert_columns(x, terms)
  if (!all(vapply(x[terms], is.numeric, logical(1)))) stop("Predictors must be numeric.")
  if (any(vapply(x[terms], function(v) any(is.infinite(v)), logical(1)))) {
    stop("Predictors contain infinite values; only finite values or native NA are permitted.")
  }
  if (any(vapply(x[terms], function(v) any(is.nan(v)), logical(1)))) {
    stop("NaN predictors must be represented explicitly as NA upstream.")
  }
  invisible(x)
}

daily_assert_no_forbidden <- function(x) {
  bad <- grepl("^(aod|blh)_yearly_|^PM(10|25)[_.]|^annual_PM", names(x))
  if (any(bad)) stop("Forbidden annual predictors/outcome collision: ", paste(names(x)[bad], collapse = ", "))
}

assert_daily_model_schema <- function(x, terms = daily_model_predictor_terms()) {
  if (!identical(terms, daily_model_predictor_terms())) stop("Invalid model schema.")
  daily_assert_columns(x, terms)
  daily_assert_no_forbidden(x)
  if (!identical(names(x)[names(x) %in% terms], terms)) stop("Predictor order differs from schema.")
  daily_assert_numeric(x, terms)
  invisible(x)
}

daily_match_rows <- function(daily, annual, keys) {
  daily_assert_keys(annual, keys)
  # Structured matching avoids delimiter collisions in station identifiers.
  idx <- vctrs::vec_match(as.data.frame(daily[keys]), as.data.frame(annual[keys]))
  if (anyNA(idx)) stop("Unmatched annual keys: ", sum(is.na(idx)))
  idx
}

join_correct_daily_annual_features <- function(daily, annual, month = NULL,
                                                terms = daily_model_predictor_terms()) {
  daily_assert_columns(daily, c("TMSID", "TMSID2", "date", "PM10", "PM25", daily_predictor_terms()))
  daily_assert_no_forbidden(daily)
  if (any(daily_static_predictor_terms() %in% names(daily))) stop("Static predictor collision on daily side.")
  daily <- daily_check_dates(daily, month = month)
  daily_assert_keys(daily, daily_key_cols())
  daily_assert_numeric(daily, c("PM10", "PM25", daily_predictor_terms()))
  daily_assert_numeric(annual, daily_static_predictor_terms())
  daily_assert_columns(annual, c("TMSID", "TMSID2", "year"))
  if (anyNA(annual$year) || any(annual$year != 2023L)) stop("Annual monitor year must be 2023.")
  idx <- daily_match_rows(daily, annual, c("TMSID", "TMSID2", "year"))
  metadata <- setdiff(names(daily), daily_predictor_terms())
  out <- cbind(as.data.frame(daily[metadata]),
               as.data.frame(annual[idx, daily_static_predictor_terms(), drop = FALSE]),
               as.data.frame(daily[daily_predictor_terms()]))
  rownames(out) <- NULL
  if (nrow(out) != nrow(daily) || !identical(out$PM10, daily$PM10) ||
      !identical(out$PM25, daily$PM25) || !identical(out$date, daily$date) ||
      !identical(out$TMSID, daily$TMSID) || !identical(out$TMSID2, daily$TMSID2)) {
    stop("Monitor join altered rows or daily outcomes.")
  }
  daily_assert_keys(out, daily_key_cols())
  assert_daily_model_schema(out, terms)
  out
}

daily_canonical_template <- function() {
  terra::rast(nrows = 20321L, ncols = 21390L, xmin = 746111, xmax = 1387811,
              ymin = 1458595, ymax = 2068225, crs = "EPSG:5179")
}

daily_template_id <- function(template) {
  crs <- sf::st_crs(terra::crs(template))
  digest::digest(list(dim = c(terra::nrow(template), terra::ncol(template)),
                      extent = as.vector(terra::ext(template)),
                      crs = if (!is.na(crs$epsg)) crs$epsg else crs$wkt), algo = "sha256")
}

daily_global_cell_id <- function(x, template) {
  daily_assert_numeric(x, c("x", "y"))
  if (anyNA(x[c("x", "y")])) stop("Missing grid coordinates.")
  xy <- as.matrix(x[c("x", "y")])
  cells <- terra::cellFromXY(template, xy)
  if (anyNA(cells)) stop("Grid coordinates outside reference extent.")
  centers <- terra::xyFromCell(template, cells)
  if (any(abs(xy - centers) > 1e-6)) stop("Grid alignment differs from canonical cell centers.")
  as.numeric(cells)
}

validate_daily_grid_chunk <- function(annual, chunk_id, grid_reference, template) {
  cols <- c("gid", "x", "y", "layer")
  daily_assert_keys(annual, c("gid", "year"))
  daily_assert_keys(grid_reference, "gid")
  daily_assert_columns(annual, cols)
  daily_assert_columns(grid_reference, cols)
  if (any(annual$year != 2023L)) stop("Annual grid year must be 2023.")
  if (length(chunk_id) != 1L || is.na(chunk_id) || !nzchar(chunk_id)) stop("Invalid chunk identity.")
  spatial <- sf::st_as_sf(grid_reference[cols], coords = c("x", "y"),
                          crs = terra::crs(template), remove = FALSE)
  if (!identical(daily_grid_chunk_key(spatial), chunk_id)) stop("Bad chunk identity.")
  if (nrow(annual) != nrow(grid_reference) || !setequal(annual$gid, grid_reference$gid)) {
    stop("Annual chunk does not match grid reference.")
  }
  idx <- match(grid_reference$gid, annual$gid)
  for (col in cols) {
    if (!isTRUE(all.equal(annual[[col]][idx], grid_reference[[col]], check.attributes = FALSE))) {
      stop("Annual chunk coordinates/metadata differ: ", col)
    }
  }
  cells <- daily_global_cell_id(grid_reference, template)
  if (anyDuplicated(cells)) stop("Duplicate global cell identity in chunk.")
  invisible(TRUE)
}

assert_daily_grid_identity <- function(x, template = NULL) {
  daily_assert_keys(x, c("chunk_id", "gid", "date"))
  daily_assert_keys(x, c("grid_template_id", "grid_cell_id", "date"))
  daily_check_dates(x)
  if (length(unique(x$grid_template_id)) != 1L) stop("Mixed reference geometry identities.")
  if (!is.numeric(x$grid_cell_id) || any(!is.finite(x$grid_cell_id)) ||
      any(x$grid_cell_id < 1 | x$grid_cell_id != floor(x$grid_cell_id))) stop("Invalid global cell identity.")
  if (!is.null(template)) {
    if (!identical(unique(x$grid_template_id), daily_template_id(template))) stop("Reference geometry identity differs.")
    if (!identical(as.numeric(x$grid_cell_id), daily_global_cell_id(x, template))) {
      stop("Global cell identity does not match coordinates.")
    }
  }
  invisible(x)
}

join_grid_daily_annual_features <- function(daily, annual, chunk_id, grid_reference, template,
                                           month = NULL, terms = daily_model_predictor_terms()) {
  daily_assert_columns(daily, c("gid", "date", daily_predictor_terms()))
  daily_assert_no_forbidden(daily)
  if (any(c(daily_static_predictor_terms(), "PM10", "PM25") %in% names(daily))) {
    stop("Predictor/outcome collision on daily grid side.")
  }
  validate_daily_grid_chunk(annual, chunk_id, grid_reference, template)
  daily <- daily_check_dates(daily, month = month)
  daily_assert_keys(daily, c("gid", "date"))
  if ("chunk_id" %in% names(daily) && anyNA(daily$chunk_id)) stop("Missing chunk identity.")
  if ("chunk_id" %in% names(daily) && any(daily$chunk_id != chunk_id)) stop("Bad chunk identity.")
  idx <- daily_match_rows(daily, annual, c("gid", "year"))
  for (col in intersect(c("x", "y", "layer"), names(daily))) {
    if (!isTRUE(all.equal(daily[[col]], annual[[col]][idx], check.attributes = FALSE))) {
      stop("Daily grid metadata differs: ", col)
    }
  }
  out <- data.frame(chunk_id = chunk_id, gid = daily$gid, date = daily$date,
                     year = daily$year, x = annual$x[idx], y = annual$y[idx], layer = annual$layer[idx])
  out$grid_cell_id <- daily_global_cell_id(out, template)
  out$grid_template_id <- daily_template_id(template)
  for (col in intersect(c("grid_cell_id", "grid_template_id"), names(daily))) {
    if (!identical(daily[[col]], out[[col]])) stop("Global grid identity changed.")
  }
  out <- cbind(out, as.data.frame(annual[idx, daily_static_predictor_terms(), drop = FALSE]),
               as.data.frame(daily[daily_predictor_terms()]))
  rownames(out) <- NULL
  if (nrow(out) != nrow(daily) || !identical(out$gid, daily$gid) ||
      !identical(out$date, daily$date)) stop("Grid join altered rows/order.")
  assert_daily_model_schema(out, terms)
  assert_daily_grid_identity(out, template)
  out
}
