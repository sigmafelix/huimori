
#' Rectify year based on a date field
#'
#' This function takes a data frame and a date field name, and rectifies the year based on the month of the date.
#' If the month is less than 6, it keeps the year as is; otherwise, it adds 1 to the year.
#' @param data A data frame containing the date field.
#' @param fieldname The name of the date field in the data frame.
#' @return A data frame with two new columns: `year_end` and `month_end`, and a rectified year column `year_rect`.
#' @export
rectify_year <-
  function(data, fieldname) {
    data |>
      dplyr::mutate(
        year_end = lubridate::year(!!rlang::sym(fieldname)),
        month_end = lubridate::month(!!rlang::sym(fieldname))
      ) |>
      dplyr::mutate(
        year_rect = ifelse(month_end < 6, year_end, year_end + 1)
      )
  }


#' Fill start or end date from data context
#' @param date Unquoted field name (e.g., field name specification in dplyr)
#' @param year numeric(1). Year to fill in.
#' @param start logical(1). Is this the start date?
#' @return Vector filled with start or end date depending on the context.
#' @note tz is fixed as "Asia/Seoul".
#' @export
fill_date <- function(date, year, start = TRUE) {
  template <- ifelse(start, "%d0101010000", "%d1231230000")

  ifelse(
    is.na(date),
    as.POSIXct(
      sprintf(template, year),
      format = "%Y%m%d%H%M%S",
      tz = "Asia/Seoul"
    ),
    date
  )
}

#' Summarize hourly data into daily
#'
#' @param data data frame
#' @param timeflag character
#' @return data frame
#' @details The function groups the data by TMSID2, TMSID, and date, and calculates the mean PM10 and PM25 values.
#' It also flags days with insufficient data or excessive missing values.
#' @export
#' @importFrom lubridate date
#' @importFrom dplyr mutate group_by summarize n ungroup
#' @importFrom rlang sym
summarize_daily <-
  function(data, timeflag = "datehour") {
    data_daily <-
      data |>
      dplyr::mutate(
        date_s = lubridate::date(!!rlang::sym(timeflag)),
        PM10 = ifelse(PM10 < 0, NA, PM10),
        PM25 = ifelse(PM25 < 0, NA, PM25)
      ) |>
      dplyr::group_by(TMSID2, TMSID, date_s) |>
      dplyr::summarize(
        PM10flag = ifelse(dplyr::n() < 18 | sum(is.na(PM10)) > 6, 1, 0),
        PM25flag = ifelse(dplyr::n() < 18 | sum(is.na(PM25)) > 6, 1, 0),
        PM10 = mean(PM10, na.rm = TRUE),
        PM25 = mean(PM25, na.rm = TRUE)
      ) |>
      dplyr::ungroup()
    return(data_daily)
  }

#' Summarize annual data
#' @param data data frame
#' @param timeflag character. Name of the time field, default is "date_s".
#' @return data frame with annual summary
#' @details The function groups the data by TMSID2, TMSID, and year, and calculates the mean PM10 and PM25 values,
#'  along with the number of days with data (ndays). It also replaces negative PM10 and PM25 values with NA.
#' @export
#' @importFrom lubridate year
#' @importFrom dplyr mutate group_by summarize n ungroup
#' @importFrom rlang sym
summarize_annual <-
  function(data, timeflag = "date_s") {
    # Halt with error if temporal granularity is not daily
    dd <- try(lubridate::day(data[[timeflag]]), silent = TRUE)
    if (inherits(dd, "try-error")) {
      stop("Temporal granularity is not daily")
    }
    data_annual <-
      data |>
      dplyr::mutate(
        year = lubridate::year(!!rlang::sym(timeflag)),
        PM10 = ifelse(PM10 < 0, NA, PM10),
        PM25 = ifelse(PM25 < 0, NA, PM25)
      ) |>
      dplyr::group_by(TMSID2, TMSID, year) |>
      dplyr::summarize(
        ndays = dplyr::n(),
        PM10 = mean(PM10, na.rm = TRUE),
        PM25 = mean(PM25, na.rm = TRUE)
      ) |>
      dplyr::ungroup()
    return(data_annual)
  }



#' Auto-generate a grid template with exactly
#' splitted in full numbers
#' @param x sf/terra object representing the polygon to be gridded
#' @param grid_size numeric(1). Size of the grid cells in meters.
#' @param chunks integer(1). Number of chunks to split the grid into.
#' @return A SpatVector object with exactly split grids
#' @export
#' @importFrom terra ext
#' @importFrom chopin par_pad_grid
auto_grid <-
  function(
    x,
    grid_size = 100,
    chunks = 30L
  ) {
    if (!inherits(x, "SpatVector")) {
      if (inherits(x, "sf")) {
        x <- terra::vect(x)
      } else {
        stop("Input x must be a SpatVector or sf object.")
      }
    }
    xext <- floor(terra::ext(x))
    chunksize <- grid_size * chunks

    rem_x <- chunksize - ((xext[2] - xext[1]) %% chunksize)
    rem_y <- chunksize - ((xext[4] - xext[3]) %% chunksize)
    rem_x_left <- ifelse(rem_x == 0, 0, -(grid_size * 10))
    rem_y_bottom <- ifelse(rem_y == 0, 0, -(grid_size * 10))
    rem_x_right <- ifelse(rem_x == 0, 0, rem_x + rem_x_left)
    rem_y_top <- ifelse(rem_y == 0, 0, rem_y + rem_y_bottom)
    xext <- c(
      xext[1] + rem_x_left,
      xext[2] + rem_x_right,
      xext[3] + rem_y_bottom,
      xext[4] + rem_y_top
    )
    xext <- terra::ext(xext)
    xext <- terra::vect(xext, crs = terra::crs(x))
    pad_res <-
      chopin::par_pad_grid(
        input = xext,
        mode = "grid",
        nx = chunks,
        ny = chunks,
        padding = 0
      )$original
    pad_res <- pad_res[x, ]
    return(pad_res)
  }


#' Initialize Earth Engine once per worker
#' @param email optional email used for rgee auth; defaults to GEE_EMAIL env var when set
#' @param drive logical; enable Drive export
#' @param gcs logical; enable GCS export
#' @return TRUE invisibly when initialization completes
#' @export
gee_init <- function(email = Sys.getenv("GEE_EMAIL", unset = NULL), drive = TRUE, gcs = FALSE) {
  rgee::ee_Initialize(
    email = if (is.null(email) || identical(email, "")) NULL else email,
    drive = drive,
    gcs = gcs,
    quiet = TRUE
  )
  invisible(TRUE)
}


#' Convert sf points to EE FeatureCollection with optional buffer
#' @noRd
gee_prepare_points <- function(points_sf, buffer_m = 0) {
  pts <- sf::st_transform(points_sf, 4326)
  pts_fc <- rgee::sf_as_ee(pts)
  if (buffer_m > 0) {
    pts_fc <- pts_fc$map(
      rgee::ee_utils_pyfunc(function(f) f$buffer(buffer_m))
    )
  }
  pts_fc
}


#' Daily wind (10 m) time series from ERA5-Land at point locations
#' @param points_sf sf POINT object with properties to keep (e.g., TMSID, TMSID2)
#' @param start_date,end_date Date or character range
#' @param buffer_m numeric; buffer radius in meters before sampling
#' @param scale numeric; sampling scale in meters
#' @param via character; extraction backend for rgee::ee_extract_timeseries
#' @param container optional Drive/GCS folder when via != "getInfo"
#' @return tibble with point identifiers, date, wind_speed_10m, wind_dir_deg
#' @export
gee_extract_daily_wind <- function(
  points_sf,
  start_date,
  end_date,
  buffer_m = 250,
  scale = 1000,
  via = Sys.getenv("EE_VIA", unset = "getInfo"),
  container = Sys.getenv("EE_DRIVE_FOLDER", unset = NULL)
) {
  gee_init()
  ee <- rgee::ee

  pts_fc <- gee_prepare_points(points_sf, buffer_m = buffer_m)

  wind_ic <-
    ee$ImageCollection("ECMWF/ERA5_LAND/HOURLY")$
    filterDate(as.character(start_date), as.character(end_date))$
    select(c("u_component_of_wind_10m", "v_component_of_wind_10m"))$
    map(
      rgee::ee_utils_pyfunc(function(img) {
        u <- img$select("u_component_of_wind_10m")
        v <- img$select("v_component_of_wind_10m")
        speed <- u$hypot(v)$rename("wind_speed_10m")
        direction <- u$atan2(v)$multiply(180 / pi)$add(180)$rename("wind_dir_deg")
        img$addBands(speed)$addBands(direction)$
          select(c("wind_speed_10m", "wind_dir_deg"))$
          copyProperties(img, list("system:time_start"))
      })
    )

  ts_sf <- rgee::ee_extract_timeseries(
    x = wind_ic,
    y = pts_fc,
    scale = scale,
    reducer = ee$Reducer$mean(),
    via = via,
    container = container,
    sf = TRUE
  )

  ts_tbl <- ts_sf |>
    sf::st_drop_geometry() |>
    tibble::as_tibble()

  point_cols <- intersect(names(points_sf), names(ts_tbl))

  ts_tbl |>
    dplyr::rename(datetime = .time) |>
    dplyr::mutate(
      date = as.Date(datetime),
      wind_speed_10m = as.numeric(wind_speed_10m),
      wind_dir_deg = as.numeric(wind_dir_deg)
    ) |>
    dplyr::select(dplyr::all_of(point_cols), date, wind_speed_10m, wind_dir_deg)
}


#' Annual building density (impervious fraction) from GAIA at point locations
#' @param points_sf sf POINT object with properties to keep
#' @param years integer vector of years to extract
#' @param buffer_m numeric; buffer radius in meters
#' @param scale numeric; sampling scale in meters
#' @param dataset character; EE ImageCollection id (default GAIA v10)
#' @param band character; band name representing built/impervious cover
#' @param via character; extraction backend for rgee::ee_extract_timeseries
#' @param container optional Drive/GCS folder when via != "getInfo"
#' @return tibble with point identifiers, year, building_density
#' @export
gee_extract_building_density <- function(
  points_sf,
  years,
  buffer_m = 100,
  scale = 30,
  dataset = "Tsinghua/FROM-GLC/GAIA/v10",
  band = "built",
  via = Sys.getenv("EE_VIA", unset = "getInfo"),
  container = Sys.getenv("EE_DRIVE_FOLDER", unset = NULL)
) {
  gee_init()
  ee <- rgee::ee

  yrs <- sort(unique(as.integer(years)))
  pts_fc <- gee_prepare_points(points_sf, buffer_m = buffer_m)

  yearly_imgs <- ee$ImageCollection(
    ee$List(yrs)$map(
      rgee::ee_utils_pyfunc(function(y) {
        year_num <- ee$Number(y)
        img_year <- ee$ImageCollection(dataset)$
          filter(ee$Filter$calendarRange(year_num, year_num, "year"))$
          select(band)$
          mean()
        img_year$rename("building_density")$
          set("system:time_start", ee$Date$fromYMD(year_num, 1, 1)$millis())
      })
    )
  )

  bd_sf <- rgee::ee_extract_timeseries(
    x = yearly_imgs,
    y = pts_fc,
    scale = scale,
    reducer = ee$Reducer$mean(),
    via = via,
    container = container,
    sf = TRUE
  )

  bd_tbl <- bd_sf |>
    sf::st_drop_geometry() |>
    tibble::as_tibble()

  point_cols <- intersect(names(points_sf), names(bd_tbl))

  bd_tbl |>
    dplyr::rename(date = .time) |>
    dplyr::mutate(
      year = lubridate::year(date),
      building_density = as.numeric(building_density)
    ) |>
    dplyr::select(dplyr::all_of(point_cols), year, building_density)
}