
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