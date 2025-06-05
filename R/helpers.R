
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