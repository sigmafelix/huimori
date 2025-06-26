
#' Impute any missing values
#'
#' @param data data.frame. Data to be imputed.
#'
#' @return Imputed data.frame.
#' @importFrom missRanger missRanger
#' @export
impute_df <-
  function(data) {
    # Check if the data frame is empty
    if (nrow(data) == 0) {
      data_imp <- data
    }

    # Check if the data frame has any missing values
    if (any(is.na(data))) {
      # Impute missing values using na_interpolation
      data_imp <- missRanger::missRanger(
        data = data,
        num.trees = 1000,
        mtry = NULL,
        min.node.size = 10,
        max.depth = 5,
        pmm.k = 3,
        seed = 2025
      )
    } else {
      data_imp <- data
    }

    return(data_imp)
  }




#' Interpolate air quality data
#'
#' This function interpolates missing values in air quality data using various methods.
#' It first creates a complete time series with regular intervals for each ID, then applies
#' ensemble interpolation methods to fill in missing values for specified columns.
#'
#' @param df A data frame containing air quality data with columns for ID, date/time, and values to interpolate.
#' @param id_col The name of the column containing unique IDs (default: "id").
#' @param date_col The name of the column containing date/time information (default: "datehour").
#' @param value_cols A vector of column names to interpolate (default: c("PM10", "PM25")).
#' @param interval The time interval for creating the complete time series (default: "hour").
#'
#' @return A data frame with interpolated values for the specified columns.
#'
interpolate_air_quality <-
  function(
    df,
    id_col = "id",
    date_col = "datehour",
    value_cols = c("PM10", "PM25"),
    interval = "hour"
  ) {
    # Convert to proper date-time format if not already
    if (!inherits(df[[date_col]], "POSIXt")) {
      df[[date_col]] <- as.POSIXct(df[[date_col]], format = "%Y-%m-%d", tz = "Asia/Seoul")
    }

    # Step 1: Arrange data by id and datehour
    df <- df %>%
      dplyr::arrange({{ id_col }}, {{ date_col }})

    # Step 2: Create complete time series with regular intervals for each id
    result <- df %>%
      # dplyr::group_by({{ id_col }}) %>%
      tidyr::complete(
        !!sym(date_col) := 
          seq(min(!!sym(date_col), na.rm = TRUE),
          max(!!sym(date_col), na.rm = TRUE),
        by = interval
      )) %>%
      ungroup()

    print(result)

    # Step 3: Apply ensemble interpolation for each value column within each id group
    for (col in value_cols) {
      if (col %in% names(result)) {
        # explicitly columnwise treatment
        result[[col]] <- imputeTS::na_seadec(as.ts(unlist(result[[col]])), algorithm = "ma")
        result[[col]] <- imputeTS::na_interpolation(as.ts(unlist(result[[col]])), option = "linear")
        result[[col]] <- imputeTS::na_kalman(as.ts(unlist(result[[col]])))
        # result <- result %>%
        #   # dplyr::group_by(!!sym(id_col)) %>%
        #   dplyr::mutate(!!col := #if_else(all(is.na(!!sym(col))),
        #     #!!sym(col), # Keep all NA if entire series is NA
        #     imputeTS::na_seadec(!!sym(col), algorithm = "ma")
        #   ) %>%
        #   # Apply multiple methods for better results
        #   dplyr::mutate(!!col := #if_else(is.na(!!sym(col)),
        #     imputeTS::na_interpolation(!!sym(col), option = "linear")
        #     #!!sym(col)
        #   ) %>%
        #   # Final backup method
        #   dplyr::mutate(!!col := #dplyr::if_else(is.na(!!sym(col)),
        #     imputeTS::na_kalman(!!sym(col))#,
        #     #!!sym(col)
        #   ) %>%
        #   dplyr::ungroup()
      }
    }

    return(result)
  }


#' Interpolate air quality data in tsibble format
#'
#' This function interpolates missing values in air quality data stored in
#'   a tsibble format.
#'
#' @param df A data frame or tibble containing air quality data with columns
#'   for ID, date/time, and values to interpolate.
#' @param id_col The name of the column containing unique IDs (default: "id").
#' @param date_col The name of the column containing date/time information
#'   (default: "datehour").
#' @param value_cols A vector of column names to interpolate
#'   (default: c("PM10", "PM25")).
#' @param interval The time interval for creating the complete time series
#'   (default: "hour").
#' @return A tsibble with interpolated values for the specified columns.
#' @importFrom tsibble as_tsibble
#' @importFrom fabletools as_tsibble
#' @importFrom dplyr group_by summarise filter mutate ungroup
#' @importFrom purrr map_dfr
#' @importFrom stats approx
#' @importFrom tidyr fill
#' @importFrom rlang sym
#' @importFrom rlang !!
#' @export
interpolate_air_quality_tsibble <-
  function(df,
           id_col = "id",
           date_col = "datehour",
           value_cols = c("PM10", "PM25"),
           interval = "hour") {
    # Ensure date column is in POSIXct format
    if (!inherits(df[[date_col]], "POSIXt")) {
      df[[date_col]] <- as.POSIXct(df[[date_col]], format = "%Y-%m-%d", tz = "Asia/Seoul")
    }

    # Convert tibble to tsibble with explicit interval
    ts_data <- df %>%
      as_tsibble(
        index = !!sym(date_col),
        key = !!sym(id_col),
        regular = FALSE # Set to FALSE for irregular data
      )

    # Determine the min and max times for each group
    time_ranges <- ts_data %>%
      group_by({{ id_col }}) %>%
      summarise(
        start_time = min({{ date_col }}, na.rm = TRUE),
        end_time = max({{ date_col }}, na.rm = TRUE)
      )

    # Create a complete dataset
    result <- purrr::map_dfr(seq(1, nrow(time_ranges)), function(i) {
      group_id <- time_ranges[[id_col]][i]
      start_time <- time_ranges$start_time[i]
      end_time <- time_ranges$end_time[i]

      # Create complete sequence of times
      complete_times <- tibble(
        !!sym(id_col) := group_id,
        !!sym(date_col) := seq(start_time, end_time, by = interval)
      )

      # Join with original data
      group_data <- ts_data %>%
        filter(!!sym(id_col) == group_id)

      complete_data <- complete_times %>%
        left_join(group_data, by = c(id_col, date_col))

      return(complete_data)
    })

    # Interpolate each value column
    for (col in value_cols) {
      if (col %in% names(result)) {
        result <- result %>%
          group_by(!!sym(id_col)) %>%
          # Use base R approx function for linear interpolation
          mutate(
            !!col := if (sum(!is.na(!!sym(col))) >= 2) {
              stats::approx(
                x = as.numeric(!!sym(date_col)),
                y = !!sym(col),
                xout = as.numeric(!!sym(date_col)),
                method = "linear",
                rule = 2
              )$y
            } else {
              !!sym(col)
            }
          ) %>%
          # Fill remaining NAs with forward and backward fill
          tidyr::fill(!!col, .direction = "down") %>%
          tidyr::fill(!!col, .direction = "up") %>%
          ungroup()
      }
    }

    # # Convert back to tibble
    # result <- ts_data %>%
    #   as_tibble()

    return(result)
  }


#' Detrend using STL model
#'
#' @param data data.table sorted by timestamps.
#' @param id character(1). Site identifier.
#' @param season_length integer(1). Length of seasonal trend detection.
#' @param vars character.
#' @param result character(1). Result variable name.
#' @param fill logical(1).
#'   Filling (imputing) missing values that present in the data.
#' @return STL fit results.
#' @import fabletools
#' @importFrom feasts STL
#' @importFrom dplyr filter
#' @importFrom tsibble fill_gaps as_tsibble
#' @importFrom tidyr fill
#' @export
detrend <-
  function(
    data,
    id,
    season_length = 8760,
    vars = NULL,
    result = "PM10",
    fill = FALSE
  ) {
    stopifnot(!is.null(id))
    stopifnot(!is.null(result))
    requireNamespace("rlang")
    requireNamespace("fable")
    requireNamespace("fabletools")
    requireNamespace("tsibble")
    requireNamespace("feasts")

    data <-
      data |>
      dplyr::filter(TMSID == id) |>
      dplyr::mutate(TMSID = as.character(TMSID))

    if (fill) {
      data <-
        data %>%# & (PM10 > 0 & PM10 < 500)) |>
        dplyr::mutate(
          !!rlang::sym(result) := ifelse(
            is.na(!!rlang::sym(result)) | !!rlang::sym(result) < 0,
            NA, !!rlang::sym(result))
        ) %>%
        tsibble::as_tsibble(key = !!rlang::sym(result), index = "datehour") %>%
        dplyr::as_tibble() %>%
        tidyr::fill(!!rlang::sym(result), .direction = "updown") %>%
        tsibble::as_tsibble(key = "TMSID", index = "datehour") %>%
        tsibble::fill_gaps(!!rlang::sym(result) := mean(!!rlang::sym(result)), .full = TRUE)
      # return(data)
    }

    if (!is.null(vars)) {
      form_custom <-
        reformulate(
          termlabels = vars,
          response = result
        )
    } else {
      form_custom <-
        as.formula(paste0(result, "~1"))
    }
    form_custom <-
      update(
        form_custom,
        ~ . +
          season(period = season_length) +
          trend(window = season_length * 12)
      )
    # form_error <-
    #   update(
    #     form_custom,
    #     ~ . +
    #       error("A") +
    #       season(period = season_length) +
    #       trend(window = season_length * 12)
    #   )

    stl_dat <-
      fabletools::model(
        .data = data,
        stl = STL(formula = !!form_custom)
        # ets = ETS(formula = !!form_error)
      )

    stl_comp <- fabletools::components(stl_dat)
    return(stl_comp)
  }


#' Fit Spatial Models and Predict Relocation Impact
#'
#' This function fits spatial models using the `sdmTMB` package
#' for two sets of spatial data: corrected site data and original site data.
#' It then predicts the impact of relocation on a specified variable
#' (e.g., PM10) over a grid and computes the difference between
#' the predictions for the two datasets.
#'
#' @param sf_correct A `sf` object containing the corrected site data with
#'   spatial and temporal information.
#' @param sf_original A `sf` object containing the original site data with
#'   spatial and temporal information.
#' @param grid_in An optional `sf` object representing the grid for predictions.
#'   If `NULL`, a grid is generated.
#' @param formula A formula specifying the model to be fitted
#'   (e.g., `PM10 ~ 1`).
#' @param year_target An integer specifying the target year for filtering
#'   the data (default: 2020).
#' @param resolution A numeric value specifying the resolution (cell size)
#'   of the grid in meters (default: 1000).
#' @param reference A `sf` object used as a reference for generating the grid.
#' @param likelihood A family object specifying the likelihood function for
#'   the model (default: `gaussian()`).
#' @param anisotropy A logical value indicating whether to include anisotropy
#'   in the model (default: `TRUE`).
#' @param log_transform A logical value indicating whether to apply
#'   a log transformation to the predictions (default: `FALSE`).
#' @param invars An optional vector of variable names to include
#'   in the prediction.
#'
#' @return A list containing the following elements:
#' \describe{
#'   \item{pred_correct}{A raster object of predictions for the corrected site data.}
#'   \item{pred_original}{A raster object of predictions for the original site data.}
#'   \item{pred_diff}{A raster object of the difference between the corrected and original predictions.}
#' }
#'
#' @details
#' The function performs the following steps:
#' 1. Generates a grid for predictions if `grid_in` is not provided.
#' 2. Filters and preprocesses the corrected and original site data
#'   for the target year.
#' 3. Creates mesh objects for spatial modeling using `sdmTMB::make_mesh`.
#' 4. Fits spatial models for the corrected and original site data
#'   using `fit_tmb`.
#' 5. Predicts the target variable over the grid for both datasets
#'   using `pred_tmb`.
#' 6. Computes the difference between the predictions for the corrected and
#'   original datasets.
#'
#' @examples
#' \dontrun{
#' result <- fit_all_tmb(
#'   sf_correct = corrected_sites,
#'   sf_original = original_sites,
#'   formula = PM10 ~ 1,
#'   year_target = 2020,
#'   resolution = 1000,
#'   reference = reference_sf
#' )
#' plot(result$pred_diff)
#' }
#'
#' @importFrom sf st_make_grid st_as_sf st_coordinates
#' @importFrom dplyr filter mutate rename
#' @importFrom lubridate ymd
#' @importFrom sdmTMB make_mesh
#' @export
fit_all_tmb <-
  function(
    sf_correct = NULL,
    sf_original = NULL,
    grid_in = NULL,
    formula = PM10 ~ 1,
    year_target = 2020,
    resolution = 1000,
    reference = NULL,
    likelihood = gaussian(),
    anisotropy = TRUE,
    log_transform = FALSE,
    invars = NULL
  ) {
    if (is.null(grid_in)) {
      message("Make grid points...")
      kgr <-
        sf::st_make_grid(
          reference,
          cellsize = resolution,
          what = "centers"
        ) |>
        sf::st_as_sf() |>
        dplyr::rename(geometry = x) |>
        dplyr::mutate(PM10 = NA, PM25 = NA)
      sf_grid <- kgr[reference, ]
    } else {
      sf_grid <- grid_in
    }

    message("Create sf locations...")
    sf_correct_base <-
      sf_correct %>%
      dplyr::filter(year == year_target) %>%
      dplyr::filter(
        date_start < lubridate::ymd(paste0(year_target, "-01-01")) &
        date_end >= lubridate::ymd(paste0(year_target, "-01-01"))
      ) %>%
      dplyr::filter(!is.na(PM10)) %>%
      cbind(
        .,
        sf::st_coordinates(.)
      ) |>
      as.data.frame() |>
      dplyr::mutate(
        lon = X / 1000,
        lat = Y / 1000
      ) |>
      dplyr::filter(!is.na(X))

    sf_original_base <-
      sf_original %>%
      dplyr::filter(year == year_target) %>%
      dplyr::filter(
        date_start < lubridate::ymd(paste0(year_target, "-01-01")) &
        date_end >= lubridate::ymd(paste0(year_target, "-01-01"))
      ) %>%
      dplyr::filter(!is.na(PM10)) %>%
      dplyr::filter(!is.nan(PM10)) %>%
      cbind(
        .,
        sf::st_coordinates(.)
      ) |>
      as.data.frame() |>
      dplyr::mutate(
        lon = X / 1000,
        lat = Y / 1000
      ) |>
      dplyr::filter(!is.na(X))

    message(summary(sf_correct_base[[as.character(formula)[2]]]))
    message("Configure mesh objects...")
    mesh_in_correct <-
      sdmTMB::make_mesh(
        sf_correct_base,
        type = "cutoff_search",
        xy_cols = c("lon", "lat"),
        cutoff = 0.00001
      )
    mesh_in_original <-
      sdmTMB::make_mesh(
        sf_original_base,
        type = "cutoff_search",
        xy_cols = c("lon", "lat"),
        cutoff = 0.00001
      )

    message("Fit TMB models (anisotropy)...")
    fit_ok_correct <-
      fit_tmb(
        formula = formula,
        data = sf_correct_base,
        mesh = mesh_in_correct,
        reml = TRUE,
        anisotropy = anisotropy,
        bayesian = TRUE,
        family = likelihood,
        spatial = "on"
      )
    fit_ok_original <-
      fit_tmb(
        formula = formula,
        data = sf_original_base,
        mesh = mesh_in_original,
        reml = TRUE,
        anisotropy = anisotropy,
        bayesian = TRUE,
        family = likelihood,
        spatial = "on"
      )

    message("Create prediction at grids...")
    pred_ok_correct <-
      pred_tmb(fit_ok_correct, grid = sf_grid, invars = invars)
    pred_ok_original <-
      pred_tmb(fit_ok_original, grid = sf_grid, invars = invars)

    rast_tmb_ok_correct <-
      pred_ok_correct %>%
      tmb_rast(log = log_transform)
    rast_tmb_ok_original <-
      pred_ok_original %>%
      tmb_rast(log = log_transform)

    rast_tmb_ok_diff <-
      rast_tmb_ok_correct - rast_tmb_ok_original
    res <-
      list(
        pred_correct = rast_tmb_ok_correct,
        pred_original = rast_tmb_ok_original,
        pred_diff = rast_tmb_ok_diff
      )
    return(res)
  }


#' Fit Spatial-Temporal Models and Predict Relocation Impact
#'
#' This function fits spatial-temporal models using the `sdmTMB` package for two datasets: 
#' corrected site data (`sf_correct`) and original site data (`sf_original`). It then predicts 
#' the impact of relocation on a grid and computes the difference between the predictions.
#'
#' @param sf_correct A `sf` object containing corrected site data with spatial and PM10 information.
#' @param sf_original A `sf` object containing original site data with spatial and PM10 information.
#' @param formula A formula specifying the model to be fitted. Default is `PM10 ~ 1`.
#' @param time A string specifying the time variable in the dataset. Default is `"year"`.
#' @param year_target An integer specifying the target year for filtering data. Default is `2020`.
#' @param resolution A numeric value specifying the resolution of the prediction grid in meters. Default is `1000`.
#' @param reference A `sf` object used as the spatial reference for creating the prediction grid.
#' @param likelihood A family object specifying the likelihood function for the model. Default is `gaussian()`.
#' @param anisotropy A logical value indicating whether to include anisotropy in the model. Default is `TRUE`.
#' @param log_transform A logical value indicating whether to log-transform the predictions. Default is `FALSE`.
#'
#' @return A list containing the following elements:
#' \describe{
#'   \item{pred_correct}{A raster object of predictions for the corrected site data.}
#'   \item{pred_original}{A raster object of predictions for the original site data.}
#'   \item{pred_diff}{A raster object of the difference between corrected and original predictions.}
#' }
#'
#' @details
#' The function performs the following steps:
#' \enumerate{
#'   \item Creates a prediction grid based on the reference spatial object.
#'   \item Prepares the corrected and original site data by filtering and adding coordinates.
#'   \item Configures mesh objects for spatial modeling.
#'   \item Fits spatial-temporal models for both corrected and original datasets.
#'   \item Predicts PM10 values on the grid for both datasets.
#'   \item Computes the difference between the predictions.
#' }
#'
#' @examples
#' \dontrun{
#' result <- fit_all_tmb_spt(
#'   sf_correct = sites_sf,
#'   sf_original = sites_asis_sf,
#'   formula = PM10 ~ 1,
#'   time = "year",
#'   year_target = 2020,
#'   resolution = 1000,
#'   reference = kor_all,
#'   likelihood = gaussian(),
#'   anisotropy = TRUE,
#'   log_transform = FALSE
#' )
#' }
#'
#' @importFrom sf st_make_grid st_as_sf st_coordinates
#' @importFrom dplyr rename mutate filter bind_rows
#' @importFrom lubridate ymd
#' @export
fit_all_tmb_spt <-
  function(
    sf_correct = NULL,
    sf_original = NULL,
    formula = PM10 ~ 1,
    time = "year",
    year_target = 2020,
    resolution = 1000,
    reference = NULL,
    likelihood = gaussian(),
    anisotropy = TRUE,
    log_transform = FALSE
  ) {
    message("Make grid points...")
    kgr <-
      sf::st_make_grid(
        reference,
        cellsize = resolution,
        what = "centers"
      ) |>
      sf::st_as_sf() |>
      dplyr::rename(geometry = x) |>
      dplyr::mutate(PM10 = NA, PM25 = NA)
    sf_grid <- kgr[reference, ]
    sf_grid_l <-
      lapply(seq(2010, 2023), function(x) {
        sf_grid |>
          dplyr::mutate(year = x)
      }) |>
      do.call(what = bind_rows)



    message("Create sf locations...")
    sf_correct_base <-
      sf_correct %>%
      # dplyr::filter(year == year_target) %>%
      # dplyr::filter(
      #   date_start < lubridate::ymd(paste0(year_target, "-01-01")) &
      #   date_end >= lubridate::ymd(paste0(year_target, "-01-01"))
      # ) %>%
      dplyr::filter(!is.na(PM10)) %>%
      cbind(
        .,
        sf::st_coordinates(.)
      ) |>
      as.data.frame() |>
      dplyr::mutate(
        lon = X / 1000,
        lat = Y / 1000
      ) |>
      dplyr::filter(!is.na(X))

    sf_original_base <-
      sf_original %>%
      # dplyr::filter(year == year_target) %>%
      # dplyr::filter(
      #   date_start < lubridate::ymd(paste0(year_target, "-01-01")) &
      #   date_end >= lubridate::ymd(paste0(year_target, "-01-01"))
      # ) %>%
      dplyr::filter(!is.na(PM10)) %>%
      dplyr::filter(!is.nan(PM10)) %>%
      cbind(
        .,
        sf::st_coordinates(.)
      ) |>
      as.data.frame() |>
      dplyr::mutate(
        lon = X / 1000,
        lat = Y / 1000
      ) |>
      dplyr::filter(!is.na(X))

    summary(sf_correct_base[[all.vars(formula)[1]]])
    message("Configure mesh objects...")
    mesh_in_correct <-
      sdmTMB::make_mesh(
        sf_correct_base,
        type = "cutoff_search",
        xy_cols = c("lon", "lat"),
        cutoff = 0.00001
      )
    mesh_in_original <-
      sdmTMB::make_mesh(
        sf_original_base,
        type = "cutoff_search",
        xy_cols = c("lon", "lat"),
        cutoff = 0.00001
      )

    message("Fit TMB models (anisotropy)...")
    fit_ok_correct <-
      fit_tmb(
        formula = formula,
        data = sf_correct_base,
        mesh = mesh_in_correct,
        reml = TRUE,
        anisotropy = anisotropy,
        bayesian = TRUE,
        family = likelihood,
        spatial = "on",
        spatiotemporal = "rw",
        time = time
      )
    fit_ok_original <-
      fit_tmb(
        formula = formula,
        data = sf_original_base,
        mesh = mesh_in_original,
        reml = TRUE,
        anisotropy = anisotropy,
        bayesian = TRUE,
        family = likelihood,
        spatial = "on",
        spatiotemporal = "rw",
        time = time
      )

    message("Create prediction at grids...")
    pred_ok_correct <- pred_tmb(fit_ok_correct, grid = sf_grid_l)
    pred_ok_original <- pred_tmb(fit_ok_original, grid = sf_grid_l)

    rast_tmb_ok_correct <-
      pred_ok_correct %>%
      tmb_rast(log = log_transform)
    rast_tmb_ok_original <-
      pred_ok_original %>%
      tmb_rast(log = log_transform)

    rast_tmb_ok_diff <-
      rast_tmb_ok_correct - rast_tmb_ok_original
    res <-
      list(
        pred_correct = rast_tmb_ok_correct,
        pred_original = rast_tmb_ok_original,
        pred_diff = rast_tmb_ok_diff
      )
    return(res)
  }




#' Convert `sdmTMB` result to terra
#' @param tmb_res `sdmTMB` result.
#' @param log logical(1). If `TRUE`, apply `exp` to the result.
#' @return `terra` raster object.
#' @importFrom rlang is_installed
#' @importFrom tidyterra as_spatraster
#' @importFrom terra app
#' @export
tmb_rast <- function(tmb_res, log = FALSE) {
  rlang::is_installed("tidyterra")
  # restore original coordinate scale
  tmb_res[["lon"]] <- tmb_res[["lon"]] * 1000
  tmb_res[["lat"]] <- tmb_res[["lat"]] * 1000
  ras <-
    tidyterra::as_spatraster(
      x = tmb_res, xycols = seq(1, 2), crs = "EPSG:5179"
    )
  if (log) {
    ras <- terra::app(ras, exp)
  }
  return(ras)
}




#' Predict using a fitted TMB model on a spatial grid
#'
#' This function generates predictions from a fitted TMB model (`fit`) over
#'   a provided spatial grid (`grid`). It extracts spatial coordinates,
#'   processes them, and constructs a new data frame with the required input
#'   variables (`invars`) before making predictions.
#'
#' @param fit A fitted TMB model object.
#' @param grid An `sf` spatial object representing the prediction grid.
#' @param invars A character vector of variable names to include
#'   in the prediction data frame.
#'
#' @return A vector or data frame of predictions from the fitted model.
#'
#' @examples
#' \dontrun{
#' predictions <- pred_tmb(fit, grid, c("var1", "var2"))
#' }
#' @export
fit_tmb <-
  function(
    data,
    formula,
    formula_s = NULL,
    mesh,
    family,
    spatial = "on",
    reml = TRUE,
    anisotropy = TRUE,
    bayesian = TRUE,
    ...
  ) {
    rlang::inject(
      sdmTMB::sdmTMB(
        formula,
        data = data,
        mesh = mesh,
        family = family,
        control = sdmTMB::sdmTMBcontrol(
          eval.max = 10000,
          iter.max = 10000,
          normalize = FALSE,
          nlminb_loops = 10L,
          newton_loops = 10L
        ),
        spatial = spatial,
        spatial_varying = formula_s,
        reml = reml,
        anisotropy = anisotropy,
        bayesian = bayesian,
        !!!list(...)
      )
    )
  }



#' Create an INLA Mesh from sf Data
#'
#' This function generates a 2D mesh suitable for spatial modeling with INLA,
#'   based on input spatial features (`sf` object).
#'
#' @param sf_data An `sf` object containing the spatial features to be meshed.
#' @param max_edge Numeric vector specifying the maximum allowed triangle
#'   edge lengths in the mesh.
#' @param crs Coordinate reference system to use for the mesh (default: 4326).
#'
#' @return An INLA mesh object created using `fmesher::fm_mesh_2d_inla`.
#'
#' @details
#' The function transforms the input `sf` data to the specified CRS,
#'   extracts coordinates, and defines a bounding box slightly larger than
#'   the data extent. It then constructs a mesh using 
#'   `fmesher::fm_mesh_2d_inla`, with boundaries and interior segments derived
#'   from the input features.
#'
#' @importFrom sf st_coordinates st_transform
#' @importFrom fmesher fm_mesh_2d_inla fm_extensions fm_as_segm
#' @importFrom INLA inla.mesh.2d
#'
#' @examples
#' \dontrun{
#' library(sf)
#' nc <- st_read(system.file("shape/nc.shp", package="sf"))
#' mesh <- create_inla_mesh(nc, max_edge = c(0.1, 0.5))
#' }
#' @export
create_inla_mesh <-
  function(
    data,
    formula,
    mesh,
    family = "gaussian",
    control = list()
  ) {
    # Assuming `data` is an sf object with geometry specified
    # `mesh` should be prepared for the SPDE model, typically using inla.mesh.2d
    requireNamespace("INLA", quietly = TRUE)
    # Define the SPDE model prior
    spde <- INLA::inla.spde2.pcmatern(mesh,
                                prior.range = c(5, 10),
                                prior.sigma = c(2, 1))

    # Stack data for INLA
    stk <- INLA::inla.stack(data=list(y = data[[all.vars(formula)[1]]]),
                      A = matrix(1, ncol(data), 1),
                      effects=list(list(model.matrix(~ . - 1, data = data[all.vars(formula)[-1]]))),
                      tag = "y")

    # Define the model formula for INLA
    formula.inla <-
      as.formula(
        paste(
          "PM10 ~", paste(all.vars(formula)[-1], collapse = " + "),
          "-1 + f(idx, model = spde)"
        )
      )

    # Fit the model
    result <-
      inla(formula.inla,
        family = family,
        data = inla.stack.data(stk),
        control.compute = list(config = TRUE),
        control.predictor = list(A = inla.stack.A(stk)),
        control.family =
        list(
          hyper =
          list(prec =
            list(initial = 1, fixed = FALSE)
          )
        ),
        control.inla = control
      )

    return(result)
  }

#' Predict using a fitted TMB model on a spatial grid
#'
#' This function generates predictions from a fitted TMB model (`fit`) over
#' a provided spatial grid (`grid`). It extracts spatial coordinates,
#' processes them, and constructs a new data frame with the required input
#' variables (`invars`) before making predictions.
#'
#' @param fit A fitted TMB model object.
#' @param grid An `sf` spatial object representing the prediction grid.
#' @param invars A character vector of variable names to include
#'   in the prediction data frame.
#' @return A vector or data frame of predictions from the fitted model.
#' @details
#' The function extracts coordinates from the grid, constructs a data frame
#' with longitude, latitude, and the specified input variables (`invars`),
#' and then uses the `predict` function to generate predictions from
#'   the fitted model.
#' @examples
#' \dontrun{
#' predictions <- pred_tmb(fit, grid, c("var1", "var2"))
#' }
#' @importFrom sf st_coordinates st_drop_geometry
#' @importFrom dplyr bind_cols mutate select all_of
#' @importFrom stats predict
#' @export
pred_tmb <-
  function(fit, grid, invars) {
    df_coord <-
      grid %>%
      sf::st_coordinates() %>%
      as.data.frame()

    df <-
      grid %>%
      dplyr::bind_cols(
        df_coord
      ) %>%
      sf::st_drop_geometry() %>%
      dplyr::mutate(
        lon = X / 1000,
        lat = Y / 1000
      ) %>%
      dplyr::select(lon, lat, dplyr::all_of(invars))
    predict(fit, newdata = df)
  }



#' Fit a Tuned XGBoost Regression Model with Tidymodels
#'
#' This function fits an XGBoost regression model using the tidymodels framework. It performs PCA on predictors starting with "class_", normalizes all predictors, and tunes hyperparameters (`min_n`, `tree_depth`, `learn_rate`) using a space-filling grid and ANOVA racing. The function returns the tuning results.
#'
#' @param data A data frame containing the training data.
#' @param formula A formula specifying the model.
#' @param invars A character vector of input variable names (not directly used in the function, but included for interface consistency).
#' @param nrounds Integer. Number of boosting rounds (trees) for XGBoost. Default is 1000.
#'
#' @return A `tune_race_anova` object containing the tuning results.
#'
#' @details
#' - Applies PCA to predictors starting with "class_" (5 components).
#' - Normalizes all predictors.
#' - Tunes `min_n`, `tree_depth`, and `learn_rate` over a space-filling grid (size 50).
#' - Uses 5-fold cross-validation and evaluates RMSE and R-squared.
#'
#' @import tidymodels
#' @import finetune
#' @importFrom recipes step_pca step_normalize
#' @importFrom dials min_n tree_depth learn_rate grid_space_filling
#' @importFrom hardhat extract_parameter_set_dials
#' @importFrom yardstick rmse rsq metric_set
#' @importFrom workflows workflow add_recipe add_model
#' @importFrom rsample vfold_cv
#'
#' @examples
#' \dontrun{
#' fit_tidy_xgb(data = my_data, formula = y ~ ., invars = names(my_data)[-1])
#' }
#' @export
fit_tidy_xgb <-
  function(data, formula, invars, nrounds = 1000) {
    xgb_spec <-
      boost_tree(
        mode = "regression",
        trees = nrounds,
        min_n = tune(),
        tree_depth = tune(),
        learn_rate = tune()
      ) |>
      set_engine("xgboost")

    xgb_rec <-
      recipe(formula, data = data) |>
      step_pca(starts_with("class_"), num_comp = 5) |>
      step_normalize(all_predictors())

    xgb_wf <-
      workflow() |>
      add_recipe(xgb_rec) |>
      add_model(xgb_spec)

    tuneset <-
      hardhat::extract_parameter_set_dials(xgb_wf) |>
      dials::grid_space_filling(
        min_n(c(2, 12)),
        tree_depth(c(3, 10)),
        learn_rate(range = c(-4, -1), trans = transform_log10()),
        size = 50
      )

    xgb_res <-
      xgb_wf |>
      finetune::tune_race_anova(
        resamples = vfold_cv(data, v = 5),
        grid = tuneset,
        metrics = yardstick::metric_set(
          yardstick::rmse, yardstick::mae),
        control = control_race(verbose = TRUE, verbose_elim = TRUE)
      )

    return(xgb_res)
  }