
list_tune_models <-
  list(
    targets::tar_target(
      name = tmb_correct_spatial,
      command = {
        yvar <- as.character(form_fit)[2]
        data_sub <- df_feat_correct_merged %>%
          dplyr::filter(year == int_years_spatial) %>%
          .[!is.na(.[[yvar]]), ] # Filter out NA values for the outcome variable
        fit_all_tmb(
          sf_correct = data_sub,
          sf_original = form_fit,
          grid_in = ,
          formula = ,
          year_target = int_years_spatial
        )
      },
      pattern = cross(int_years_spatial, form_fit),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_08")
      )
    ),
    targets::tar_target(
      name = workflow_tune_incorrect_spatial,
      command = {
        yvar <- as.character(form_fit)[2]
        data_sub <- df_feat_incorrect_merged %>%
          dplyr::filter(year == int_years_spatial) %>%
          .[!is.na(.[[yvar]]), ] # Filter out NA values for the outcome variable
        fit_tidy_xgb(
          data = data_sub,
          formula = form_fit,
          invars = chr_terms_x
        )
      },
      pattern = cross(int_years_spatial, form_fit),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_08")
      )
    ),
    targets::tar_target(
      name = workflow_fit_correct,
      command = {
        yvar <- tune::outcome_names(workflow_tune_correct_spatial)

        df_combined <-
          df_feat_grid_merged %>%
          purrr::map(
            .x = .,
            .f = ~ sf::st_drop_geometry(dplyr::select(.x, all_of(chr_terms_x)))
          ) %>%
          purrr::reduce(
            .x = .,
            .f = dplyr::bind_rows
          )
        fitted <-
          tune::fit_best(
            workflow_tune_correct_spatial,
            metric = "rmse"
          ) %>%
          predict(
            .,
            df_combined
          )
        names(fitted) <- yvar
        fitted
      },
      pattern = map(workflow_tune_correct_spatial)
    ),
    targets::tar_target(
      name = workflow_fit_incorrect,
      command = {
        yvar <- tune::outcome_names(workflow_tune_incorrect_spatial)

        df_combined <-
          df_feat_grid_merged %>%
          purrr::map(
            .x = .,
            .f = ~ sf::st_drop_geometry(dplyr::select(.x, all_of(chr_terms_x)))
          ) %>%
          purrr::reduce(
            .x = .,
            .f = dplyr::bind_rows
          )
        fitted <-
          tune::fit_best(
            workflow_tune_incorrect_spatial,
            metric = "rmse"
          ) %>%
          predict(
            .,
            df_combined
          )
        names(fitted) <- yvar
        fitted
      },
      pattern = map(workflow_tune_incorrect_spatial)
    )
  )