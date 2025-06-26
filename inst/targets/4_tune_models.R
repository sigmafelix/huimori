

list_fit_models <-
  list(
    targets::tar_target(
      name = chr_terms_x,
      command = {
        c(
          "dsm", "dem", "d_road",
          sprintf("class_%02d", c(1,3,11,13,14,15,16,21,22,31,32))
        )
      }
    )
    ,
    targets::tar_target(
      name = chr_outcome,
      command = {
        c("PM10", "PM25")
      }
    )
    ,
    targets::tar_target(
      name = form_fit,
      command = {
        total_formula <-
          reformulate(
            termlabels = chr_terms_x,
            response = chr_outcome
          )
        total_formula
      },
      pattern = map(chr_outcome),
      iteration = "list"
    )
    ,
    targets::tar_target(
      name = list_fit_tmb,
      command = {
        huimori::fit_tmb(

        )
      }
    )
  )


list_tune_models <-
  list(
    targets::tar_target(
      name = int_years_spatial,
      command = seq(2015, 2023, 1)
    ),
    targets::tar_target(
      name = workflow_tune_correct_spatial,
      command = {
        yvar <- as.character(form_fit)[2]
        data_sub <- df_feat_correct_merged %>%
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
      name = workflow_tune_correct_full,
      command = {
        fit_tidy_xgb(
          data = df_feat_correct_merged,
          formula = form_fit,
          invars = chr_terms_x
        )
      },
      pattern = map(form_fit),
      iteration = "list"
    ),
    targets::tar_target(
      name = workflow_tune_incorrect_full,
      command = {
        fit_tidy_xgb(
          data = df_feat_incorrect_merged,
          formula = form_fit,
          invars = chr_terms_x
        )
      },
      pattern = map(form_fit),
      iteration = "list"
    )

  )