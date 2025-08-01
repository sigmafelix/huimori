

list_fit_models <-
  list(
    targets::tar_target(
      name = chr_terms_x,
      command = {
        c(
          "dsm", "dem", "d_road", "mtpi", "n_emittors_watershed",
          sprintf(
            "frac_%d",
            c(10, 11, 20, 51, 52, 61, 62, 71, 72,
              82, 91, 130, 150, 181, 182, 183, 186, 187,
              190, 200, 210
            )
          )
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
          .[!is.na(.[[yvar]]), ] %>% # Filter out NA values for the outcome variable
          dplyr::mutate(site_type = droplevels(site_type))
        res <-
          fit_tidy_xgb(
            data = data_sub,
            formula = form_fit,
            invars = chr_terms_x,
            strata = "site_type",
            device = "cpu"
          )
        attr(res, "year") <- int_years_spatial
        res
      },
      pattern = cross(int_years_spatial, form_fit),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_04")
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
    # targets::tar_target(
    #   name = workflow_tune_correct_full,
    #   command = {
    #     fit_tidy_xgb(
    #       data = df_feat_correct_merged,
    #       formula = form_fit,
    #       invars = chr_terms_x
    #     )
    #   },
    #   pattern = map(form_fit),
    #   iteration = "list"
    # ),
    # targets::tar_target(
    #   name = workflow_tune_incorrect_full,
    #   command = {
    #     fit_tidy_xgb(
    #       data = df_feat_incorrect_merged,
    #       formula = form_fit,
    #       invars = chr_terms_x
    #     )
    #   },
    #   pattern = map(form_fit),
    #   iteration = "list"
    # ),
    targets::tar_target(
      name = workflow_fit_correct,
      command = {
        yvar <- tune::outcome_names(workflow_tune_correct_spatial)

        df_combined <-
          df_feat_grid_merged %>%
          sf::st_drop_geometry() %>%
          dplyr::mutate(
            n_emittors_watershed = ifelse(
              is.na(n_emittors_watershed),
              0,
              n_emittors_watershed
            )
          )
        nonexistent_terms <-
          setdiff(
            chr_terms_x,
            names(df_combined)
          )
        nonexistent_terms <- grep("^frac_", nonexistent_terms, value = TRUE)
        if (length(nonexistent_terms) > 0) {
          for (term in nonexistent_terms) {
            df_combined[[term]] <- 0
          }
        }

          #dplyr::filter(!is.na(class_03))
          # purrr::map(
          #   .x = .,
          #   .f = ~ sf::st_drop_geometry(dplyr::select(.x, all_of(chr_terms_x)))
          # ) %>%
          # purrr::reduce(
          #   .x = .,
          #   .f = dplyr::bind_rows
          # )
        fitted <-
          tune::fit_best(
            workflow_tune_correct_spatial,
            metric = "rmse"
          ) %>%
          predict(
            .,
            df_combined
          )
        fitted <-
          dplyr::bind_cols(fitted, df_combined[, c(4, 1, 2)]) %>%
          dplyr::mutate(year = attr(workflow_tune_correct_spatial, "year"))
        names(fitted)[1] <- yvar
        fitted
      },
      pattern = cross(workflow_tune_correct_spatial, df_feat_grid_merged),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_08")
      )
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
      pattern = map(workflow_tune_incorrect_spatial),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_08")
      )
    )

  )

list_pred_process <-
  list(
    targets::tar_target(
      df_diff_correct_incorrect,
      command = {
        data.frame(
          diff = unlist(workflow_fit_correct) - unlist(workflow_fit_incorrect)
        )
      },
      pattern = map(workflow_fit_correct, workflow_fit_incorrect),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_08")
      )
    ),
    targets::tar_target(
      name = chr_file_grid_250m,
      command = file.path(chr_dir_data, "grid_250m.gpkg")
    ),
    targets::tar_target(
      name = df_grid_250m,
      command = {
        sf::st_read(
          dsn = chr_file_grid_250m,
          quiet = TRUE
        ) %>%
          dplyr::select(
            -gid
          ) %>%
          sf::st_coordinates() %>%
          dplyr::as_tibble()
      }
    ),
    targets::tar_target(
      name = df_diff_correct_incorrect_coord,
      command = {
        df_diff_correct_incorrect %>%
          dplyr::bind_cols(
            df_grid_250m
          )
      },
      pattern = map(df_diff_correct_incorrect),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_08")
      )
    ),
    targets::tar_target(
      name = sf_pred_correct_xgb_pm,
      command = {
        sf_pred_xgb_pm <-
          sf::st_read(
            dsn = chr_file_grid_250m,
            quiet = TRUE
          ) %>%
          dplyr::select(
            -gid
          ) %>%
          dplyr::bind_cols(
            workflow_fit_correct
          )
        sf_pred_xgb_pm
      },
      pattern = map(workflow_fit_correct)
    ),
    targets::tar_target(
      name = sf_pred_incorrect_xgb_pm,
      command = {
        sf_pred_xgb_pm <-
          sf::st_read(
            dsn = chr_file_grid_250m,
            quiet = TRUE
          ) %>%
          dplyr::select(
            -gid
          ) %>%
          dplyr::bind_cols(
            workflow_fit_incorrect
          )
        sf_pred_xgb_pm
      },
      pattern = map(workflow_fit_incorrect)
    )
  )