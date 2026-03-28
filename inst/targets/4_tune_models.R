

list_fit_models <-
  list(
    targets::tar_target(
      name = chr_terms_x,
      command = {
        pat <- paste(
        c(
          "dsm", "dem", "d_road", "mtpi", "n_emittors_watershed",
          "blh",
          sprintf(
            "landuse_frac_%d",
            c(10, 11, 20, 51, 52, 61, 62, 71, 72,
              82, 91, 130, 150, 181, 182, 183, 186, 187,
              190, 200, 210
            )
          ),
          "aod"
        ),
        collapse = "|"
        )
        grep(pat, names(df_feat_correct_merged), value = TRUE)
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
    # ,
    # targets::tar_target(
    #   name = list_fit_tmb,
    #   command = {
    #     huimori::fit_tmb(
    # 
    #     )
    #   }
    # )
  )


list_tune_models <-
  list(
    targets::tar_target(
      name = workflow_tune_xgb_correct_spatial,
      command = {
        yvar <- as.character(form_fit)[2]
        data_sub <- df_feat_correct_merged %>%
          # dplyr::filter(year == int_years_spatial) %>%
          .[!is.na(.[[yvar]]), ] %>% # Filter out NA values for the outcome variable
          dplyr::mutate(site_type = droplevels(site_type))
        data_sub <-
          data_sub |>
          dplyr::mutate(
            latitude = as.double(stringi::stri_extract_first_regex(coords_google, pattern = "[3-4][0-9]\\.[0-9]{2,8}")),
            longitude = as.double(stringi::stri_extract_last_regex(coords_google, pattern = "1[2-4][0-9]\\.[0-9]{2,8}"))
          ) |>
          sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326) |>
          sf::st_transform(crs = "EPSG:5179")

        res <-
          fit_tidy_xgb(
            data = data_sub,
            formula = form_fit,
            invars = chr_terms_x,
            strata = "spatial",
            device = "cpu"
          )
        attr(res, "year") <- int_years_spatial
        res
      },
      # pattern = cross(int_years_spatial, form_fit),
      pattern = map(form_fit),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_10")
      )
    ),
    targets::tar_target(
      name = workflow_tune_mamba_correct_spatial,
      command = {
        data_sub <- df_feat_correct_merged %>%
          dplyr::filter(year == int_years_spatial) %>%
          .[!is.na(.[[yvar]]), ] %>% # Filter out NA values for the outcome variable
          dplyr::mutate(site_type = droplevels(site_type))

        data_sub
        # formula-like interface:
        # tidied data into tensors to run mamba
        # res <-
        #   fit_torch_mamba(
        #     data = data_sub,
        #     formula = form_fit,
        #     invars = chr_terms_x,
        #     strata = "site_type",
        #     device = "cpu"
        #   )
        # attr(res, "year") <- int_years_spatial
        # res
      },
      pattern = cross(int_years_spatial, form_fit),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_04")
      )
    ),
    targets::tar_target(
      name = workflow_tune_xgb_incorrect_spatial,
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
      name = workflow_tune_mamba_incorrect_spatial,
      command = {
        data_sub <- df_feat_correct_merged %>%
          dplyr::filter(year == int_years_spatial) %>%
          .[!is.na(.[[yvar]]), ] %>% # Filter out NA values for the outcome variable
          dplyr::mutate(site_type = droplevels(site_type))
        data_sub
        # formula-like interface:
        # tidied data into tensors to run mamba
        # res <-
        #   fit_torch_mamba(
        #     data = data_sub,
        #     formula = form_fit,
        #     invars = chr_terms_x,
        #     strata = "site_type",
        #     device = "cpu"
        #   )
        # attr(res, "year") <- int_years_spatial
        # res
      },
      pattern = cross(int_years_spatial, form_fit),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_04")
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
      name = workflow_fit_xgb_correct,
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


list_tune_eval <- list(
  targets::tar_target(
    name = df_tune_correct_metrics,
    command = {
      df_metrics <- tune::collect_metrics(workflow_tune_xgb_correct_spatial)
      df_metrics
    },
    pattern = map(workflow_tune_xgb_correct_spatial),
    iteration = "list",
    resources = targets::tar_resources(
      crew = targets::tar_resources_crew(controller = "controller_08")
    )
  ),
  # Variable importance from the best model in the tuning results
  targets::tar_target(
    name = df_tune_correct_vip,
    command = {
      train_variables <-
        workflow_tune_xgb_correct_spatial |>
        attr("workflow") |>
        _[["pre"]] |>
        _[["actions"]] |>
        _[["recipe"]] |>
        _[["recipe"]]

      train_data <-
        train_variables[["template"]]

      names_variables <-
        train_variables |>
        _[["var_info"]] |>
        _[["variable"]]
      # remove the outcome variable
      names_target <- names_variables[length(names_variables)]
      names_variables <- names_variables[-length(names_variables)]
      names_variables


      df_train_fit <-
        workflow_tune_xgb_correct_spatial %>%
        tune::fit_best() %>%
        tune::extract_fit_parsnip()

      pfun_shap <- function(object, newdata) {
        predict(object, new_data = newdata, type = "raw")
      }

      # requires fastshap
      df_vip_fastshap <-
        vip::vi(
          object = df_train_fit,
          pred_wrapper = pfun_shap,
          method = "shap",
          feature_names = names_variables,
          train = train_data
        ) |>
        dplyr::rename(
          importance_shap = Importance
        )
      df_vip_permute <-
        vip::vi(
          object = df_train_fit,
          method = "permute",
          feature_names = names_variables,
          train = train_data,
          target = names_target,
          pred_wrapper = pfun_shap,
          metric = "rmse"
        ) |>
        dplyr::rename(
          importance_permute = Importance
        )
      df_vip <-
        dplyr::left_join(
          df_vip_fastshap, df_vip_permute, by = "Variable"
        )
      df_vip
    },
    pattern = map(workflow_tune_xgb_correct_spatial),
    iteration = "list",
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


# ----------------------------------------------------------------
# 변경 log 기록(dhnyu)
## 2026.01.31

### DAG 상에서 최종 객체와 직접적으로 이어지지 않는 target 체크
#### list_fit_tmb 주석처리