

list_fit_models <-
  list(
    targets::tar_target(
      name = chr_terms_x,
      command = {
        terms_expected <-
          c(
            "dsm", "dem", "d_road", "mtpi", "mtpi_1km",
            landuse_fixed_terms(int_landuse_radius),
            yearly_buffer_mean_terms("aod_yearly", int_landuse_radius),
            yearly_buffer_mean_terms("blh_yearly", int_landuse_radius)
          )
        if (length(terms_expected) != 113L) {
          stop(
            "Expected 113 predictors for chr_terms_x, got ",
            length(terms_expected), "."
          )
        }
        missing_terms <- setdiff(terms_expected, names(df_feat_correct_merged))
        if (length(missing_terms) > 0L) {
          stop(
            "df_feat_correct_merged is missing expected predictors: ",
            paste(missing_terms, collapse = ", ")
          )
        }
        terms_expected
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
        data_sub <-
          prepare_xgb_correct_training_data(
            data = df_feat_correct_merged,
            formula = form_fit,
            target_year = int_years_spatial
          )
        resamples_spatial <-
          make_xgb_spatial_resamples(
            data = data_sub,
            v = 5L,
            method = "kmeans",
            id_col = "TMSID",
            crs = "EPSG:5179",
            seed = 20260728L,
            nstart = 100L
          )
        cv_diagnostics <-
          write_xgb_spatial_fold_diagnostics(
            data = data_sub,
            resamples = resamples_spatial,
            output_dir = file.path("daehoon", "logs", "cv_blocks")
          )
        res <-
          fit_tidy_xgb(
            data = data_sub,
            formula = form_fit,
            invars = chr_terms_x,
            resamples = resamples_spatial,
            grid_size = 250L,
            race_burn_in = 4,
            race_alpha = 0.01,
            race_num_ties = 25L,
            device = "cpu",
            nthread = 20L
          )
        attr(res, "target_year") <- attr(data_sub, "target_year")
        attr(res, "outcome") <- attr(data_sub, "outcome")
        attr(res, "cv_png") <- cv_diagnostics$paths[["map_png"]]
        attr(res, "cv_diagnostics") <- cv_diagnostics$paths
        res
      },
      pattern = cross(int_years_spatial, form_fit),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_01")
      )
    ),
    targets::tar_target(
      name = workflow_tune_mamba_correct_spatial,
      command = {
        yvar <- as.character(form_fit)[2]
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
        missing_terms <- setdiff(chr_terms_x, names(data_sub))
        for (term in missing_terms) {
          data_sub[[term]] <- 0
        }
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
        yvar <- as.character(form_fit)[2]
        data_sub <- df_feat_incorrect_merged %>%
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
      name = workflow_final_xgb_correct,
      command = {
        final_wf <-
          fit_best_tune_result(
            workflow_tune_xgb_correct_spatial,
            metric = "rmse"
          )
        attr(final_wf, "outcome") <- tune::outcome_names(workflow_tune_xgb_correct_spatial)
        attr(final_wf, "target_year") <- attr(workflow_tune_xgb_correct_spatial, "target_year")
        final_wf
      },
      pattern = map(workflow_tune_xgb_correct_spatial),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_01")
      )
    ),
    targets::tar_target(
      name = workflow_fit_xgb_correct,
      command = {
        predict_grid_with_matching_year_models(
          grid_data = df_feat_grid_merged,
          fitted_models = workflow_final_xgb_correct,
          chr_terms_x = chr_terms_x
        )
      },
      pattern = map(df_feat_grid_merged),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_04")
      )
    ),
    targets::tar_target(
      name = workflow_fit_incorrect,
      command = {
        yvar <- tune::outcome_names(workflow_tune_xgb_incorrect_spatial)

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
            workflow_tune_xgb_incorrect_spatial,
            metric = "rmse"
          ) %>%
          predict(
            .,
            df_combined
          )
        names(fitted) <- yvar
        fitted
      },
      pattern = map(workflow_tune_xgb_incorrect_spatial),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_08")
      )
    )

  )


list_tune_eval <- list(
  targets::tar_target(
    name = df_tune_correct_metrics,
    command = {
      df_metrics <- collect_tune_metrics_with_metadata(workflow_tune_xgb_correct_spatial)
      df_metrics
    },
    pattern = map(workflow_tune_xgb_correct_spatial),
    iteration = "list",
    resources = targets::tar_resources(
      crew = targets::tar_resources_crew(controller = "controller_01")
    )
  ),
  # Variable importance from the best model in the tuning results
  targets::tar_target(
    name = df_tune_correct_vip,
    command = {
      train_data <-
        extract_tune_training_template(workflow_tune_xgb_correct_spatial)

      names_variables <-
        extract_tune_training_variables(workflow_tune_xgb_correct_spatial)
      # remove the outcome variable
      names_target <- names_variables[length(names_variables)]
      names_variables <- names_variables[-length(names_variables)]
      names_variables


      df_train_fit <-
        workflow_tune_xgb_correct_spatial %>%
        fit_best_tune_result() %>%
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
      crew = targets::tar_resources_crew(controller = "controller_01")
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
