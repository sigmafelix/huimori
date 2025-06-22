

list_fit_models <-
  list(
    targets::tar_target(
      name = chr_terms_x,
      command = {
        c(

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
      name = workflow_tune,
      command = {
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
              grid_space_filling(
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
                metric_set(rmse, rsq),
                control = control_race(verbose = TRUE)
              )

            return(xgb_res)
          }
        fitted_xgb_model <-
          fit_tidy_xgb(
            data = df_feat_correct_merged,
            formula = form_fit,
            invars = chr_terms_x
          )
      },
      pattern = map(form_fit),
      iteration = "list"
    )
  )