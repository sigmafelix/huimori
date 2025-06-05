

list_fit_models <-
  list(
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
      command = return("workflow")
    )
  )