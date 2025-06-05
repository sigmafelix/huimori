

list_tune_models <-
  list(
    targets::tar_target(
      name = workflow_tune,
      command = "workflow"
    )
  )