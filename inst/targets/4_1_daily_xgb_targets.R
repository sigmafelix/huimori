# Resolve selected *names* against the current upstream branch catalog. The
# resulting slice edges are real targets dependencies, not hidden tar_read calls.
list_fit_models_daily <- local({
  audit_root <- "daehoon/data/daily_xgb_targets_audit/run=preflight_004"
  selection_path <- file.path(audit_root, "grid_contracts_2023.csv")
  design <- daily_2023_selection()
  selection <- design$grid
  registry_path <- "daehoon/data/daily_xgb_gate_d/year=2023/month=01/run=all593_parallel8x5_v3_001/registry.csv"
  registry <- design$registry
  baseline <- "daehoon/data/daily_xgb_2023_baseline/run=original_fixed300_001"
  monitor <- list(branches = design$monitor_branches)
  metadata <- daily_2023_definition_metadata()
  indices <- function(parent, names) daily_2023_select_indices(parent, names, design, metadata)
  pattern <- function(parent, names) substitute(slice(PARENT, index = INDEX),
    list(PARENT = as.name(parent), INDEX = indices(parent, names)))
  target <- function(name, command, pattern = NULL, format = "qs", iteration = "list", controller = "daily_fit_01") {
    targets::tar_target_raw(name, command, pattern = pattern, format = format, iteration = iteration,
      error = "stop", resources = targets::tar_resources(crew = targets::tar_resources_crew(controller = controller)))
  }
  list(
    target("file_daily_2023_selection", quote("inst/config/daily_xgb_2023_selection.json"), format = "file"),
    target("file_daily_2023_raster_implementation", quote(c("inst/python/daily_targets_raster.py",
      "daehoon/scripts/daily_gate_e_core.py", "daehoon/scripts/audit_daily_gate_e.py")), format = "file"),
    target("file_daily_2023_authority", substitute({
      daily_2023_assert_selection_runtime(daily_2023_selection(file_daily_2023_selection), daily_2023_definition_metadata())
      c(SELECTION, REGISTRY,
      "daehoon/data/daily_xgb_national_smoke/year=2023/month=01/run=january_gate_a_nanfix_w40_001/result.json",
      "daehoon/data/daily_xgb_national_smoke/year=2023/month=01/run=january_gate_a_nanfix_w40_001/normalization_by_branch.csv",
      "daehoon/data/daily_xgb_gate_d/year=2023/month=01/run=all593_parallel8x5_v3_001/result.json",
      "daehoon/data/daily_xgb_gate_e/year=2023/month=01/run=national_cog_002/result.json")},
      list(SELECTION = selection_path, REGISTRY = registry_path)), format = "file"),
    target("file_daily_2023_reference_models", substitute(file.path(BASE,
      paste0(c("PM10", "PM25"), "_ORIGINAL_DAILY_2023_BASELINE_RETROSPECTIVE_NONPRODUCTION.rds")),
      list(BASE = baseline)), format = "file"),
    target("file_daily_2023_folds", substitute(file.path(BASE, "fold_assignments.parquet"),
      list(BASE = original_baseline_root())), format = "file"),
    target("file_daily_2023_windows", quote("daehoon/data/daily_xgb_gate_e/year=2023/month=01/run=national_cog_002/windows.csv"), format = "file"),
    target("file_daily_2023_configuration", quote(daily_2023_config(
      "daehoon/data/daily_xgb_targets/year=2023/run=original_fixed300_001", file_daily_2023_authority)), format = "file"),
    target("df_daily_2023_annual_monitor", quote(df_feat_correct_merged),
      pattern("df_feat_correct_merged", "df_feat_correct_merged_46aa21fcdccfd1f3")),
    target("df_daily_2023_monitor_month", quote(daily_2023_monitor_month(df_feat_correct_merged_daily)),
      pattern("df_feat_correct_merged_daily", monitor$branches[startsWith(monitor$branches, "df_feat_correct_merged_daily_")])),
    target("df_feat_correct_daily_model_2023", quote(daily_2023_training(
      df_daily_2023_annual_monitor, df_daily_2023_monitor_month))),
    target("workflow_fit_xgb_correct_daily_spatial", quote(daily_2023_spatial_fit(
      df_feat_correct_daily_model_2023, file_daily_2023_configuration,
      file_daily_2023_reference_models, file_daily_2023_folds)), format = "file"),
    target("workflow_final_xgb_correct_daily", quote(daily_2023_full_fit(
      df_feat_correct_daily_model_2023, workflow_fit_xgb_correct_daily_spatial,
      file_daily_2023_configuration, file_daily_2023_reference_models)), format = "file"),
    target("df_daily_2023_static_descriptor", substitute(daily_2023_static_descriptor(
      df_feat_grid_merged, file_daily_2023_authority[basename(file_daily_2023_authority) == "registry.csv"],
      file_daily_2023_authority[basename(file_daily_2023_authority) == "normalization_by_branch.csv"])),
      pattern("df_feat_grid_merged", registry$annual_branch), controller = "daily_predict_08"),
    target("list_daily_2023_static_catalog", quote(daily_2023_static_catalog(df_daily_2023_static_descriptor))),
    target("contract_daily_2023_january", quote(daily_2023_contract(df_feat_grid_merged_daily)),
      pattern("df_feat_grid_merged_daily", selection$branch[selection$month == "2023-01"])),
    target("contract_daily_2023_remaining", quote(daily_2023_contract(df_feat_grid_merged_daily)),
      pattern("df_feat_grid_merged_daily", selection$branch[selection$month != "2023-01"])),
    target("workflow_fit_xgb_correct_daily_january", quote(daily_2023_predict_month(
      contract_daily_2023_january, list_daily_2023_static_catalog, workflow_final_xgb_correct_daily,
      file_daily_2023_configuration)), quote(map(contract_daily_2023_january)), format = "file", controller = "daily_predict_08"),
    target("file_daily_2023_prediction_reuse", quote({
      plan <- daily_2023_resources_v3(file_daily_2023_resource_plan_v3)
      plan$reuse_registry
    }), format = "file"),
    target("list_daily_2023_prediction_reuse", quote(unname(readRDS(file_daily_2023_prediction_reuse)))),
    target("daily_2023_prediction_reuse_check", quote(daily_2023_check_reuse_target(
      list_daily_2023_prediction_reuse, contract_daily_2023_remaining, list_daily_2023_static_catalog,
      workflow_final_xgb_correct_daily, file_daily_2023_configuration)),
      quote(map(list_daily_2023_prediction_reuse)), format = "file", controller = "daily_predict_20"),
    target("daily_2023_prediction_reuse_gate", quote(daily_2023_reuse_gate(
      daily_2023_prediction_reuse_check, file_daily_2023_prediction_reuse, file_daily_2023_configuration)), format = "file"),
    target("workflow_fit_xgb_correct_daily_remaining", quote(daily_2023_predict_month_v3(
      contract_daily_2023_remaining, list_daily_2023_static_catalog, workflow_final_xgb_correct_daily,
      file_daily_2023_configuration, daily_2023_january_equivalence,
      file_daily_2023_prediction_reuse, daily_2023_prediction_reuse_gate)),
      quote(map(contract_daily_2023_remaining)), format = "file", controller = "daily_predict_20"),
    target("workflow_fit_xgb_correct_daily", quote(daily_2023_prediction_catalog(
      workflow_fit_xgb_correct_daily_january, workflow_fit_xgb_correct_daily_remaining)))
  )
})

list_export_prediction_maps_daily <- local({
  target <- function(name, command, pattern = NULL, format = "file", iteration = "list", controller = "daily_raster_40") {
    targets::tar_target_raw(name, command, pattern = pattern, format = format, iteration = iteration,
      error = "stop", resources = targets::tar_resources(crew = targets::tar_resources_crew(controller = controller)))
  }
  list(
    target("file_daily_2023_resource_plan", quote("inst/config/daily_xgb_2023_resources_v2.json")),
    target("file_daily_2023_resource_plan_v3", quote("inst/config/daily_xgb_2023_resources_v3.json")),
    target("file_daily_2023_raster_implementation_v3", quote(c("inst/python/daily_targets_raster_v3.py",
      "inst/python/daily_targets_raster.py", "daehoon/scripts/daily_gate_e_core.py", "daehoon/scripts/audit_daily_gate_e.py"))),
    target("file_daily_2023_review_implementation_v3", quote(c("inst/python/daily_targets_review_v3.py",
      "R/daily_targets_export_v3.R"))),
    target("file_daily_2023_review_implementation", quote(c("inst/python/daily_targets_review.py",
      "R/daily_targets_review_parallel.R"))),
    target("chr_daily_2023_january_dates", quote(as.character(daily_2023_dates("2023-01"))), format = "qs", iteration = "vector"),
    target("chr_daily_2023_remaining_dates", quote(as.character(daily_2023_dates())[32:365]), format = "qs", iteration = "vector"),
    target("daily_2023_january_raster", quote(daily_2023_raster_date(workflow_fit_xgb_correct_daily_january,
      chr_daily_2023_january_dates, file_daily_2023_configuration, file_daily_2023_windows,
      file_daily_2023_raster_implementation)), quote(map(chr_daily_2023_january_dates))),
    target("daily_2023_january_review_catalog", quote(daily_2023_review_catalog(daily_2023_january_raster,
      january = TRUE)), format = "qs"),
    target("daily_2023_january_review", quote(daily_2023_review_parallel(daily_2023_january_review_catalog,
      file_daily_2023_windows, file_daily_2023_raster_implementation, file_daily_2023_review_implementation)),
      quote(map(daily_2023_january_review_catalog)), controller = "daily_review_40"),
    target("daily_2023_january_equivalence", quote(daily_2023_january_gate(daily_2023_january_review,
      file_daily_2023_configuration))),
    target("daily_2023_remaining_raster", quote(daily_2023_raster_parallel_v3(workflow_fit_xgb_correct_daily,
      chr_daily_2023_remaining_dates, file_daily_2023_configuration, file_daily_2023_windows,
      file_daily_2023_raster_implementation_v3, file_daily_2023_resource_plan_v3)), quote(map(chr_daily_2023_remaining_dates))),
    target("daily_2023_remaining_review_catalog", quote(daily_2023_review_catalog(daily_2023_remaining_raster)), format = "qs"),
    target("daily_2023_remaining_review", quote(daily_2023_review_parallel_v3(daily_2023_remaining_review_catalog,
      file_daily_2023_windows, file_daily_2023_raster_implementation_v3, file_daily_2023_review_implementation_v3)),
      quote(map(daily_2023_remaining_review_catalog)), controller = "daily_review_40"),
    target("workflow_fit_xgb_correct_daily_raster", quote(daily_2023_all_rasters(
      daily_2023_january_raster, daily_2023_remaining_raster))),
    target("workflow_fit_xgb_correct_daily_map_validation", quote(daily_2023_final_validation(
      workflow_fit_xgb_correct_daily_raster, daily_2023_january_review,
      daily_2023_remaining_review, file_daily_2023_configuration)))
  )
})
