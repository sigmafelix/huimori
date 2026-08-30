list_process_site_daily <-
  list(
    targets::tar_target(
      name = sf_monitors_correct_daily,
      command = {
        daily_limit_nested_threads()
        build_sf_monitors_correct_daily(
          measurements = dt_measurements,
          site_history = sf_monitors_base,
          min_valid_hours = 18L
        )
      },
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = sf_monitors_correct_month,
      command = {
        daily_limit_nested_threads()
        subset_daily_monitor_month(
          sf_monitors_correct_daily,
          chr_months_spatial
        )
      },
      pattern = map(chr_months_spatial),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    )
  )


list_process_feature_daily <-
  list(
    targets::tar_target(
      name = rast_aod_daily,
      command = {
        build_aod_monthly_cube(
          month = chr_months_spatial,
          aod_dir = chr_dir_aod,
          output_root = daily_native_artifact_root()
        )
      },
      pattern = map(chr_months_spatial),
      iteration = "list",
      format = "file",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = rast_era5_daily,
      command = {
        build_era5_land_monthly_cube(
          month = chr_months_spatial,
          era5_dir = chr_dir_era5_land,
          output_root = daily_native_artifact_root()
        )
      },
      pattern = map(chr_months_spatial),
      iteration = "list",
      format = "file",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_40")
      )
    ),
    targets::tar_target(
      name = rast_blh_daily,
      command = {
        build_blh_monthly_cube(
          month = chr_months_spatial,
          era5_dir = chr_dir_era5_blh,
          output_root = daily_native_artifact_root()
        )
      },
      pattern = map(chr_months_spatial),
      iteration = "list",
      format = "file",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_correct_aod_daily,
      command = {
        extract_daily_source_points(
          base = sf_monitors_correct_month,
          cube_files = rast_aod_daily,
          source = "aod",
          id_cols = c("TMSID", "TMSID2")
        )
      },
      pattern = map(
        chr_months_spatial,
        sf_monitors_correct_month,
        rast_aod_daily
      ),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_correct_era5_land_daily,
      command = {
        extract_daily_source_points(
          base = sf_monitors_correct_month,
          cube_files = rast_era5_daily,
          source = "era5_land",
          id_cols = c("TMSID", "TMSID2")
        )
      },
      pattern = map(
        chr_months_spatial,
        sf_monitors_correct_month,
        rast_era5_daily
      ),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_40")
      )
    ),
    targets::tar_target(
      name = df_feat_correct_blh_daily,
      command = {
        extract_daily_source_points(
          base = sf_monitors_correct_month,
          cube_files = rast_blh_daily,
          source = "blh",
          id_cols = c("TMSID", "TMSID2")
        )
      },
      pattern = map(
        chr_months_spatial,
        sf_monitors_correct_month,
        rast_blh_daily
      ),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_correct_merged_daily,
      command = {
        merge_correct_daily_month(
          base = sf_monitors_correct_month,
          aod = df_feat_correct_aod_daily,
          era5_land = df_feat_correct_era5_land_daily,
          blh = df_feat_correct_blh_daily,
          month = chr_months_spatial
        )
      },
      pattern = map(
        chr_months_spatial,
        sf_monitors_correct_month,
        df_feat_correct_aod_daily,
        df_feat_correct_era5_land_daily,
        df_feat_correct_blh_daily
      ),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_40")
      )
    ),
    targets::tar_target(
      name = file_grid_raster_cell,
      command = {
        write_grid_daily_cell_map(
          grid = list_pred_calc_grid,
          source_specs = daily_cube_specs_from_branches(
            aod = rast_aod_daily,
            era5_land = rast_era5_daily,
            blh = rast_blh_daily
          ),
          output_root = daily_native_artifact_root()
        )
      },
      pattern = map(list_pred_calc_grid),
      iteration = "list",
      format = "file",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_aod_daily,
      command = {
        new_daily_grid_source_contract(
          cube_files = rast_aod_daily,
          source = "aod",
          month = chr_months_spatial
        )
      },
      pattern = map(chr_months_spatial, rast_aod_daily),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_era5_land_daily,
      command = {
        new_daily_grid_source_contract(
          cube_files = rast_era5_daily,
          source = "era5_land",
          month = chr_months_spatial
        )
      },
      pattern = map(chr_months_spatial, rast_era5_daily),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_40")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_blh_daily,
      command = {
        new_daily_grid_source_contract(
          cube_files = rast_blh_daily,
          source = "blh",
          month = chr_months_spatial
        )
      },
      pattern = map(chr_months_spatial, rast_blh_daily),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_merged_daily,
      command = {
        new_daily_grid_month_contract(
          cell_map_file = file_grid_raster_cell,
          aod = df_feat_grid_aod_daily,
          era5_land = df_feat_grid_era5_land_daily,
          blh = df_feat_grid_blh_daily
        )
      },
      pattern = cross(
        map(file_grid_raster_cell),
        map(
          chr_months_spatial,
          df_feat_grid_aod_daily,
          df_feat_grid_era5_land_daily,
          df_feat_grid_blh_daily
        )
      ),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_40")
      )
    )
  )
