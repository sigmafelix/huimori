
list_process_site <-
  list(
    targets::tar_target(
      name = sf_monitors_correct,
      command = sf::st_as_sf(chr_monitors_file)
    ),
    targets::tar_target(
      name = sf_monitors_incorrect,
      command = sf::st_as_sf(chr_monitors_incorrect_file)
    ),
    targets::tar_target(
      name = dt_measurements,
      command = nanoparquet::read_parquet(chr_file_measurements)
    ),
    targets::tar_target(
      name = dt_asos,
      command = nanoparquet::read_parquet(chr_asos_file)
    )
  )

list_process_split <-
  list(
    targets::tar_target(
      name = int_grid_radii,
      command = c(50, 100, 250, 500),
      iteration = "list"
    ),
    targets::tar_target(
      name = int_size_split,
      command = c(20, 10, 5, 2),
      iteration = "list"
    ),
    targets::tar_target(
      name = sf_grid_radii,
      command = sf::st_make_grid(
        sf_monitors_correct,
        cellsize = int_grid_radii,
        what = "centers"
      ),
      iteration = "list",
      pattern = map(int_grid_radii)
    ),
    targets::tar_target(
      name = sf_grid_correct_split,
      command = chopin::par_pad_grid(
        input = sf_grid_radii,
        mode = "grid",
        nx = int_size_split,
        ny = int_size_split,
        padding = 10
      )$original,
      iteration = "list",
      pattern = map(sf_grid_radii, int_size_split)
    ),
    targets::tar_target(
      name = sf_grid_correct_set,
      command = sf_grid_radii[sf_grid_correct_split, ],
      iteration = "list",
      pattern = cross(sf_grid_radii, sf_grid_correct_split)
    )
  )



list_process_feature <-
  list(
    targets::tar_target(
      name = df_feat_correct_d_road,
      command = sf::st_nearest_feature(
        x = sf_monitors_correct,
        y = sf::st_read(chr_road_files, quiet = TRUE)
      )
    ),
    targets::tar_target(
      name = df_feat_correct_dsm,
      command = chopin::extract_at(
        x = chr_dsm_file,
        y = sf_monitors_correct,
        radius = 1e-6,
        force_df = TRUE
      )
    ),
    targets::tar_target(
      name = df_feat_correct_dem,
      command = chopin::extract_at(
        x = chr_dem_file,
        y = sf_monitors_correct,
        radius = 1e-6,
        force_df = TRUE
      )
    ),
    targets::tar_target(
      name = df_feat_correct_landuse,
      command = {
        landuse_ras <- terra::rast(chr_landuse_file)
        flt7 <-
          matrix(
            c(0, 0, 1, 1, 1, 0, 0,
              0, 1, 1, 1, 1, 1, 0,
              1, 1, 1, 1, 1, 1, 1,
              1, 1, 1, 1, 1, 1, 1,
              1, 1, 1, 1, 1, 1, 1,
              0, 1, 1, 1, 1, 1, 0,
              0, 0, 1, 1, 1, 0, 0),
            nrow = 7, ncol = 7, byrow = TRUE
          )
        landuse_freq <-
          huimori::rasterize_freq(
            ras = landuse_ras,
            mat = flt7
          )
        chopin::extract_at(
          x = landuse_freq,
          y = sf_monitors_correct,
          radius = 1e-6,
          force_df = TRUE
        )
      }
    ),
    targets::tar_target(
      name = df_feat_grid_d_road,
      command = sf::st_nearest_feature(
        x = sf_monitors_correct,
        y = sf::st_read(chr_road_files, quiet = TRUE)
      )
    ),
    targets::tar_target(
      name = df_feat_grid_dsm,
      command = chopin::extract_at(
        x = chr_dsm_file,
        y = sf_monitors_correct,
        radius = 1e-6,
        force_df = TRUE
      )
    ),
    targets::tar_target(
      name = df_feat_grid_dem,
      command = chopin::extract_at(
        x = chr_dem_file,
        y = sf_monitors_correct,
        radius = 1e-6,
        force_df = TRUE
      )
    ),
    targets::tar_target(
      name = df_feat_grid_landuse,
      command = {
        landuse_ras <- terra::rast(chr_landuse_file)
        flt7 <-
          matrix(
            c(0, 0, 1, 1, 1, 0, 0,
              0, 1, 1, 1, 1, 1, 0,
              1, 1, 1, 1, 1, 1, 1,
              1, 1, 1, 1, 1, 1, 1,
              1, 1, 1, 1, 1, 1, 1,
              0, 1, 1, 1, 1, 1, 0,
              0, 0, 1, 1, 1, 0, 0),
            nrow = 7, ncol = 7, byrow = TRUE
          )
        landuse_freq <-
          huimori::rasterize_freq(
            ras = landuse_ras,
            mat = flt7
          )
        chopin::extract_at(
          x = landuse_freq,
          y = sf_grid_correct_set,
          radius = 1e-6,
          force_df = TRUE
        )
      }
    )
  )