
list_process_site <-
  list(
    targets::tar_target(
      name = sf_monitors,
      command = sf::st_as_sf(chr_monitors_file)
    ),
    targets::tar_target(
      name = dt_measurements,
      command = nanoparquet::read_parquet(chr_file_measurements)
    )
  )



list_process_feature <-
  list(
    targets::tar_target(
      name = df_feat_distance_road,
      command = terra::distance()
    ),
    targets::tar_target(
      name = df_feat_dsm,
      command = chopin::extract_at(
        x = chr_dsm_file,
        y = sf_monitors,
        radius = 1e-6
      )
    ),
    targets::tar_target(
      name = df_feat_dem,
      command = chopin::extract_at(
        x = chr_dem_file,
        y = sf_monitors,
        radius = 1e-6
      )
    ),
    targets::tar_target(
      name = df_feat_landuse,
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
          y = sf_monitors,
          radius = 1e-6
        )
      }
    )

  )