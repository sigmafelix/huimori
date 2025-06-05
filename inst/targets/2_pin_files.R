list_basefiles <-
  list(
    targets::tar_target(
      name = chr_monitors_file,
      command = file.path("/mnt/s", "Korea")
    ),
    targets::tar_target(
      name = chr_measurement_file,
      command = file.path("/mnt/s/", "Korea", "airquality", "outdoor", "korea_2010_2023_yd.parquet")
    ),
    targets::tar_target(
      name = chr_landuse_file,
      command = file.path(chr_dir_data, "landcover", "lc_mcd12q1v061.p1_c_500m_s_20210101_20211231_go_epsg.4326_v20230818.tif")
    ),
    targets::tar_target(
      name = chr_dem_file,
      command = file.path(chr_dir_data, "elevation", "kngii_2022_merged_res30d.tif")
    ),
    targets::tar_target(
      name = chr_dsm_file,
      command = file.path(chr_dir_data, "elevation", "copernicus_korea_30m.tif")
    ),
    targets::tar_target(
      name = chr_road_dir,
      command =
        list.files(
          file.path(chr_dir_data, "transportation", "nodelink", "data"),
          pattern = "MOCT_LINK.shp$",
          recursive = TRUE,
          full.names = TRUE
        )
    )
  )