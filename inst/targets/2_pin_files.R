list_basefiles <-
  list(
    targets::tar_target(
      name = chr_monitors_file,
      command = file.path(chr_dir_git, "data/sites", "sites_history_cleaning_20250311.xlsx")
    ),
    targets::tar_target(
      name = chr_measurement_dir,
      # command = file.path("/home/felix", "Documents")
      command = file.path(chr_dir_data, "airquality", "outdoor")
    ),
    targets::tar_target(
      name = chr_measurement_file,
      command = file.path(chr_measurement_dir, "sites_airkorea_2010_2023_spt_yd.parquet"),
      format = "file"
    ),
    targets::tar_target(
      name = chr_landuse_file,
      command = file.path(chr_dir_data, "landcover", "lc_mcd12q1v061.p1_c_500m_s_20210101_20211231_go_epsg.4326_v20230818.tif"),
      format = "file"
    ),
    targets::tar_target(
      name = chr_dem_file,
      command = file.path(chr_dir_data, "elevation", "kngii_2022_merged_res30d.tif"),
      format = "file"
    ),
    targets::tar_target(
      name = chr_dsm_file,
      command = file.path(chr_dir_data, "elevation", "copernicus_korea_30m.tif"),
      format = "file"
    ),
    targets::tar_target(
      name = chr_road_files,
      command =
        list.files(
          file.path(chr_dir_data, "transportation", "nodelink", "data"),
          pattern = "MOCT_LINK.shp$",
          recursive = TRUE,
          full.names = TRUE
        )
    ),
    targets::tar_target(
      name = chr_asos_file,
      command = file.path(chr_dir_data, "weather", "data", "asos_2010_2023.parquet")
    ),
    targets::tar_target(
      name = chr_asos_site_file,
      command = file.path(chr_dir_data, "weather", "data", "asos_sites.xlsx")
    ),
    targets::tar_target(
      name = sf_korea_all,
      command = {
        geodata::geodata_path(file.path("~", "geodatacache"))
        geodata::gadm(
          country = "KOR",
          level = 0
        ) %>%
        sf::st_as_sf() %>%
        sf::st_transform("EPSG:5179")
      }
    ),
    targets::tar_target(
      name = chr_korea_watershed,
      command = {
        file.path(chr_dir_data, "watersheds", "data", "watershed-korea.gpkg")
      }
    ),
    targets::tar_target(
      name = chr_mtpi_file,
      command = file.path(chr_dir_data, "elevation", "kngii_90m_mtpi.tif")
    ),
    targets::tar_target(
      name = chr_file_emission_locs,
      command = file.path(chr_dir_data, "emission", "data", "emission_location.gpkg")
    )
  )