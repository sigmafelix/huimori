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
      command = {
        glc_dir <- file.path(chr_dir_data, "landuse", "glc_fcs30d")
        list.files(glc_dir, pattern = "tif$", full.names = TRUE)
      },
      iteration = "list"
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
      name = chr_mtpi_1km_file,
      command = {
        mtpi <- terra::rast(chr_mtpi_file)
        mtpi_1km <- terra::aggregate(mtpi, fact = 11, fun = mean, na.rm = TRUE)
        out_file <- file.path(chr_dir_data, "elevation", "kngii_1km_mtpi.tif")
        terra::writeRaster(mtpi_1km, out_file, overwrite = TRUE)
        out_file
      },
      format = "file"
    ),
    targets::tar_target(
      name = chr_landuse_freq_file,
      command = {
        # 1km mask
        year <- stringi::stri_extract_first_regex(
          chr_landuse_file, pattern = "20[0-2][0-9]"
        )
        year <- as.integer(year)
        flt_mask <- make_binary_mask(67, 67)
        landuse_ras <- terra::rast(chr_landuse_file)
        landuse_freq <-
          huimori::rasterize_freq(
            ras = landuse_ras,
            mat = flt_mask
          )
        year_file_name <- sprintf("landuse_freq_glc_fcs30d_%d.tif", year)
        out_file <- file.path(chr_dir_data, "landuse", year_file_name)
        terra::writeRaster(landuse_freq, out_file, overwrite = TRUE)
        out_file
      },
      format = "file",
      iteration = "list",
      pattern = map(chr_landuse_file),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_01")
      )
    ),
    targets::tar_target(
      name = chr_file_emission_locs,
      command = file.path(chr_dir_data, "emission", "data", "emission_location.gpkg")
    ),
    targets::tar_target(
      name = chr_landuse_files,
      command = {
        list.files(
          file.path(chr_dir_data, "landuse", "glc_fcs30d"),
          pattern = ".tif$",
          full.names = TRUE
        )
      }
    )
  )