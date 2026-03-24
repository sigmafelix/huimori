list_basefiles <-
  list(
    ## B01. Monitor table ####
    targets::tar_target(
      name = chr_monitors_file,
      command = file.path(chr_dir_git, "data/sites", "sites_history_cleaning_20250311.xlsx")
    ),
    ## B02. Measurement data ####
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
    ## B03. Landuse data ####
    targets::tar_target(
      name = chr_landuse_files,
      command = {
        glc_dir <- file.path(chr_dir_data, "landuse", "glc_fcs30d")
        yrs <- unlist(int_years_spatial)
        yrs <- yrs - 1L
        yrs_pattern <- paste0(yrs, collapse = "|")
        list.files(glc_dir, pattern = paste0("(", yrs_pattern, ")", "\\.tif$"), full.names = TRUE)
      },
      iteration = "list"
    ),
    ## B04. DEM data ####
    targets::tar_target(
      name = chr_dem_file,
      command = file.path(chr_dir_data, "elevation", "kngii_2022_merged_res30d.tif"),
      format = "file"
    ),
    ## B05. DSM data ####
    targets::tar_target(
      name = chr_dsm_file,
      command = file.path(chr_dir_data, "elevation", "copernicus_korea_30m.tif"),
      format = "file"
    ),
    ## B06. Road network data (node-link data from KTDB) ####
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
    ## B07. ASOS weather data ####
    targets::tar_target(
      name = chr_asos_file,
      command = file.path(chr_dir_data, "weather", "data", "asos_2010_2023.parquet")
    ),
    targets::tar_target(
      name = chr_asos_site_file,
      command = file.path(chr_dir_data, "weather", "data", "asos_sites.xlsx")
    ),
    ## B08. Boundary Data ####
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
    ## B09. Watershed ####
    targets::tar_target(
      name = chr_korea_watershed,
      command = {
        file.path(chr_dir_data, "watersheds", "data", "watershed-korea.gpkg")
      }
    ),
    ## B10. MTPI (multiscale terrain position index) ####
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
    ## B10-1. Preprocessed landuse fraction files ####
    targets::tar_target(
      name = chr_landuse_freq_file,
      command = {
        # 1km mask
        year <- stringi::stri_extract_first_regex(
          chr_landuse_files, pattern = "20[0-2][0-9]"
        )
        year <- as.integer(year)
        year_file_name <- sprintf("glc_freq_%d.tif", year)
        file.path(chr_dir_data, "landuse", year_file_name)
      },
      iteration = "list",
      pattern = map(chr_landuse_files),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_01")
      ),
      cue = tar_cue(mode = "never")
    ),
    ## B11. Emission locations ####
    targets::tar_target(
      name = chr_file_emission_locs,
      command = file.path(chr_dir_data, "emission", "data", "emission_location.gpkg")
    ),
    targets::tar_target(
      name = chr_dir_aod,
      command = {
        file.path(chr_dir_data, "airquality", "aod")
      }
    )
  )


# ----------------------------------------------------------------
# 변경 log 기록(dhnyu)
## 2026.01.31

### DAG 상에서 최종 객체와 직접적으로 이어지지 않는 target 체크
#### chr_asos_file, chr_asos_site_file, chr_landuse_files






