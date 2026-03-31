
list_process_site <-
  list(
    targets::tar_target(
      name = dt_measurements,
      command = {
        dt <- data.table::as.data.table(nanoparquet::read_parquet(chr_measurement_file))
        # 시간 밀림 보정 (9시간 가산)
        dt[, datehour := datehour + lubridate::hours(9)]
        dt[, date := data.table::as.IDate(date + lubridate::hours(9))]
        # 음수값(-999)은 일괄적으로 결측치(NA) 처리
        cols_to_fix <- c("SO2", "CO", "O3", "NO2", "PM10", "PM25")
        dt[, (cols_to_fix) := lapply(.SD, function(x) ifelse(x < 0, NA, x)), 
           .SDcols = cols_to_fix]
        dt
      }
    ),
    targets::tar_target(
      name = sf_monitors_base,
      command = {
        sites <- readxl::read_excel(
          chr_monitors_file
        )

        # another approach: sites_c
        sites_c <- sites |>
          dplyr::filter(!grepl("(광화학|중금속|산성|유해)", site_type)) |>
          dplyr::arrange(TMSID, site_type, year) |>
          dplyr::filter(!grepl("\\-[1-9]$", TMSID)) |>
          dplyr::ungroup() |>
          # pre-cleaning: detect max year
          dplyr::group_by(TMSID, site_type) |>
          dplyr::mutate(year_max = max(year)) |>
          dplyr::ungroup() |>
          # distict rows by selected fields
          # .keep_all will keep the first row in each group
          dplyr::distinct(
            TMSID, date_start, date_end, coords_google, floor,
            .keep_all = TRUE
          ) |>
          dplyr::mutate(
            date_start = as.POSIXct(date_start, tz = "Asia/Seoul"),
            date_end = as.POSIXct(date_end, tz = "Asia/Seoul")
          ) |>
          dplyr::group_by(TMSID) |>
          # Assign first and last row
          dplyr::mutate(
            date_start =
            replace(date_start, dplyr::row_number() == 1, unique(fill_date(date_start, min(year), TRUE))),
            date_end =
            replace(date_end, dplyr::row_number() == dplyr::n(), unique(fill_date(date_end, max(year_max), start = FALSE)))
          ) |>
          # if no location changes were detected and date_start and date_end are NAs,
          # use the minimum year to assign date_start and
          # the maximum year to assign date_end, respectively.
          dplyr::mutate(
            date_start = ifelse(dplyr::n() == 1 & is.na(date_start), as.POSIXct(sprintf("%d0101010000", year), format = "%Y%m%d%H%M%S", tz = "Asia/Seoul"), date_start),
            date_end = ifelse(dplyr::n() == 1 & is.na(date_end), as.POSIXct(sprintf("%d1231230000", year_max), format = "%Y%m%d%H%M%S", tz = "Asia/Seoul"), date_end)
          ) |>
          dplyr::filter(!(is.na(date_start) & is.na(date_end))) |>
          dplyr::mutate(
            #date_start = ifelse(is.na(date_start), dplyr::lead(date_end), date_start)#,
            date_end = ifelse(is.na(date_end), dplyr::lead(date_end), date_end)
          ) |>
          dplyr::ungroup() |>
          dplyr::filter(!is.na(date_start)) |>
          dplyr::mutate(
            date_start = as.POSIXct(date_start, tz = "Asia/Seoul"),
            date_end = as.POSIXct(date_end, tz = "Asia/Seoul")
          )

        check_lookup <-
          c("[도시대기측정망]", "[도로변대기측정망]", "[PM2.5성분측정망]", "[교외대기측정망]",
            "[항만측정망]", "[국가배경농도(도서)측정망]", "[대기오염집중측정망]")
        target_lookup <-
          c("Urban", "Roadside", "PM2.5", "Suburban",
            "Port", "Island", "Concentrated")

        # Grand lookup table for temporal join
        sites_ch <- sites_c |>
          dplyr::select(TMSID, site_type, dplyr::starts_with("date_"), dplyr::starts_with("coords_google")) |>
          dplyr::distinct() |>
          dplyr::rowwise() |>
          dplyr::mutate(
            lon = as.numeric(stringi::stri_split_fixed(coords_google, pattern = ", ")[[1]][2]),
            lat = as.numeric(stringi::stri_split_fixed(coords_google, pattern = ", ")[[1]][1])
          ) |>
          dplyr::ungroup() |>
          dplyr::mutate(
            site_type = sub(" ", "", site_type),
            site_type = plyr::mapvalues(site_type, check_lookup, target_lookup),
            site_type = factor(site_type, levels = target_lookup[c(1, 2, 4, 5, 3, 6, 7)])
          ) |>
          dplyr::group_by(TMSID) |>
          dplyr::mutate(TMSID2 = paste0(TMSID, LETTERS[seq_len(length(TMSID))])) |>
          dplyr::ungroup()

        sites_ch
      }
    ),
    # annualize the monitor data (correct coordinates)
    targets::tar_target(
      name = sf_monitors_correct,
      command = {

        ak_sites_annual <- huimori::summarize_annual(
          data = dt_measurements,
          timeflag = "date"
        )

        # relocation distance
        sites_cfd <-
          sf_monitors_base |>
          dplyr::arrange(TMSID, date_start) |>
          dplyr::group_by(TMSID) |>
          dplyr::mutate(lon2 = lag(lon), lat2 = lag(lat)) |>
          dplyr::rowwise() |>
          # dplyr::ungroup() |>
          # dplyr::group_by(TMSID) |>
          dplyr::mutate(dist_m = geosphere::distGeo(c(lon, lat), c(lon2, lat2))) |>
          dplyr::ungroup()


        # Unique space-time by years
        sites_fullrange <- sites_cfd %>%
          dplyr::group_by(TMSID, TMSID2) %>%
          dplyr::filter(!is.na(date_start) & !is.na(date_end)) %>%
          tidyr::nest() %>%
          dplyr::mutate(
            year_all = purrr::map(data, function(df) {
              # compute year_start, year_end from df
              ystart <- lubridate::year(df$date_start)
              yend   <- lubridate::year(df$date_end)
              data.frame(year = seq(ystart, yend))
            })
          ) %>%
          tidyr::unnest(c(year_all, data)) %>%
          dplyr::ungroup()

        ##   weight by lengths of each location for annual mean
        sites_sf <-
          sites_fullrange |>
          dplyr::filter(!is.na(lon)) |>
          st_as_sf(
            coords = c("lon", "lat"),
            crs = 4326
          ) |>
          st_transform(5179) |>
          dplyr::full_join(
            ak_sites_annual,
            by = c("TMSID", "TMSID2", "year")
          ) |>
          dplyr::filter(!sf::st_is_empty(geometry))
        sites_sf

      }
    ),
    # sf_monitors_correct branched by year (subset by year)
    targets::tar_target(
      name = sf_monitors_correct_yr,
      command = {
        sf_monitors_correct |>
          dplyr::filter(year == int_years_spatial)
      },
      pattern = map(int_years_spatial),
      iteration = "list"
    ),
    # full spacetime data frame for unique TMSID-date combinations
    targets::tar_target(
      name = sf_monitors_correct_full,
      command = {
        sfm_corr <- sf_monitors_correct |>
          dplyr::filter(date_start <= date_end)
        extend_grid(data = sfm_corr)
      }
    ),
    targets::tar_target(
      name = sf_monitors_incorrect,
      command = {

        path <- file.path(
          chr_dir_git, "data/sites",
          "station_original_cleaned_20250221.rds"
        )
        sites_orig <- readRDS(path)
        sites_orig_lean <-
          sites_orig |>
          dplyr::filter(grepl("(도시|종합|도로|교외|항만|배경|도서)", site_type)) |>
          dplyr::select(
            TMSID, year, date_start, date_end, site_type,
            longitude, latitude, longitude_common, latitude_common
          )

        ak_sites_annual <- huimori::summarize_annual(
          data = dt_measurements,
          timeflag = "date"
        )


        sites_orig_lean2 <- sites_orig_lean |>
          dplyr::mutate(
            longitude = ifelse(longitude == "", NA, longitude),
            latitude = ifelse(latitude == "", NA, latitude)
          ) |>
          # collapse::na_locf(set = TRUE) |>
          dplyr::transmute(
            TMSID = TMSID,
            year = year,
            lono = as.numeric(longitude),
            lato = as.numeric(latitude)
          ) |>
          dplyr::group_by(TMSID) |>
          dplyr::mutate(
            lono = ifelse(is.na(lono), lono[which(!is.na(lono))][1], lono),
            lato = ifelse(is.na(lato), lato[which(!is.na(lato))][1], lato)
          ) |>
          dplyr::ungroup()

        # as-is
        sites_asis <-
          sf_monitors_base |>
          rectify_year(fieldname = "date_start") |>
          # dplyr::filter(
          #   year_rect <= 2020
          # ) |>
          dplyr::group_by(TMSID) |>
          tidyr::nest() |>
          dplyr::mutate(data = purrr::map(data, function(df) df[nrow(df), ])) |>
          tidyr::unnest(data) |>
          dplyr::ungroup() |>
          dplyr::inner_join(y = sites_orig_lean2, by = "TMSID") |>
          dplyr::rowwise() |>
          dplyr::mutate(
            dist_m =
            geosphere::distGeo(c(lon, lat), c(lono, lato))
          ) |>
          dplyr::ungroup()

        # as-is
        sites_asis_sf <-
          sites_asis |>
          dplyr::filter(!is.na(lono) & !is.na(lato)) |>
          st_as_sf(
            coords = c("lono", "lato"),
            crs = 4326
          ) |>
          st_transform(5179) |>
          dplyr::left_join(
            ak_sites_annual,
            by = c("TMSID", "TMSID2", "year")
          )

        sites_asis_sf
      }
    ),
    targets::tar_target(
      name = dt_asos,
      command = nanoparquet::read_parquet(chr_asos_file)
    ),
    # targets::tar_target(
    #   name = df_feat_correct_wind_daily,
    #   command = {
    #     points_use <- sf_monitors_correct |>
    #       dplyr::filter(!sf::st_is_empty(geometry))
    #     start_date <- min(dt_measurements$date)
    #     end_date <- max(dt_measurements$date)

    #     gee_extract_daily_wind(
    #       points_sf = points_use,
    #       start_date = start_date,
    #       end_date = end_date,
    #       buffer_m = 250,
    #       scale = 1000
    #     )
    #   }
    # ),
    # targets::tar_target(
    #   name = df_feat_correct_wind_annual,
    #   command = {
    #     df_feat_correct_wind_daily |>
    #       dplyr::mutate(year = lubridate::year(date)) |>
    #       dplyr::group_by(TMSID, TMSID2, year) |>
    #       dplyr::summarize(
    #         wind_speed_10m = mean(wind_speed_10m, na.rm = TRUE),
    #         wind_dir_deg = mean(wind_dir_deg, na.rm = TRUE),
    #         .groups = "drop"
    #       )
    #   }
    # ),
    # targets::tar_target(
    #   name = df_feat_correct_building_density,
    #   command = {
    #     points_use <- sf_monitors_correct |>
    #       dplyr::filter(!sf::st_is_empty(geometry))
    #     yrs <- sort(unique(points_use$year))

    #     gee_extract_building_density(
    #       points_sf = points_use,
    #       years = yrs,
    #       buffer_m = 100,
    #       scale = 30
    #     )
    #   }
    # ),
    targets::tar_target(
      name = df_feat_incorrect_wind_daily,
      command = {
        points_use <- sf_monitors_incorrect |>
          dplyr::filter(!sf::st_is_empty(geometry))
        start_date <- min(dt_measurements$date)
        end_date <- max(dt_measurements$date)

        gee_extract_daily_wind(
          points_sf = points_use,
          start_date = start_date,
          end_date = end_date,
          buffer_m = 250,
          scale = 1000
        )
      }
    ),
    targets::tar_target(
      name = df_feat_incorrect_wind_annual,
      command = {
        df_feat_incorrect_wind_daily |>
          dplyr::mutate(year = lubridate::year(date)) |>
          dplyr::group_by(TMSID, TMSID2, year) |>
          dplyr::summarize(
            wind_speed_10m = mean(wind_speed_10m, na.rm = TRUE),
            wind_dir_deg = mean(wind_dir_deg, na.rm = TRUE),
            .groups = "drop"
          )
      }
    ),
    targets::tar_target(
      name = df_feat_incorrect_building_density,
      command = {
        points_use <- sf_monitors_incorrect |>
          dplyr::filter(!sf::st_is_empty(geometry))
        yrs <- sort(unique(points_use$year))

        gee_extract_building_density(
          points_sf = points_use,
          years = yrs,
          buffer_m = 100,
          scale = 30
        )
      }
    ),
    targets::tar_target(
      name = ras_landuse_freq,
      command = {
        # the last file should be fixed when years are branched
        landuse_ras <-
          terra::rast(chr_landuse_files[length(chr_landuse_files)], win = c(124, 132.5, 33, 38.6))

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
          ras_res <-
            huimori::rasterize_freq(
              ras = landuse_ras,
              mat = flt7
            )
          terra::writeRaster(
            x = ras_res,
            filename = file.path(chr_dir_data, "landuse_freq_glc_fcs30d_2022.tif"),
            overwrite = TRUE
          )
          TRUE
      }
    )
  )


## Grid processing for prediction ####
list_process_split <-
  list(
    targets::tar_target(
      name = int_grid_size,
      command = c(30L),#, 100, 250, 500),
      iteration = "list"
    ),
    targets::tar_target(
      name = int_size_split,
      command = c(70L),#, 20L, 10L, 2L),
      iteration = "list"
    ),
    targets::tar_target(
      name = sf_grid_size,
      command = {
        korea_poly <- sf_korea_all |>
          sf::st_simplify(preserveTopology = TRUE, dTolerance = 100) |>
          terra::vect()

        korea_grid <- auto_grid(
          x = korea_poly,
          grid_size = int_grid_size,
          chunks = 40L
        )
        sf::st_as_sf(korea_grid)
      }
      ,
      pattern = map(int_grid_size),
      iteration = "vector"
    ),
    targets::tar_target(
      name = sf_grid_size_group,
      command = {
        sf_grid_size %>%
          dplyr::group_by(CGRIDID) %>%
          tar_group()
      },
      iteration = "group"
    ),
    targets::tar_target(
      name = list_pred_calc_grid,
      command = {
        kor_ext <- floor(terra::ext(sf_grid_size_group))
        ras_template <- terra::rast(kor_ext, resolution = int_grid_size)
        ras_pad <- rasterize(sf_korea_all, ras_template)
        vec_ras <- terra::as.data.frame(ras_pad, xy = TRUE)
        vec_ras$gid <- seq_len(nrow(vec_ras))
        sf::st_as_sf(
          vec_ras,
          coords = c("x", "y"),
          crs = 5179,
          remove = FALSE
        )
      },
      pattern = cross(map(sf_grid_size_group), int_grid_size),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    # Branched data for correct coordinates
    targets::tar_target(
      name = sf_grid_correct_split,
      command = {
        grid_org <-
          chopin::par_pad_grid(
            input = sf_korea_all,
            mode = "grid",
            nx = int_size_split,
            ny = int_size_split,
            padding = 10
          )$original
        grid_org[sf_korea_all, ]
      }
    ),
    targets::tar_target(
      name = int_split_grid_ids,
      command = seq_len(nrow(sf_grid_correct_split)),
      iteration = "list"
    )
  )


## Annualized feature calculation ####
list_process_feature <-
  list(
    ### F01. Distance to the nearest road ####
    targets::tar_target(
      name = df_feat_correct_d_road,
      command = {
        road <- sf::st_read(chr_road_files[length(chr_road_files)], quiet = TRUE)
        road <- sf::st_transform(road, sf::st_crs(sf_monitors_correct_yr))
        road <- road %>%
          dplyr::filter(!ROAD_TYPE %in% c("002", "004") & ROAD_USE == 0)
        nearest_idx <- sf::st_nearest_feature(
          x = sf_monitors_correct_yr,
          y = road
        )
        road_nearest <- road[nearest_idx, ]
        dist_road_nearest <-
          sf::st_distance(
            x = sf_monitors_correct_yr,
            y = road_nearest,
            by_element = TRUE
          )
        sf_monitors_dist_att <-
          sf_monitors_correct_yr |>
          dplyr::select(
            TMSID, TMSID2, year
          ) |>
          dplyr::mutate(
            d_road = dist_road_nearest
          ) |>
          sf::st_drop_geometry()
        sf_monitors_dist_att
      },
      pattern = map(sf_monitors_correct_yr)
    ),
    ### F02. DSM (surface elevation) ####
    targets::tar_target(
      name = df_feat_correct_dsm,
      command = {
        chopin::extract_at(
          x = chr_dsm_file,
          y = sf_monitors_correct_yr,
          radius = 1e-6,
          id = c("TMSID", "TMSID2", "year"),
          force_df = TRUE
        ) |>
          dplyr::rename(dsm = mean)
      },
      pattern = map(sf_monitors_correct_yr)
    ),
    ### F03. DEM (ground elevation) ####
    targets::tar_target(
      name = df_feat_correct_dem,
      command = {
        chopin::extract_at(
          x = chr_dem_file,
          y = sf_monitors_correct_yr,
          radius = 1e-6,
          id = c("TMSID", "TMSID2", "year"),
          force_df = TRUE
        ) |>
          dplyr::rename(dem = mean)
      },
      pattern = map(sf_monitors_correct_yr)
    ),
    ### F04. Land use fractions ####
    targets::tar_target(
      name = int_landuse_radius,
      command = c(30, 100, 500, 2000),
      iteration = "list"
    ),
    targets::tar_target(
      name = df_feat_correct_landuse,
      command = {
        # landuse_ras <-
        #   terra::rast(
        #     chr_landuse_files,
        #     win = c(124, 132.5, 33, 38.6)
        #   )
        # flt7 <-
        #   matrix(
        #     c(0, 0, 1, 1, 1, 0, 0,
        #       0, 1, 1, 1, 1, 1, 0,
        #       1, 1, 1, 1, 1, 1, 1,
        #       1, 1, 1, 1, 1, 1, 1,
        #       1, 1, 1, 1, 1, 1, 1,
        #       0, 1, 1, 1, 1, 1, 0,
        #       0, 0, 1, 1, 1, 0, 0),
        #     nrow = 7, ncol = 7, byrow = TRUE
        #   )
        # should be set as chr_landuse_freq_file when preprocessed files are used
        landuse_ras <-
          terra::rast(chr_landuse_files)

        # landuse_freq <-
        #   huimori::rasterize_freq(
        #     ras = landuse_ras,
        #     mat = flt7
        #   )
        df_extract <- chopin::extract_at(
          x = landuse_ras,
          y = sf_monitors_correct_yr,
          radius = int_landuse_radius,
          id = c("TMSID", "TMSID2", "year"),
          func = "frac",
          force_df = TRUE
        )
        df_extract |>
          dplyr::rename_with(
            .cols = dplyr::contains("frac"),
            .fn = ~ paste0("landuse_", ., "_", int_landuse_radius)
          )
      },
      pattern = cross(map(sf_monitors_correct_yr, chr_landuse_files), int_landuse_radius),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_04")
      )
    ),
    # results from the same radius as one, such that chr_landuse_files are the only branching factor
    targets::tar_target(
      name = df_feat_correct_landuse_agg,
      command = {
        # rename columns to indicate radius
        # group 1:4, 5:8, ..., 53:56 for 2010, ... 2023, respectively
        list(
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[1:4]),
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[5:8]),
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[9:12]),
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[13:16]),
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[17:20]),
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[21:24]),
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[25:28]),
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[29:32]),
          Reduce(\(x, y) collapse::join(x, y, on = c("TMSID", "TMSID2", "year")), df_feat_correct_landuse[33:36])
        )
      },
      iteration = "list"
    ),
    ## Preprocessed landuse fraction files (to save time for repeated runs)
    targets::tar_target(
      name = chr_landuse_freq_files,
      command = {
          years <- stringi::stri_extract_first_regex(
            chr_landuse_files, pattern = "20[0-2][0-9]"
          )
          year <- as.integer(years)
          if (!dir.exists(file.path(chr_dir_data, "landuse", "preprocessed"))) {
            dir.create(file.path(chr_dir_data, "landuse", "preprocessed"))
          }
          int_radius_cells <- int_landuse_radius %/% 30L
          chr_radius_cells <- as.character(int_radius_cells)
          # assuming 30m resolution for landuse rasters

          chr_output_file <-
            file.path(
              chr_dir_data, "landuse",
              "preprocessed",
              sprintf("landuse_fraction_%d_%04d.tif", year, int_landuse_radius)
            )

        cmd <-
          c(
            "--input", chr_landuse_files,
            "--output", chr_output_file,
              "--radius", chr_radius_cells, "--radius-in-cells",
              "--chunksize", "2048",
              "--threads", "4",
              "--compression", "ZSTD",
              "--verbose"
          )
        # cmd_bin <- if (basename(tool_bin) == "cargo") "cargo" else tool_bin

        status <- system2(tool_bin, cmd)
        if (!identical(status, 0L)) {
          stop("landuse fraction tool failed with exit status ", status, call. = FALSE)
        }
        if (!file.exists(chr_output_file)) {
          stop("Expected output file was not created: ", chr_output_file, call. = FALSE)
        }
        chr_output_file

      },
      pattern = cross(chr_landuse_files, int_landuse_radius),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_04")
      )
    ),
    ### F05. MTPI (multiscale terrain position index) ####
    targets::tar_target(
      name = df_feat_correct_mtpi,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_file)
        chopin::extract_at(
          x = mtpi_ras,
          y = sf_monitors_correct_yr,
          radius = 1e-6,
          id = c("TMSID", "TMSID2", "year"),
          force_df = TRUE
        ) |>
          dplyr::rename(mtpi = mean)
      },
      pattern = map(sf_monitors_correct_yr)
    ),
    ### F06. MTPI at 1km buffer ####
    targets::tar_target(
      name = df_feat_correct_mtpi_1km,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_1km_file)
        chopin::extract_at(
          x = mtpi_ras,
          y = sf_monitors_correct_yr,
          radius = 1e-6,
          id = c("TMSID", "TMSID2", "year"),
          force_df = TRUE
        ) |>
          dplyr::rename(mtpi_1km = mean)
      },
      pattern = map(sf_monitors_correct_yr)
    ),
    ### F07. Emittors ####
    targets::tar_target(
      name = sf_emission_locs,
      command = {
        sf::st_read(chr_file_emission_locs, quiet = TRUE) |>
          sf::st_transform(5179) |>
          dplyr::filter(
            영업상태구분코드 ==  "01"
          )
      }
    ),
    targets::tar_target(
      name = sf_korea_watershed,
      command = {
        sf::st_read(chr_korea_watershed, quiet = TRUE) |>
          sf::st_transform(5179)
      }
    ),
    targets::tar_target(
      name = sf_feat_nemittors,
      command = {
        watersheds <-
          sf::st_read(
            dsn = chr_korea_watershed,
            quiet = TRUE
          )
        emittors <-
          sf::st_read(
            dsn = chr_file_emission_locs,
            quiet = TRUE
          ) |>
          sf::st_transform(5179) |>
          dplyr::filter(
            영업상태구분코드 ==  "01"
          ) |>
          dplyr::mutate(
            class_weight = as.integer(sub("종", "", 종별명)),
            class_weight = dplyr::case_when(
              class_weight == 1 ~ 80,
              class_weight == 2 ~ 20,
              class_weight == 3 ~ 10,
              class_weight == 4 ~ 2,
              class_weight == 5 ~ 0.25,
              TRUE ~ 0
            )
          ) |>
          sf::st_join(watersheds, join = sf::st_within)
        nemittors <- emittors |>
          sf::st_drop_geometry() |>
          dplyr::select(SBSNCD, class_weight) |>
          dplyr::group_by(SBSNCD) |>
          dplyr::summarize(
            n_emittors_watershed = sum(class_weight, na.rm = TRUE)
          ) |>
          dplyr::ungroup()
        watersheds_n_emit <-
          watersheds |>
          dplyr::left_join(nemittors, by = "SBSNCD") |>
          dplyr::transmute(
            # n_emittors_watershed = ifelse(is.na(n), 0, n)
            n_emittors_watershed = ifelse(is.na(n_emittors_watershed), 0, n_emittors_watershed)
          )
        watersheds_n_emit
      },
      cue = targets::tar_cue("never")
    ),
    targets::tar_target(
      name = df_feat_correct_emittors,
      command = {
        result <- huimori::gw_emittors(
          input = sf_monitors_correct_yr,
          target = sf_emission_locs,
          clip = sf_korea_watershed,
          wfun = "gaussian",
          bw = 2000,
          dist_method = "geodesic"
        ) |>
          dplyr::select(1, 2, year, 3, PM10, PM25, gw_emission)
        result
      },
      pattern = map(sf_monitors_correct_yr)
    ),
    ### F08. Aerosol Optical Depth (daily) ####
    targets::tar_target(
      name = chr_aod_date_seq,
      command = {
        seq(
          from = as.Date("2010-01-01"),
          to = as.Date("2023-12-31"),
          by = "30 days"
        )
      }
    ),
    targets::tar_target(
      name = chr_aod_date_chunks,
      command = {
        start_dates <- chr_aod_date_seq
        end_dates <- c(
          chr_aod_date_seq[-1] - 1,
          as.Date("2023-12-31")
        )
        df_dates <-
          data.frame(
            start_date = start_dates,
            end_date = end_dates
          ) |>
          dplyr::mutate(
            chunk_id = dplyr::row_number()
          ) |>
          dplyr::group_by(chunk_id) |>
          targets::tar_group()
      },
      iteration = "group"
    ),
    # targets::tar_target(
    #   name = df_feat_correct_aod,
    #   command = {
    #     date_pattern <- strftime(
    #       seq(
    #         chr_aod_date_chunks$start_date,
    #         chr_aod_date_chunks$end_date,
    #         by = "day"
    #       ),
    #       "%Y%j",
    #     )

    #     aod_files <- file.path(
    #       chr_dir_aod,
    #       paste0("MCD19A2_Daily_Composite_", date_pattern, ".tif")
    #     )
    #     aod_files <- aod_files[file.exists(aod_files)]

    #     result <- purrr::map_df(
    #       aod_files,
    #       function(file) {
    #         aod_ras <- terra::rast(file)
    #         extracted <- exactextractr::exact_extract(
    #           x = aod_ras,
    #           y = sf_monitors_correct,
    #           fun = "mean",
    #           weights = NULL
    #         )
    #         data.frame(
    #           TMSID = sf_monitors_correct$TMSID,
    #           TMSID2 = sf_monitors_correct$TMSID2,
    #           date = as.Date(basename(file), format = "MCD19A2_Daily_Composite_%Y%j.tif"),
    #           aod = extracted
    #         )
    #       }
    #     )

    #     result |>
    #       dplyr::group_by(TMSID, TMSID2) |>
    #       dplyr::mutate(year = lubridate::year(date)) |>
    #       dplyr::summarize(
    #         aod = mean(aod, na.rm = TRUE),
    #         .groups = "drop_last"
    #       ) |>
    #       dplyr::ungroup()
    #   },
    #   pattern = map(chr_aod_date_chunks),
    #   iteration = "list"
    # ),
    ### F08A. Aerosol Optical Depth (annual) ####
    targets::tar_target(
      name = int_aod_year_chunks,
      command = {
        yrs <-
          strftime(chr_date_range, "%Y") |>
          as.integer()
        yrs_vec <- seq(yrs[1], yrs[2], by = 1)
        yrs_vec
      },
      iteration = "vector"
    ),
    targets::tar_target(
      name = rast_year_aod,
      command = {
        year_i <- int_aod_year_chunks
        chr_year_aod_files <- list.files(
          pattern = paste0("MCD19A2_Daily_Composite_", year_i, "[0-9]{3,3}.tif$"),
          path = chr_dir_aod,
          full.names = TRUE,
          recursive = TRUE
        )

        r_list <- lapply(chr_year_aod_files, terra::rast)

        template <- r_list[[1]]

        aligned_list <- lapply(r_list, function(r) {
          if (terra::ext(r) == terra::ext(template)) {
            return(r)
          } else {
            # extend() adds NA padding if 'r' is smaller than the template
            # crop() trims 'r' if it is larger than the template
            r_extended <- terra::extend(r, template)
            return(terra::crop(r_extended, template))
          }
        })

        aod_ras <- terra::rast(aligned_list)

        aod_yr <- terra::app(
          aod_ras,
          fun = function(x) median(x, na.rm = TRUE)
        )

        aod_yr_dir <- file.path(
          chr_dir_data,
          "aerosol"
        )
        aod_yr_file <-
          file.path(aod_yr_dir, paste0("aod_yearly_", year_i, ".tif"))

        if (!dir.exists(aod_yr_dir)) {
          dir.create(aod_yr_dir, recursive = TRUE)
        }
        terra::writeRaster(
          aod_yr,
          filename = aod_yr_file,
          overwrite = TRUE
        )
        aod_yr_file
      },
      pattern = map(int_aod_year_chunks),
      iteration = "vector"
    ),
    targets::tar_target(
      name = df_feat_correct_year_aod,
      command = {
        aod_ras <- terra::rast(rast_year_aod)
        crs_ras <- terra::crs(aod_ras)
        year_i <- int_aod_year_chunks
        # read
        sf_monitors_correct_buff <-
          sf_monitors_correct_yr |>
          sf::st_transform(crs_ras) |>
          sf::st_buffer(dist = 0.001, nQuadSegs = 90L)

        extracted <- exactextractr::exact_extract(
          x = aod_ras,
          y = sf_monitors_correct_buff,
          fun = "mean",
          weights = NULL,
          force_df = TRUE,
          append_cols = c("TMSID", "TMSID2", "year")
        ) |>
          dplyr::transmute(
            TMSID = TMSID,
            TMSID2 = TMSID2,
            year = year,
            aod = ifelse(is.nan(mean), 0L, mean)
          )
        extracted
      },
      pattern = map(sf_monitors_correct_yr, int_aod_year_chunks, rast_year_aod),
      iteration = "list"
    ),
    ### F09. CHELSA ####
    targets::tar_target(
      name = df_feat_correct_chelsa,
      command = {
        int_year_chelsa <-
          int_aod_year_chunks
        chr_file_chelsa <-
          list.files(
            pattern = paste0("CHELSA_", int_year_chelsa, "_[0-9]{2,2}.nc$"),
            path = chr_dir_chelsa,
            full.names = TRUE,
            recursive = TRUE
          )

        chelsa_ras <- terra::rast(chr_file_chelsa)
        layer_unique <- unique(terra::varnames(chelsa_ras))
        layer_names <- names(chelsa_ras)
        layer_names <- gsub("_[0-9]{1,3}", "", layer_names)

        # Find which layers correspond to which unique variable
        indices_app <- match(layer_names, layer_unique)
        chelsa_ras <-
          terra::tapp(chelsa_ras, index = indices_app, fun = "median")
        names(chelsa_ras) <- layer_unique

        chopin::extract_at(
          x = chelsa_ras,
          y = sf_monitors_correct_yr,
          radius = 1e-6,
          id = c("TMSID", "TMSID2", "year"),
          force_df = TRUE
        )
      },
      pattern = map(sf_monitors_correct_yr, int_aod_year_chunks)
    ),
    ### F10. BLH (ERA5) ####
    targets::tar_target(
      name = rast_era5_blh,
      command = {
        year_i <- int_aod_year_chunks
        chr_year_blh_files <- list.files(
          pattern = paste0("ERA5_BLH_", year_i, "_[0-9]{2,2}.nc$"),
          path = chr_dir_era5_blh,
          full.names = TRUE,
          recursive = TRUE
        )

        blh_ras <- terra::rast(chr_year_blh_files)

        blh_yr <- terra::app(
          blh_ras,
          fun = "median"
        )

        blh_yr_dir <- file.path(
          chr_dir_climate,
          "ERA5_BLH_processed"
        )
        blh_yr_file <-
          file.path(blh_yr_dir, paste0("era5_blh_yearly_", year_i, ".tif"))

        if (!dir.exists(blh_yr_dir)) {
          dir.create(blh_yr_dir, recursive = TRUE)
        }
        terra::writeRaster(
          blh_yr,
          filename = blh_yr_file,
          overwrite = TRUE
        )
        blh_yr_file
      },
      pattern = map(int_aod_year_chunks),
      iteration = "vector"
    ),
    targets::tar_target(
      name = df_feat_correct_blh,
      command = {

        blh_ras <- terra::rast(rast_era5_blh)
        crs_ras <- terra::crs(blh_ras)
        year_i <- int_aod_year_chunks
        # read
        sf_monitors_correct_buff <-
          sf_monitors_correct_yr |>
          sf::st_transform(crs_ras) |>
          sf::st_buffer(dist = 0.00001, nQuadSegs = 90L)

        extracted <-
          exactextractr::exact_extract(
            x = blh_ras,
            y = sf_monitors_correct_buff,
            fun = "mean",
            weights = NULL,
            force_df = TRUE,
            append_cols = c("TMSID", "TMSID2", "year")
          ) |>
          dplyr::transmute(
            TMSID = TMSID,
            TMSID2 = TMSID2,
            year = year,
            blh = ifelse(is.nan(mean), 0L, mean)
          )
        extracted

      },
      pattern = map(sf_monitors_correct_yr, int_aod_year_chunks, rast_era5_blh)
    ),
    ### F11. Merge features ####
    targets::tar_target(
      name = df_feat_correct_merged,
      command = {
        df_res <-
          purrr::reduce(
            list(
              sf_monitors_correct_yr,
              df_feat_correct_d_road,
              df_feat_correct_dem,
              df_feat_correct_dsm,
              df_feat_correct_emittors,
              df_feat_correct_landuse_agg,
              df_feat_correct_mtpi,
              df_feat_correct_mtpi_1km,
              df_feat_correct_year_aod,
              df_feat_correct_blh
            ),
            .f = collapse::join,
            on = c("TMSID", "TMSID2", "year")
          ) %>%
          # dplyr::left_join(
          #   df_feat_correct_wind_annual,
          #   by = c("TMSID", "TMSID2", "year")
          # ) %>%
          dplyr::mutate(
            d_road = as.numeric(d_road) / 1000,
            dsm = as.numeric(dsm),
            dem = as.numeric(dem),
            mtpi = as.numeric(mtpi)#,
            # building_density = as.numeric(building_density),
            # wind_speed_10m = as.numeric(wind_speed_10m),
            # wind_dir_deg = as.numeric(wind_dir_deg),
            # n_emittors_watershed =
            #   ifelse(
            #     is.na(n_emittors_watershed), 0,
            #     as.numeric(n_emittors_watershed)
            #   )
          ) %>%
          sf::st_drop_geometry()
        names(df_res) <- sub("mean.", "", names(df_res))
        df_res
      },
      pattern = map(
        sf_monitors_correct_yr,
        df_feat_correct_d_road,
        df_feat_correct_dem,
        df_feat_correct_dsm,
        df_feat_correct_emittors,
        df_feat_correct_landuse_agg,
        df_feat_correct_mtpi,
        df_feat_correct_mtpi_1km,
        df_feat_correct_year_aod,
        df_feat_correct_blh
      )
    ),
    targets::tar_target(
      name = df_feat_correct_merged_old,
      command = {
        df_res <-
          purrr::reduce(
            .x =
            list(
              sf_monitors_correct_yr,
              df_feat_correct_d_road
            ),
            .f = collapse::join,
            on = c("TMSID", "TMSID2", "year")
          ) %>%
          dplyr::bind_cols(
            df_feat_correct_landuse
          ) %>%
          # dplyr::left_join(
          #   df_feat_correct_building_density,
          #   by = c("TMSID", "TMSID2", "year")
          # ) %>%
          dplyr::left_join(
            {
              purrr::reduce(
              .x = df_feat_correct_year_aod,
              .f = rbind
              ) %>%
              dplyr::rename(aod = mean)
            },
            by = c("TMSID", "TMSID2", "year")
          ) %>%
          # dplyr::left_join(
          #   df_feat_correct_wind_annual,
          #   by = c("TMSID", "TMSID2", "year")
          # ) %>%
          dplyr::mutate(
            dsm = unlist(df_feat_correct_dsm),
            dem = unlist(df_feat_correct_dem),
            # n_emittors_watershed = unlist(df_feat_correct_emittors$n_emittors_watershed),
            mtpi = unlist(df_feat_correct_mtpi),
            mtpi_1km = unlist(df_feat_correct_mtpi_1km)
          ) %>%
          # 4. 단위 변환 및 데이터 타입 정제
          dplyr::mutate(
            d_road = as.numeric(d_road) / 1000,           # m 단위를 km로 변환
            dsm = as.numeric(dsm),
            dem = as.numeric(dem),
            mtpi = as.numeric(mtpi)#,
            # building_density = as.numeric(building_density),
            # wind_speed_10m = as.numeric(wind_speed_10m),
            # wind_dir_deg = as.numeric(wind_dir_deg),
            # n_emittors_watershed =
            #   ifelse(
            #     is.na(n_emittors_watershed), 0,
            #     as.numeric(n_emittors_watershed)
            #   )
          ) %>%
          sf::st_drop_geometry()
        names(df_res) <- sub("mean.", "", names(df_res))
        df_res
      }
    ),
    # Incorrect addresses
    targets::tar_target(
      name = df_feat_incorrect_d_road,
      command = {
        road <- sf::st_read(chr_road_files, quiet = TRUE)
        road <- sf::st_transform(road, sf::st_crs(sf_monitors_incorrect))
        road <- road %>%
          dplyr::filter(!ROAD_TYPE %in% c("002", "004") & ROAD_USE == 0)
        nearest_idx <- sf::st_nearest_feature(
          x = sf_monitors_incorrect,
          y = road
        )
        road_nearest <- road[nearest_idx, ]
        dist_road_nearest <-
          sf::st_distance(
            x = sf_monitors_incorrect,
            y = road_nearest,
            by_element = TRUE
          )
        sf_monitors_dist_att <-
          sf_monitors_incorrect |>
          dplyr::select(
            TMSID, TMSID2, year
          ) |>
          dplyr::mutate(
            d_road = dist_road_nearest
          )
        sf_monitors_dist_att
      }
    ),
    targets::tar_target(
      name = df_feat_incorrect_dsm,
      command = chopin::extract_at(
        x = chr_dsm_file,
        y = sf_monitors_incorrect,
        radius = 1e-6,
        force_df = TRUE
      )
    ),
    targets::tar_target(
      name = df_feat_incorrect_dem,
      command = chopin::extract_at(
        x = chr_dem_file,
        y = sf_monitors_incorrect,
        radius = 1e-6,
        force_df = TRUE
      )
    ),
    targets::tar_target(
      name = df_feat_incorrect_landuse,
      command = {
        # landuse_ras <-
        #   terra::rast(
        #     chr_landuse_file,
        #     win = c(124, 132.5, 33, 38.6)
        #   )

        landuse_freq <-
          terra::rast(chr_landuse_freq_file)
        chopin::extract_at(
          x = landuse_freq,
          y = sf_monitors_incorrect,
          radius = 1e-6,
          force_df = TRUE
        )
      },
      pattern = map(chr_landuse_freq_file),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_incorrect_mtpi,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_file)
        chopin::extract_at(
          x = mtpi_ras,
          y = sf_monitors_incorrect,
          radius = 1e-6,
          force_df = TRUE
        )
      }
    ),
    targets::tar_target(
      name = df_feat_incorrect_mtpi_1km,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_1km_file)
        chopin::extract_at(
          x = mtpi_ras,
          y = sf_monitors_incorrect,
          radius = 1e-6,
          force_df = TRUE
        )
      }
    ),
    targets::tar_target(
      name = df_feat_incorrect_emittors,
      command = {
        result <- gw_emittors(
            input = sf_monitors_incorrect,
            target = sf_emission_locs,
            clip = sf_korea_watershed,
            wfun = "gaussian",
            bw = 5000,
            dist_method = "geodesic"
          )
        result
      }
    ),
    targets::tar_target(
      name = df_feat_incorrect_merged,
      command = {
        df_res <-
          purrr::reduce(
            .x =
              list(
                sf_monitors_incorrect,
                df_feat_incorrect_d_road
              ),
            .f = collapse::join,
            on = c("TMSID", "TMSID2", "year")
          ) %>%
          dplyr::bind_cols(
            df_feat_incorrect_landuse
          ) %>%
          dplyr::left_join(
            df_feat_incorrect_building_density,
            by = c("TMSID", "TMSID2", "year")
          ) %>%
          # dplyr::left_join(
          #   df_feat_incorrect_wind_annual,
          #   by = c("TMSID", "TMSID2", "year")
          # ) %>%
          dplyr::mutate(
            dsm = unlist(df_feat_incorrect_dsm),
            dem = unlist(df_feat_incorrect_dem)
          ) %>%
          dplyr::mutate(
            d_road = as.numeric(d_road) / 1000,
            dsm = as.numeric(dsm),
            dem = as.numeric(dem),
            building_density = as.numeric(building_density),
            wind_speed_10m = as.numeric(wind_speed_10m),
            wind_dir_deg = as.numeric(wind_dir_deg),
            n_emittors_watershed = unlist(df_feat_correct_emittors$n_emittors_watershed),
            mtpi = unlist(df_feat_incorrect_mtpi),
            mtpi_1km = unlist(df_feat_incorrect_mtpi_1km)
          ) %>%
          sf::st_drop_geometry()
        names(df_res) <- sub("mean.", "", names(df_res))
        df_res
      }
    ),
    # Grid point features
   targets::tar_target(
     name = df_feat_grid_d_road,
     command = {
       road <- sf::st_read(chr_road_files, quiet = TRUE)
       road <- sf::st_transform(road, sf::st_crs(list_pred_calc_grid))
       road <- road %>%
         dplyr::filter(!ROAD_TYPE %in% c("002", "004") & ROAD_USE == 0)
       nearest_idx <- sf::st_nearest_feature(
         x = list_pred_calc_grid,
         y = road
       )
       road_nearest <- road[nearest_idx, ]
       dist_road_nearest <-
         sf::st_distance(
           x = list_pred_calc_grid,
           y = road_nearest,
           by_element = TRUE
         )
       
       sf_grid_dist_att <-
         list_pred_calc_grid |>
         dplyr::mutate(
           d_road = dist_road_nearest
         ) |>
         sf::st_drop_geometry()
       
       sf_grid_dist_att
     },
     iteration = "list",
     
     # Cross mapping
     pattern = cross(list_pred_calc_grid, chr_road_files),
     resources = targets::tar_resources(
       crew = targets::tar_resources_crew(controller = "controller_20")
     )
   ),
    targets::tar_target(
      name = df_feat_grid_dsm,
      command = {
        chopin::extract_at(
          x = chr_dsm_file,
          y = list_pred_calc_grid,
          radius = 1e-6,
          force_df = TRUE
        )
      },
      iteration = "list",
      pattern = map(list_pred_calc_grid),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_dem,
      command = {
        chopin::extract_at(
          x = chr_dem_file,
          y = list_pred_calc_grid,
          radius = 1e-6,
          force_df = TRUE
        )
      },
      iteration = "list",
      pattern = map(list_pred_calc_grid),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_landuse,
      command = {
        chunk_size <- 2e4L
        n_grid <- nrow(list_pred_calc_grid)
        n_chunk <- ceiling(n_grid / chunk_size)
        pad_m <- int_landuse_radius + 100
        year_landuse <- as.integer(
          gsub(".*?(20[0-9]{2,2}).*", "\\1", basename(chr_landuse_freq_file))
        )
        init_list <- vector("list", n_chunk)

        for (i in seq_len(n_chunk)) {
          idx_start <- ((i - 1L) * chunk_size) + 1L
          idx_end <- min(i * chunk_size, n_grid)
          list_pred_calc_grid_i <- list_pred_calc_grid[idx_start:idx_end, ]

          ext_reproj <-
            terra::project(
              terra::ext(list_pred_calc_grid_i) + pad_m,
              from = "EPSG:5179",
              to = "EPSG:4326"
            )

          landuse_ras <-
            terra::rast(
              chr_landuse_freq_file,
              win = ext_reproj
            )

          list_pred_calc_grid_i[["year"]] <- year_landuse
          init_list[[i]] <-
            chopin::extract_at(
              x = landuse_ras,
              y = list_pred_calc_grid_i,
              radius = int_landuse_radius,
              id = c("gid", "year"),
              func = "mean",
              force_df = TRUE
            )

          rm(landuse_ras, list_pred_calc_grid_i)
          if (i %% 10L == 0L) {
            gc(FALSE)
          }
        }

        df_extract <- collapse::rowbind(init_list, fill = TRUE)
        cols_landuse <- setdiff(names(df_extract), c("gid", "year"))

        df_extract |>
          dplyr::rename_with(
            .cols = dplyr::all_of(cols_landuse),
            .fn = ~ paste0("landuse_", ., "_", int_landuse_radius)
          )
      },
      iteration = "list",
      pattern = cross(cross(list_pred_calc_grid, chr_landuse_freq_file), int_landuse_radius),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_40")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_mtpi,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_file)
        chopin::extract_at(
          x = mtpi_ras,
          y = list_pred_calc_grid,
          radius = 1e-6,
          force_df = TRUE
        )
      },
      iteration = "list",
      pattern = map(list_pred_calc_grid),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_mtpi_1km,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_1km_file)
        chopin::extract_at(
          x = mtpi_ras,
          y = list_pred_calc_grid,
          radius = 1e-6,
          force_df = TRUE
        )
      },
      iteration = "list",
      pattern = map(list_pred_calc_grid),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_emittors,
      command = {
        result <-
          gw_emittors(
            input = list_pred_calc_grid,
            target = sf_emission_locs,
            clip = sf_korea_watershed,
            wfun = "gaussian",
            bw = 5000,
            dist_method = "geodesic"
          )
        result
      },
      iteration = "list",
      pattern = map(list_pred_calc_grid),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_08")
      )
    ),
   targets::tar_target(
     name = df_feat_grid_merged,
     command = {
       df_res <- df_feat_grid_d_road %>%
         dplyr::bind_cols(df_feat_grid_landuse)
       df_res %>%
         dplyr::mutate(
           dsm = as.numeric(df_feat_grid_dsm),
           dem = as.numeric(df_feat_grid_dem),
           mtpi = as.numeric(df_feat_grid_mtpi),
           n_emittors_watershed = as.numeric(df_feat_grid_emittors$n_emittors_watershed),
           d_road = as.numeric(d_road) / 1000
         )
     },
     iteration = "list",
     pattern = map(
       df_feat_grid_d_road,
       df_feat_grid_landuse
     )
   )
)



# ----------------------------------------------------------------
# 변경 log 기록(dhnyu)
## 2026.01.31

### DAG 상에서 최종 객체와 직접적으로 이어지지 않는 target 체크
#### dt_asos, ras_landuse_freq
#### int_size_split, sf_grid_correct_split, int_split_grid_ids, list_pred_calc_grid_old
#### df_feat_incorrect_emittors, df_feat_grid_mtpi_1km

### df_feat_grid_d_road 수정: 연도별 값 반영하여 시공간변수화
### df_feat_grid_merged 수정: 일반 공간변수(593)와 시공간변수(8302, road/landuse)를 같은 길이로 매핑하도록 수정.


## 2026.02.05
### df_feat_correct_landuse 수정:
#### (1) landuse_ras <- terra::rast(chr_landuse_freq_file)는 실수형이 아니라 비율형이기 때문에 `chopin::extract_at(func="frac")`을 `chopin::extract_at(func="mean")`로 수정.
#### (2) 현재 토지피복 연도가 측정소 연도보다 1년 전인 행만 유지하기.

## 2026.02.06
### dt_measurements 수정:
#### (1) 시간 밀림 보정 (time zone 정보가 없어서 시간별 미세먼지 데이터의 시간이 9시간씩 밀려있었음.)
#### (2) 대기질 농도에서 음수값(-999로 기록됨)은 결측치 처리

### df_feat_correct_merged 수정: landuse 변경사항에 맞게 수정
