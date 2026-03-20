
list_process_site <-
  list(
    targets::tar_target(
      name = dt_measurements,
      command = nanoparquet::read_parquet(chr_measurement_file)
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

# Is it really necessary?
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
    ),
    targets::tar_target(
      list_pred_calc_grid_old,
      command = {
        grid_unit <- sf::st_bbox(sf_grid_correct_split[int_split_grid_ids, ])
        sf::st_as_sf(
          sf_grid_size |>
            dplyr::filter(
              (X <= grid_unit[3] & X >= grid_unit[1]) &
              (Y <= grid_unit[4] & Y >= grid_unit[2])
            ),
          coords = c("X", "Y"),
          crs = 5179,
          remove = FALSE
        )
      },
      iteration = "list",
      pattern = map(int_split_grid_ids),
      description = "Split prediction grid into list by chopin grid",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_15")
      )
    )
  )



list_process_feature <-
  list(
    ### F01. Distance to the nearest road ####
    targets::tar_target(
      name = df_feat_correct_d_road,
      command = {
        road <- sf::st_read(chr_road_files[length(chr_road_files)], quiet = TRUE)
        road <- sf::st_transform(road, sf::st_crs(sf_monitors_correct))
        road <- road %>%
          dplyr::filter(!ROAD_TYPE %in% c("002", "004") & ROAD_USE == 0)
        nearest_idx <- sf::st_nearest_feature(
          x = sf_monitors_correct,
          y = road
        )
        road_nearest <- road[nearest_idx, ]
        dist_road_nearest <-
          sf::st_distance(
            x = sf_monitors_correct,
            y = road_nearest,
            by_element = TRUE
          )
        sf_monitors_dist_att <-
          sf_monitors_correct |>
          dplyr::select(
            TMSID, TMSID2, year
          ) |>
          dplyr::mutate(
            d_road = dist_road_nearest
          )
        sf_monitors_dist_att
      }
    ),
    ### F02. DSM (surface elevation) ####
    targets::tar_target(
      name = df_feat_correct_dsm,
      command = chopin::extract_at(
        x = chr_dsm_file,
        y = sf_monitors_correct,
        radius = 1e-6,
        force_df = TRUE
      )
    ),
    ### F03. DEM (ground elevation) ####
    targets::tar_target(
      name = df_feat_correct_dem,
      command = chopin::extract_at(
        x = chr_dem_file,
        y = sf_monitors_correct,
        radius = 1e-6,
        force_df = TRUE
      )
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
          y = sf_monitors_correct,
          radius = int_landuse_radius,
          func = "frac",
          force_df = TRUE
        )
        df_extract |>
          dplyr::rename_with(~ paste0("landuse_frac_", ., "_", int_landuse_radius))
      },
      pattern = cross(chr_landuse_files, int_landuse_radius),
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
          df_feat_correct_landuse[[1:4]],
          df_feat_correct_landuse[[5:8]],
          df_feat_correct_landuse[[9:12]],
          df_feat_correct_landuse[[13:16]],
          df_feat_correct_landuse[[17:20]],
          df_feat_correct_landuse[[21:24]],
          df_feat_correct_landuse[[25:28]],
          df_feat_correct_landuse[[29:32]],
          df_feat_correct_landuse[[33:36]],
          df_feat_correct_landuse[[37:40]],
          df_feat_correct_landuse[[41:44]],
          df_feat_correct_landuse[[45:48]],
          df_feat_correct_landuse[[49:52]],
          df_feat_correct_landuse[[53:56]]
        )
      },
      iteration = "list"
    ),
    ### F05. MTPI (multiscale terrain position index) ####
    targets::tar_target(
      name = df_feat_correct_mtpi,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_file)
        chopin::extract_at(
          x = mtpi_ras,
          y = sf_monitors_correct,
          radius = 1e-6,
          force_df = TRUE
        )
      }
    ),
    ### F06. MTPI at 1km buffer ####
    targets::tar_target(
      name = df_feat_correct_mtpi_1km,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_1km_file)
        chopin::extract_at(
          x = mtpi_ras,
          y = sf_monitors_correct,
          radius = 1e-6,
          force_df = TRUE
        )
      }
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
      }
    ),
    targets::tar_target(
      name = df_feat_correct_emittors,
      command = {
        result <- gw_emittors(
          input = sf_monitors_correct,
          target = sf_emission_locs,
          clip = sf_korea_watershed,
          wfun = "gaussian",
          bw = 5000,
          dist_method = "geodesic"
        )
        result
      }
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
      iteration = "list"
    ),
    targets::tar_target(
      name = rast_year_aod,
      command = {
        year_i <- int_aod_year_chunks
        aod_files <- grep(
          paste0("MCD19A2_Daily_Composite_", year_i, "[0-9]{3,3}.tif$"),
          chr_dir_aod,
          value = TRUE
        )

        r_list <- lapply(aod_files, terra::rast)

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
          fun = function(x) median(x, na.rm = TRUE),
          cores = 4L
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
      iteration = "list"
    ),
    targets::tar_target(
      name = df_feat_correct_year_aod,
      command = {
        year_i <- int_aod_year_chunks
        # read
        sf_monitors_correct_buff <-
          sf_monitors_correct |>
          dplyr::filter(year == year_i) |>
          sf::st_transform(terra::crs(template)) |>
          sf::st_buffer(dist = 0.001, nQuadSegs = 90L)

        extracted <- exactextractr::exact_extract(
          x = rast_year_aod,
          y = sf_monitors_correct_buff,
          fun = "mean",
          weights = NULL,
          force_df = TRUE,
          append_cols = c("TMSID", "TMSID2", "year")
        )
        extracted
      },
      pattern = map(int_aod_year_chunks, rast_year_aod),
      iteration = "list"
    ),
    ### F09. Merge features ####
    targets::tar_target(
      name = df_feat_correct_merged,
      command = {
        df_res <-
          purrr::reduce(
            .x =
            list(
              sf_monitors_correct,
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
            df_feat_correct_year_aod,
            by = c("TMSID", "TMSID2", "year")
          ) %>%
          # dplyr::left_join(
          #   df_feat_correct_wind_annual,
          #   by = c("TMSID", "TMSID2", "year")
          # ) %>%
          dplyr::mutate(
            dsm = unlist(df_feat_correct_dsm),
            dem = unlist(df_feat_correct_dem),
            n_emittors_watershed = unlist(df_feat_correct_emittors$n_emittors_watershed),
            mtpi = unlist(df_feat_correct_mtpi),
            mtpi_1km = unlist(df_feat_correct_mtpi_1km)
          ) %>%
          dplyr::mutate(
            d_road = as.numeric(d_road) / 1000,
            dsm = as.numeric(dsm),
            dem = as.numeric(dem),
            mtpi = as.numeric(mtpi),
            building_density = as.numeric(building_density),
            wind_speed_10m = as.numeric(wind_speed_10m),
            wind_dir_deg = as.numeric(wind_dir_deg),
            n_emittors_watershed =
              ifelse(
                is.na(n_emittors_watershed), 0,
                as.numeric(n_emittors_watershed)
              )
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
        road <- sf::st_read(chr_road_files[length(chr_road_files)], quiet = TRUE)
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
      pattern = map(list_pred_calc_grid),
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
        init_list <- list()
        # another implementation: memory-minded
        list_10k_split <-
          list_pred_calc_grid |>
          nrow() |>
          seq_len()
        list_10k_split <-
          ceiling(list_10k_split / 5e4)
        for (i in seq_len(max(list_10k_split))) {
          list_pred_calc_grid_i <-
            list_pred_calc_grid[list_10k_split == i, ]
          if (nrow(list_pred_calc_grid_i) == 0) {
            next
          }

          crs_ras <- "EPSG:4326"
          crs_vec <- "EPSG:5179"
          ext_reproj <-
            terra::project(
              terra::ext(list_pred_calc_grid_i) + 1000,
              crs_vec, crs_ras
            )

          landuse_ras <-
            terra::rast(
              chr_landuse_freq_file,
              win = ext_reproj
            )
          extracted_i <-
            chopin::extract_at(
              x = landuse_ras,
              y = list_pred_calc_grid_i,
              radius = 100,
              func = "frac",
              force_df = TRUE
            )
          init_list[[i]] <- extracted_i
          rm(extracted_i)
        }

        collapse::rowbind(init_list, fill = TRUE)

        # old implementation
        # landuse_ras <-
        #   terra::rast(chr_landuse_files[length(chr_landuse_files)], win = c(124, 132.5, 33, 38.6))

        # # landuse_freq <-
        # #   terra::rast(file.path(chr_dir_data, "landuse_freq_glc_fcs30d_2022.tif"))
        # chopin::extract_at(
        #   x = landuse_ras,
        #   y = list_pred_calc_grid,
        #   radius = 100,
        #   func = "frac",
        #   force_df = TRUE
        # )
      },
      iteration = "list",
      pattern = cross(list_pred_calc_grid, chr_landuse_files),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
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
       df_res <-
          purrr::reduce(
            .x =
              list(
                list_pred_calc_grid,
                df_feat_grid_d_road
              ),
            .f = collapse::join,
            on = c("gid")
          ) %>%
          dplyr::bind_cols(
            df_feat_grid_landuse
          ) %>%
          dplyr::mutate(
            dsm = unlist(df_feat_grid_dsm),
            dem = unlist(df_feat_grid_dem),
            n_emittors_watershed = unlist(df_feat_grid_emittors$n_emittors_watershed),
            mtpi = unlist(df_feat_grid_mtpi)
          ) %>%
          dplyr::mutate(
            d_road = as.numeric(d_road) / 1000,
            dsm = as.numeric(dsm),
            dem = as.numeric(dem),
            mtpi = as.numeric(mtpi),
            n_emittors_watershed = as.numeric(n_emittors_watershed)
          ) %>%
          sf::st_drop_geometry()
        names(df_res) <- sub("mean.", "", names(df_res))
        df_res
      },
      iteration = "list",
      pattern =
      map(
        list_pred_calc_grid,
        df_feat_grid_d_road,
        df_feat_grid_dsm,
        df_feat_grid_dem,
        df_feat_grid_landuse,
        df_feat_grid_mtpi,
        df_feat_grid_emittors
      )
  )
)