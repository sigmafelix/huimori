
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
    )
  )

# Is it really necessary?
list_process_split <-
  list(
    targets::tar_target(
      name = int_grid_size,
      command = c(50, 100, 250, 500),
      iteration = "list"
    ),
    targets::tar_target(
      name = int_size_split,
      command = c(50L, 20L, 10L, 2L),
      iteration = "list"
    ),
    targets::tar_target(
      name = sf_grid_size,
      command = {
        # sf_monitors_ext <-
        #   sf::st_bbox(sf_monitors_correct) %>%
        #   sf::st_as_sfc() %>%
        #   sf::st_as_sf() %>%
        #   sf::st_buffer(
        #     endCapStyle = "SQUARE",
        #     joinStyle = "MITRE",
        #     dist = 5000
        #   )
        korea_poly <- sf_korea_all |>
          sf::st_make_valid() |>
          sf::st_buffer(1000)
        sf::st_make_grid(
          korea_poly,
          cellsize = int_grid_size,
          what = "centers"
        ) %>%
        sf::st_as_sf() %>%
        dplyr::mutate(gid = seq_len(n()))
      }
      ,
      iteration = "list",
      pattern = map(int_grid_size)
    ),
    targets::tar_target(
      name = sf_grid_correct_split,
      command = chopin::par_pad_grid(
        input = sf_grid_size,
        mode = "grid",
        nx = int_size_split,
        ny = int_size_split,
        padding = 10
      )$original,
      iteration = "list",
      pattern = map(sf_grid_size, int_size_split)
    ),
    targets::tar_target(
      name = sf_grid_correct_set,
      command = sf_grid_size[sf_grid_correct_split, ],
      iteration = "list",
      pattern = cross(sf_grid_size, sf_grid_correct_split)
    )
  )



list_process_feature <-
  list(
    targets::tar_target(
      name = df_feat_correct_d_road,
      command = {
        road <- sf::st_read(chr_road_files[[11]], quiet = TRUE)
        road <- sf::st_transform(road, sf::st_crs(sf_monitors_correct))
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
          dplyr::mutate(
            d_road = dist_road_nearest
          )
        sf_monitors_dist_att
      }
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
        landuse_ras <-
          terra::rast(
            chr_landuse_file,
            win = c(124, 132.5, 33, 38.6)
          )
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
      name = df_feat_correct_merged,
      command = {
        purrr::reduce(
          .x =
            list(
              sf_monitors_correct,
              df_feat_correct_d_road,
              df_feat_correct_dsm,
              df_feat_correct_dem,
              df_feat_correct_landuse
            ),
          .f = collapse::join,
          on = c("TMSID", "TMSID2", "year")
        )
      }
    ),
    targets::tar_target(
      name = df_feat_grid_d_road,
      command = {
        road <- sf::st_read(chr_road_files[[11]], quiet = TRUE)
        road <- sf::st_transform(road, sf::st_crs(sf_grid_correct_set))
        nearest_idx <- sf::st_nearest_feature(
          x = sf_grid_correct_set,
          y = road
        )
        road_nearest <- road[nearest_idx, ]
        dist_road_nearest <-
          sf::st_distance(
            x = sf_grid_correct_set,
            y = road_nearest,
            by_element = TRUE
          )
        sf_grid_dist_att <-
          sf_grid_correct_set |>
          dplyr::mutate(
            d_road = dist_road_nearest
          )
        sf_grid_dist_att

      },
      iteration = "list",
      pattern = map(sf_grid_correct_set)
    ),
    targets::tar_target(
      name = df_feat_grid_dsm,
      command = {
        chopin::extract_at(
          x = chr_dsm_file,
          y = sf_grid_correct_set,
          radius = 1e-6,
          force_df = TRUE
        )
      },
      iteration = "list",
      pattern = map(sf_grid_correct_set)
    ),
    targets::tar_target(
      name = df_feat_grid_dem,
      command = {
        chopin::extract_at(
          x = chr_dem_file,
          y = sf_grid_correct_set,
          radius = 1e-6,
          force_df = TRUE
        )
      },
      iteration = "list",
      pattern = map(sf_grid_correct_set)
    ),
    targets::tar_target(
      name = df_feat_grid_landuse,
      command = {
        landuse_ras <-
          terra::rast(chr_landuse_file, win = c(124, 132.5, 33, 38.6))

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
      },
      iteration = "list",
      pattern = map(sf_grid_correct_set)
    ),
    targets::tar_target(
      name = df_feat_grid_merged,
      command = {
        purrr::reduce(
          .x = 
          list(
            sf_grid_correct_set,
            df_feat_grid_d_road,
            df_feat_grid_dsm,
            df_feat_grid_dem,
            df_feat_grid_landuse
          ),
          .f = collapse::join,
          on = "gid"
        )
      },
      iteration = "list",
      pattern = map(sf_grid_correct_set)
  )
)