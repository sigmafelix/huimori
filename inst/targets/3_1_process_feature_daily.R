
list_process_site_daily <-
  list(
    targets::tar_target(
      name = sf_monitors_correct_daily,
      command = {
        # 1. 일별 농도 집계 (Hourly -> Daily) 및 전처리
        ak_sites_daily <- huimori::summarize_daily(
          data = dt_measurements, 
          timeflag = "date"
        ) %>%
          dplyr::rename(date = date_s) %>%
          dplyr::mutate(
            date = as.Date(date),
            TMSID = as.character(TMSID),
            TMSID2 = as.character(TMSID2)
          )
        
        # 2. 지점 이동 거리 계산 (Relocation Distance)
        sites_cfd <- sf_monitors_base |>
          dplyr::arrange(TMSID, date_start) |>
          dplyr::group_by(TMSID) |>
          dplyr::mutate(lon2 = lag(lon), lat2 = lag(lat)) |>
          dplyr::rowwise() |>
          dplyr::mutate(dist_m = geosphere::distGeo(c(lon, lat), c(lon2, lat2))) |>
          dplyr::ungroup()
        
        # 3. 시공간 범위 확장 (Daily Grid Generation)
        # 시작일이 종료일보다 늦은 오류 지점 필터링 포함
        sites_fullrange_daily <- sites_cfd %>%
          dplyr::group_by(TMSID, TMSID2) %>%
          dplyr::filter(
            !is.na(date_start) & 
              !is.na(date_end) & 
              as.Date(date_start) <= as.Date(date_end)
          ) %>%
          tidyr::nest() %>%
          dplyr::mutate(
            date_all = purrr::map(data, function(df) {
              dstart <- as.Date(df$date_start)
              dend   <- as.Date(df$date_end)
              data.frame(date = seq.Date(dstart, dend, by = "day"))
            })
          ) %>%
          tidyr::unnest(c(date_all, data)) %>%
          dplyr::ungroup()
        
        # 4. 공간 데이터 변환 및 일별 농도 데이터 최종 조인
        res <- sites_fullrange_daily |>
          dplyr::mutate(
            date = as.Date(date),
            TMSID = as.character(TMSID),
            TMSID2 = as.character(TMSID2)
          ) |>
          dplyr::filter(!is.na(lon)) |>
          sf::st_as_sf(coords = c("lon", "lat"), crs = 4326) |>
          sf::st_transform(5179) |>
          # 집계된 일별 농도 데이터와 병합
          dplyr::full_join(
            ak_sites_daily,
            by = c("TMSID", "TMSID2", "date")
          ) |>
          # 유효한 기하 정보 유지 및 메타데이터 정리
          dplyr::filter(!sf::st_is_empty(geometry)) |>
          dplyr::mutate(year = lubridate::year(date)) |>
          dplyr::filter(year >= 2010) |>
          # 컬럼 순서 재배치 (Identifier - Target - Features 순)
          dplyr::relocate(
            any_of(c("date", "PM10", "PM25")),
            .after = TMSID2
          )
        
        res
      }
    ),
    # sf_monitors_correct branched by month (subset by month)                  ############# 추가 (daehoon)
    targets::tar_target(
      name = sf_monitors_correct_month,
      command = {
        sf_monitors_correct_daily |>
          dplyr::filter(format(date, "%Y-%m") == chr_months_spatial)
      },
      pattern = map(chr_months_spatial),
      iteration = "list"
    )
  )


## Grid processing for prediction ####
# list_process_split_daily <-
#   (
#   )


## Annualized feature calculation ####
list_process_feature_daily <-
  list(
    # targets::tar_target(
    #   name = chr_aod_date_seq,
    #   command = {
    #     seq(
    #       from = as.Date("2010-01-01"),
    #       to = as.Date("2023-12-31"),
    #       by = "30 days"
    #     )
    #   }
    # ),
    # targets::tar_target(
    #   name = chr_aod_date_chunks,
    #   command = {
    #     start_dates <- chr_aod_date_seq
    #     end_dates <- c(
    #       chr_aod_date_seq[-1] - 1,
    #       as.Date("2023-12-31")
    #     )
    #     df_dates <-
    #       data.frame(
    #         start_date = start_dates,
    #         end_date = end_dates
    #       ) |>
    #       dplyr::mutate(
    #         chunk_id = dplyr::row_number()
    #       ) |>
    #       dplyr::group_by(chunk_id) |>
    #       targets::tar_group()
    #   },
    #   iteration = "group"
    # ),
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
    ### F08. Aerosol Optical Depth (daily) ####                                ############# 추가 (daehoon) 
    targets::tar_target(
      name = df_feat_correct_aod_daily,
      command = {
        buffer_radii_m <- c(100, 500, 1000, 2000, 5000)
        mon_start <- as.Date(paste0(chr_months_spatial, "-01"))
        mon_end   <- lubridate::rollback(mon_start + months(1))
        date_seq  <- seq(mon_start, mon_end, by = "day")
        date_pattern <- strftime(date_seq, "%Y%j")

        aod_files <- file.path(
          chr_dir_aod,
          paste0("MCD19A2_Daily_Composite_", date_pattern, ".tif")
        )
        
        valid_files <- aod_files[file.exists(aod_files)]
        if (length(valid_files) == 0) return(data.frame())
        
        # 월 내부의 일별(Daily) 추출 루프
        purrr::map_df(valid_files, function(file) {
          date_str <- stringr::str_extract(basename(file), "\\d{7}")
          current_date <- as.Date(date_str, format = "%Y%j")
          
          # 해당 날짜에 실제 운영 중인 측정소만 필터링
          current_sf <- sf_monitors_correct_month |> 
            dplyr::filter(date == current_date)
          
          if (nrow(current_sf) == 0) return(NULL)
          
          # Raster 로드 및 좌표계 일치
          aod_ras <- terra::rast(file)
          ras_crs <- terra::crs(aod_ras)

          current_sf_proj <- current_sf |> 
            sf::st_transform(ras_crs)

          result <- data.frame(
            TMSID  = current_sf$TMSID,
            TMSID2 = current_sf$TMSID2,
            date   = current_date
          )

          for (buffer_m in buffer_radii_m) {
            current_sf_buff <- current_sf_proj |> 
              sf::st_buffer(dist = buffer_m)

            extracted <- exactextractr::exact_extract(
              x = aod_ras,
              y = current_sf_buff,
              fun = "mean",
              force_df = TRUE,
              progress = FALSE
            )

            result[[paste0("aod_", buffer_m, "m")]] <-
              ifelse(is.nan(extracted$mean), NA, extracted$mean)
          }

          result
        })
      },
      # chr_months_spatial (108개)를 따라 branch 생성
      pattern = map(chr_months_spatial, sf_monitors_correct_month),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_15")
      )
    ),
    ### F09. CHELSA ####
    
    
    
    ### F10-1. ERA5-Land (daily) ####
    targets::tar_target(
      name = df_feat_correct_era5_land_daily,
      command = {
        buffer_radii_m <- c(100, 500, 1000, 2000, 5000)

        curr_yr  <- substr(chr_months_spatial, 1, 4)
        curr_mon <- substr(chr_months_spatial, 6, 7)
        zip_nc_path <- file.path(chr_dir_era5_land, 
                                 paste0("ERA5_Land_", curr_yr, "_", curr_mon, ".nc"))
        if (!file.exists(zip_nc_path)) return(data.frame())
        temp_sub <- file.path(tempdir(), paste0("era5_target_", curr_yr, curr_mon, "_", sample(1e5, 1)))
        dir.create(temp_sub, showWarnings = FALSE, recursive = TRUE)
        
        tryCatch({
          unzip(zip_nc_path, files = "data_0.nc", exdir = temp_sub)
          real_nc <- file.path(temp_sub, "data_0.nc")
          if (!file.exists(real_nc)) return(data.frame())
          r_full <- terra::rast(real_nc)
          n_lyr_per_var <- terra::nlyr(r_full) / 6
          start_utc <- as.POSIXct(paste0(chr_months_spatial, "-01 00:00:00"), tz = "UTC")
          time_seq_utc <- seq(from = start_utc, by = "hour", length.out = n_lyr_per_var)

          date_kst <- as.Date(time_seq_utc, tz = "Asia/Seoul")
          unique_dates <- sort(unique(date_kst))
          v_mean <- c("t2m", "u10", "v10", "sp")    # 평균 처리할 변수들 (기온, 풍속, 표면기압)
          v_sum  <- c("ssr", "tp")                  # 누적 집계할 변수들 (일사량, 강수량)
          
          daily_list <- list()
          for (v in c(v_mean, v_sum)) {
            idx <- grep(v, names(r_full))
            if (length(idx) == 0) next
            agg_fun <- if (v %in% v_mean) "mean" else "sum"
            daily_list[[v]] <- terra::tapp(r_full[[idx]], 
                                           index = date_kst[1:length(idx)], 
                                           fun = agg_fun, 
                                           na.rm = TRUE)
          }

          current_sf <- sf_monitors_correct_month |>
            dplyr::arrange(date) |>
            dplyr::group_by(TMSID, TMSID2) |>
            dplyr::slice(1) |>
            dplyr::ungroup()
          
          current_sf_proj <- current_sf |> 
            sf::st_transform(terra::crs(r_full))

          res_list <- lapply(buffer_radii_m, function(buffer_m) {
            s_buff <- current_sf_proj |> 
              sf::st_buffer(dist = buffer_m)

            var_list <- lapply(names(daily_list), function(v_name) {
              ext <- exactextractr::exact_extract(daily_list[[v_name]], s_buff, 
                                                  fun = "mean", force_df = TRUE, progress = FALSE)
              
              # 데이터가 비어있는지 확인
              if (nrow(ext) == 0) return(NULL)
              
              ext |>
                dplyr::mutate(TMSID = current_sf$TMSID, TMSID2 = current_sf$TMSID2) |>
                tidyr::pivot_longer(cols = dplyr::starts_with("mean"), 
                                    names_to = "discard", 
                                    values_to = paste0(v_name, "_", buffer_m, "m")) |>
                dplyr::group_by(TMSID, TMSID2) |>
                dplyr::mutate(date = unique_dates[1:dplyr::n()]) |>
                dplyr::ungroup() |>
                dplyr::select(TMSID, TMSID2, date, dplyr::all_of(paste0(v_name, "_", buffer_m, "m")))
            })

            var_list <- Filter(Negate(is.null), var_list)
            if (length(var_list) == 0) return(NULL)
            Reduce(function(x, y) dplyr::left_join(x, y, by = c("TMSID", "TMSID2", "date")), var_list)
          })
          
          res_list <- Filter(Negate(is.null), res_list)
          if (length(res_list) == 0) return(data.frame())

          final_mon <- Reduce(function(x, y) dplyr::left_join(x, y, by = c("TMSID", "TMSID2", "date")), res_list) |>
            dplyr::mutate(
              dplyr::across(dplyr::starts_with("t2m_"), ~ .x - 273.15)
            ) |>
            dplyr::filter(
              !is.na(date),
              format(date, "%Y-%m") == chr_months_spatial
            )

          return(final_mon)
          
        }, error = function(e) {
          message(paste("Error in branch", chr_months_spatial, ":", e$message))
          return(data.frame())
        }, finally = {
          unlink(temp_sub, recursive = TRUE)
        })
      },
      pattern = map(chr_months_spatial, sf_monitors_correct_month),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_15")
      )
    ),
    
    
    ### F10-2. BLH (daily) ####
    targets::tar_target(
      name = df_feat_correct_era5_blh_daily,
      command = {
        buffer_radii_m <- c(100, 500, 1000, 2000, 5000)

        curr_yr  <- substr(chr_months_spatial, 1, 4)
        curr_mon <- substr(chr_months_spatial, 6, 7)
        nc_path  <- file.path(chr_dir_era5_blh, 
                              paste0("ERA5_BLH_", curr_yr, "_", curr_mon, ".nc"))

        if (!file.exists(nc_path)) return(data.frame())
        
        tryCatch({

          r_full <- terra::rast(nc_path)

          start_utc <- as.POSIXct(paste0(chr_months_spatial, "-01 00:00:00"), tz = "UTC")
          time_seq_utc <- seq(from = start_utc, by = "hour", length.out = terra::nlyr(r_full))
          
          date_kst <- as.Date(time_seq_utc, tz = "Asia/Seoul")
          unique_dates <- sort(unique(date_kst))

          r_daily <- terra::tapp(r_full, 
                                 index = date_kst[1:terra::nlyr(r_full)], 
                                 fun = "mean", 
                                 na.rm = TRUE)

          current_sf <- sf_monitors_correct_month |>
            dplyr::arrange(date) |>
            dplyr::group_by(TMSID, TMSID2) |>
            dplyr::slice(1) |>
            dplyr::ungroup()
          
          current_sf_proj <- current_sf |> 
            sf::st_transform(terra::crs(r_full))

          res_list <- lapply(buffer_radii_m, function(buffer_m) {
            s_buff <- current_sf_proj |> 
              sf::st_buffer(dist = buffer_m)

            ext <- exactextractr::exact_extract(r_daily, s_buff, 
                                                fun = "mean", force_df = TRUE, progress = FALSE)
            
            ext |>
              dplyr::mutate(TMSID = current_sf$TMSID, TMSID2 = current_sf$TMSID2) |>
              tidyr::pivot_longer(cols = dplyr::starts_with("mean"), names_to = "discard", values_to = paste0("blh_", buffer_m, "m")) |>
              dplyr::group_by(TMSID, TMSID2) |>
              dplyr::mutate(date = unique_dates[1:dplyr::n()]) |>
              dplyr::ungroup() |>
              dplyr::select(TMSID, TMSID2, date, dplyr::all_of(paste0("blh_", buffer_m, "m")))
          })

          final_mon <- Reduce(function(x, y) dplyr::left_join(x, y, by = c("TMSID", "TMSID2", "date")), res_list) |>
            dplyr::filter(
              !is.na(date),
              format(date, "%Y-%m") == chr_months_spatial
            )

          return(final_mon)
          
        }, error = function(e) {
          message(paste("Error in BLH branch", chr_months_spatial, ":", e$message))
          return(data.frame())
        })
      },

      pattern = map(chr_months_spatial, sf_monitors_correct_month),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_15")
      )
    ),
    
    # F11. Merged (daily)
    targets::tar_target(
      name = df_feat_correct_merged_daily,
      command = {
        curr_mon_str <- chr_months_spatial
        curr_yr <- as.numeric(substr(curr_mon_str, 1, 4))
        base_data <- sf_monitors_correct_month
        if (is.null(base_data) || nrow(base_data) == 0) return(NULL)

        res_daily <- base_data %>% 
          dplyr::mutate(year = curr_yr)
        
        dynamic_list <- list(
          df_feat_correct_era5_land_daily,
          df_feat_correct_aod_daily,
          df_feat_correct_era5_blh_daily
        )
        
        for (feat_df in dynamic_list) {
          if (!is.null(feat_df) && nrow(feat_df) > 0) {
            res_daily <- res_daily %>%
              dplyr::left_join(feat_df, by = c("TMSID", "TMSID2", "date"))
          }
        }

        yearly_feat_raw <- if (is.data.frame(df_feat_correct_merged)) {
          list(df_feat_correct_merged)
        } else {
          Filter(
            function(x) {
              is.data.frame(x) &&
                "year" %in% names(x) &&
                any(x$year == curr_yr)
            },
            df_feat_correct_merged
          )
        }
        
        if (length(yearly_feat_raw) > 0) {
          static_feat <- dplyr::bind_rows(yearly_feat_raw) %>%
            dplyr::filter(year == curr_yr) %>%
            dplyr::select(
              TMSID, TMSID2, year,
              d_road, dem, dsm, gw_emission, 
              starts_with("landuse_"), 
              mtpi, mtpi_1km
            )
          
          res_daily <- res_daily %>% 
            dplyr::left_join(static_feat, by = c("TMSID", "TMSID2", "year"))
        }
        
        res_daily %>% sf::st_drop_geometry()
      },
      pattern = map(
        chr_months_spatial, 
        sf_monitors_correct_month, 
        df_feat_correct_era5_land_daily, 
        df_feat_correct_aod_daily, 
        df_feat_correct_era5_blh_daily
      ),
      iteration = "list"
    )
   )
