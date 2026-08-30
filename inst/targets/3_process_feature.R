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
          .SDcols = cols_to_fix
        ]
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
            # date_start = ifelse(is.na(date_start), dplyr::lead(date_end), date_start)#,
            date_end = ifelse(is.na(date_end), dplyr::lead(date_end), date_end)
          ) |>
          dplyr::ungroup() |>
          dplyr::filter(!is.na(date_start)) |>
          dplyr::mutate(
            date_start = as.POSIXct(date_start, tz = "Asia/Seoul"),
            date_end = as.POSIXct(date_end, tz = "Asia/Seoul")
          )

        check_lookup <-
          c(
            "[도시대기측정망]", "[도로변대기측정망]", "[PM2.5성분측정망]", "[교외대기측정망]",
            "[항만측정망]", "[국가배경농도(도서)측정망]", "[대기오염집중측정망]"
          )
        target_lookup <-
          c(
            "Urban", "Roadside", "PM2.5", "Suburban",
            "Port", "Island", "Concentrated"
          )

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
              yend <- lubridate::year(df$date_end)
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
        sites_orig$date_start <- as.Date(unclass(sites_orig$date_start), origin = "1970-01-01")
        sites_orig$date_end <- as.Date(unclass(sites_orig$date_end), origin = "1970-01-01")
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
        if (!reticulate::py_module_available("ee")) {
          warning("earthengine-api is not available in the active Python; returning empty wind features")
          return(data.frame(
            TMSID = character(),
            TMSID2 = character(),
            date = as.Date(character()),
            wind_speed_10m = numeric(),
            wind_dir_deg = numeric()
          ))
        }
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
        if (!reticulate::py_module_available("ee")) {
          warning("earthengine-api is not available in the active Python; returning NA building-density features")
          return(
            points_use |>
              sf::st_drop_geometry() |>
              dplyr::transmute(
                TMSID = as.character(TMSID),
                TMSID2 = as.character(TMSID2),
                year = as.integer(year),
                building_density = NA_real_
              )
          )
        }
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
        preprocessed_file <- file.path(chr_dir_data, "landuse", "glc_freq_2022.tif")
        if (file.exists(preprocessed_file)) {
          return(preprocessed_file)
        }

        landuse_ras <-
          terra::rast(chr_landuse_files[length(chr_landuse_files)]) |>
          terra::crop(terra::ext(124, 132.5, 33, 38.6))

        flt7 <-
          matrix(
            c(
              0, 0, 1, 1, 1, 0, 0,
              0, 1, 1, 1, 1, 1, 0,
              1, 1, 1, 1, 1, 1, 1,
              1, 1, 1, 1, 1, 1, 1,
              1, 1, 1, 1, 1, 1, 1,
              0, 1, 1, 1, 1, 1, 0,
              0, 0, 1, 1, 1, 0, 0
            ),
            nrow = 7, ncol = 7, byrow = TRUE
          )
        ras_res <-
          huimori::rasterize_freq(
            ras = landuse_ras,
            mat = flt7
          )
        terra::writeRaster(
          x = ras_res,
          filename = preprocessed_file,
          overwrite = TRUE
        )
        preprocessed_file
      }
    )
  )


## Grid processing for prediction ####
list_process_split <-
  list(
    targets::tar_target(
      name = int_grid_size,
      command = c(30L), # , 100, 250, 500),
      iteration = "list"
    ),
    targets::tar_target(
      name = int_size_split,
      command = c(70L), # , 20L, 10L, 2L),
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
      },
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
    ) # ,
    # targets::tar_target(
    #   list_pred_calc_grid_old,
    #   command = {
    #     grid_unit <- sf::st_bbox(sf_grid_correct_split[int_split_grid_ids, ])
    #     sf::st_as_sf(
    #       sf_grid_size |>
    #         dplyr::filter(
    #           (X <= grid_unit[3] & X >= grid_unit[1]) &
    #           (Y <= grid_unit[4] & Y >= grid_unit[2])
    #         ),
    #       coords = c("X", "Y"),
    #       crs = 5179,
    #       remove = FALSE
    #     )
    #   },
    #   iteration = "list",
    #   pattern = map(int_split_grid_ids),
    #   description = "Split prediction grid into list by chopin grid",
    #   resources = targets::tar_resources(
    #     crew = targets::tar_resources_crew(controller = "controller_15")
    #   )
    # )
  )


## Annualized feature calculation ####
extract_landuse_fraction_debug <-
  function(
    x = NULL,
    y = NULL,
    id = NULL,
    func = "frac",
    extent = NULL,
    radius = NULL,
    out_class = "sf",
    kernel = NULL,
    kernel_func = stats::weighted.mean,
    bandwidth = NULL,
    max_cells = 3e+07,
    .standalone = TRUE,
    log_event = function(event, radius = NA_real_, extra = "") NULL
  ) {
    check_raster <- getFromNamespace(".check_raster", "chopin")
    check_vector <- getFromNamespace(".check_vector", "chopin")
    reproject_to_raster <- getFromNamespace("reproject_to_raster", "chopin")
    dep_check <- getFromNamespace("dep_check", "chopin")
    dep_switch <- getFromNamespace("dep_switch", "chopin")
    kernel_weighting <- getFromNamespace(".kernel_weighting", "chopin")

    log_event(".check_raster start", radius = radius)
    x <- check_raster(x, extent = extent)
    log_event(".check_raster end", radius = radius)

    if (.standalone) {
      log_event(".check_vector start", radius = radius)
      y <- check_vector(
        y,
        extent = extent,
        out_class = out_class,
        subject_id = id
      )
      log_event(".check_vector end", radius = radius)

      log_event("reproject_to_raster start", radius = radius)
      y <- reproject_to_raster(vector = y, raster = x)
      log_event("reproject_to_raster end", radius = radius)

      if (dep_check(y) == "terra") {
        y <- dep_switch(y)
      }
    }

    if (!is.null(radius)) {
      ygeom <- tolower(as.character(sf::st_geometry_type(y)))
      if (!all(grepl("point", ygeom))) {
        cli::cli_warn("Buffer is set with non-point geometries.")
      }
      if (sf::st_is_longlat(y)) {
        stop(
          "Refusing to buffer landuse geometries in a longitude/latitude CRS. ",
          "Create meter-based buffers before transforming to the raster CRS."
        )
      }
      log_event("st_buffer start", radius = radius)
      y <- sf::st_buffer(y, radius, nQuadSegs = 90L)
      log_event("st_buffer end", radius = radius)
    }

    iskernel <- !is.null(kernel)
    log_event("exactextractr::exact_extract start", radius = radius)
    exact_extract_args <- list(
      x = x,
      y = y,
      fun = if (iskernel) NULL else func,
      force_df = TRUE,
      stack_apply = !iskernel,
      append_cols = if (iskernel) NULL else id,
      include_cols = if (iskernel) id else NULL,
      progress = FALSE,
      include_area = iskernel,
      include_xy = iskernel
    )
    if (!is.null(max_cells)) {
      exact_extract_args$max_cells_in_memory <- max_cells
    }
    extracted <- do.call(exactextractr::exact_extract, exact_extract_args)
    log_event(
      "exactextractr::exact_extract end",
      radius = radius,
      extra = paste0("nrow=", nrow(extracted), " ncol=", ncol(extracted))
    )

    if (iskernel) {
      stopifnot(!is.null(bandwidth))
      cli::cli_inform(sprintf(
        "Kernel function [ %s ] is applied to calculate weights...",
        kernel
      ))
      extracted <- kernel_weighting(
        x_ras = x,
        y_vec = y,
        id = id,
        extracted = extracted,
        kernel = kernel,
        kernel_func = kernel_func,
        bandwidth = bandwidth
      )
    }

    log_event("return start", radius = radius)
    log_event("return end", radius = radius)
    return(extracted)
  }

select_road_file_for_year <- function(road_files, year) {
  year <- unique(as.integer(year))
  if (length(year) != 1L || is.na(year)) {
    stop("Expected exactly one feature year for road file selection.")
  }
  road_years <- suppressWarnings(
    as.integer(sub("^([0-9]{4})_NODELINK$", "\\1", basename(dirname(road_files))))
  )
  idx <- which(road_years == year)
  if (length(idx) != 1L) {
    stop(
      "Expected exactly one MOCT_LINK.shp for year ", year,
      "; matched ", length(idx), " files: ",
      paste(road_files[idx], collapse = ", ")
    )
  }
  road_files[[idx]]
}

normalize_road_type <- function(x) {
  x <- trimws(as.character(x))
  digit_idx <- grepl("^[0-9]+$", x)
  x[digit_idx] <- sprintf("%03d", as.integer(x[digit_idx]))
  x
}

standardize_road_columns <- function(road, road_file = NA_character_) {
  column_map <- list(
    ROAD_TYPE = c("ROAD_TYPE", "ROAD_TYPE_"),
    ROAD_USE = c("ROAD_USE", "ROAD_USE_")
  )

  for (standard_name in names(column_map)) {
    if (!standard_name %in% names(road)) {
      source_name <- intersect(column_map[[standard_name]], names(road))
      source_name <- setdiff(source_name, standard_name)
      if (length(source_name) != 1L) {
        stop(
          "Could not standardize road column ", standard_name,
          " for file ", road_file,
          ". Candidate columns present: ",
          paste(intersect(column_map[[standard_name]], names(road)), collapse = ", ")
        )
      }
      road[[standard_name]] <- road[[source_name]]
    }
  }

  missing_cols <- setdiff(c("ROAD_TYPE", "ROAD_USE"), names(road))
  if (length(missing_cols) > 0L) {
    stop(
      "Road file is missing required columns after standardization: ",
      paste(missing_cols, collapse = ", "),
      ". File: ", road_file
    )
  }

  road |>
    dplyr::mutate(
      ROAD_TYPE = normalize_road_type(ROAD_TYPE),
      ROAD_USE = suppressWarnings(as.integer(as.character(ROAD_USE)))
    )
}

load_filtered_road_for_year <- function(road_files, year, target_crs) {
  road_file <- select_road_file_for_year(
    road_files = road_files,
    year = year
  )
  road <- sf::st_read(road_file, quiet = TRUE)
  road <- standardize_road_columns(road, road_file = road_file)

  filter_before <- nrow(road)
  road <-
    road |>
    dplyr::filter(!ROAD_TYPE %in% c("002", "004") & ROAD_USE == 0)
  filter_after <- nrow(road)
  if (filter_after < 1L) {
    stop("Road filter removed all rows for year ", year, ". File: ", road_file)
  }

  road <- sf::st_transform(road, target_crs)
  if (sf::st_is_longlat(road)) {
    stop("Road distance CRS must be projected, not longitude/latitude.")
  }

  attr(road, "road_file") <- road_file
  attr(road, "filter_before") <- filter_before
  attr(road, "filter_after") <- filter_after
  road
}

extract_nearest_road_distance <- function(points_sf, road, id_cols) {
  missing_id_cols <- setdiff(id_cols, names(points_sf))
  if (length(missing_id_cols) > 0L) {
    stop("Road distance input is missing id columns: ", paste(missing_id_cols, collapse = ", "))
  }

  points_metric <- sf::st_transform(points_sf, sf::st_crs(road))
  if (sf::st_is_longlat(points_metric)) {
    stop("Road distance points must be in a projected CRS.")
  }

  nearest_idx <- sf::st_nearest_feature(
    x = points_metric,
    y = road
  )
  road_nearest <- road[nearest_idx, ]
  dist_road_nearest <-
    sf::st_distance(
      x = points_metric,
      y = road_nearest,
      by_element = TRUE
    )

  points_sf |>
    dplyr::select(dplyr::all_of(id_cols)) |>
    dplyr::mutate(d_road = as.numeric(dist_road_nearest)) |>
    sf::st_drop_geometry()
}

landuse_fixed_classes <- c(
  0, 10, 11, 20, 51, 52, 61, 62, 71, 72, 81, 82,
  91, 120, 130, 140, 150, 181, 182, 183, 186, 187,
  190, 200, 210
)

landuse_fixed_terms <- function(radii) {
  radii <- as.numeric(unlist(radii))
  unlist(
    lapply(radii, \(radius_i) {
      paste0("landuse_frac_", landuse_fixed_classes, "_", radius_i)
    }),
    use.names = FALSE
  )
}

select_landuse_buffer_crs <- function(points_sf, fallback_crs = 5179) {
  point_crs <- sf::st_crs(points_sf)
  if (is.na(point_crs)) {
    stop("Landuse input points must have a valid CRS.")
  }
  if (!sf::st_is_longlat(points_sf)) {
    return(point_crs)
  }
  sf::st_crs(fallback_crs)
}

select_metric_buffer_crs <- function(points_sf, fallback_crs = 5179, context = "buffer") {
  point_crs <- sf::st_crs(points_sf)
  if (is.na(point_crs)) {
    stop(context, " input points must have a valid CRS.")
  }
  if (!sf::st_is_longlat(points_sf)) {
    return(point_crs)
  }
  sf::st_crs(fallback_crs)
}

normalize_buffer_radii <- function(radii, context = "buffer") {
  radii <- as.numeric(unlist(radii))
  if (length(radii) < 1L || anyNA(radii) || any(radii < 0)) {
    stop("Expected one or more non-negative meter radii for ", context, ".")
  }
  unique(radii)
}

make_feature_buffer_set <- function(
  points_sf,
  radii,
  id_cols,
  buffer_crs = NULL,
  fallback_crs = 5179,
  row_col = ".feature_buffer_row",
  context = "feature buffer",
  log_event = function(event, radius = NA_real_, extra = "") NULL
) {
  radii <- normalize_buffer_radii(radii, context = context)
  missing_id_cols <- setdiff(id_cols, names(points_sf))
  if (length(missing_id_cols) > 0L) {
    stop(context, " input is missing id columns: ", paste(missing_id_cols, collapse = ", "))
  }
  if (row_col %in% names(points_sf)) {
    stop(context, " row column already exists in input: ", row_col)
  }
  if (is.null(buffer_crs)) {
    buffer_crs <- select_metric_buffer_crs(
      points_sf = points_sf,
      fallback_crs = fallback_crs,
      context = context
    )
  }

  points_metric <-
    points_sf |>
    dplyr::mutate(!!row_col := dplyr::row_number()) |>
    sf::st_transform(buffer_crs)
  if (sf::st_is_longlat(points_metric)) {
    stop(context, " CRS must be projected, not longitude/latitude.")
  }

  id_cols_extract <- c(id_cols, row_col)
  meta <-
    points_metric |>
    sf::st_drop_geometry() |>
    dplyr::select(dplyr::all_of(id_cols_extract))

  buffers <- stats::setNames(
    lapply(radii, function(radius_i) {
      log_event("st_buffer start", radius = radius_i)
      out <- sf::st_buffer(points_metric, dist = radius_i, nQuadSegs = 90L)
      log_event("st_buffer end", radius = radius_i)
      out |>
        dplyr::select(dplyr::all_of(id_cols_extract))
    }),
    as.character(radii)
  )

  list(
    radii = radii,
    id_cols = id_cols,
    row_col = row_col,
    buffer_crs = sf::st_crs(points_metric),
    meta = meta,
    buffers = buffers
  )
}

get_feature_buffer <- function(buffer_set, radius, id_cols, context = "feature buffer") {
  if (!is.list(buffer_set) || is.null(buffer_set$buffers) || is.null(buffer_set$meta)) {
    stop(context, " must be a feature buffer set created by make_feature_buffer_set().")
  }
  missing_id_cols <- setdiff(id_cols, buffer_set$id_cols)
  if (length(missing_id_cols) > 0L) {
    stop(context, " does not contain id columns: ", paste(missing_id_cols, collapse = ", "))
  }
  radius_key <- as.character(as.numeric(radius))
  if (!radius_key %in% names(buffer_set$buffers)) {
    stop(context, " does not contain radius ", radius_key, ".")
  }
  buffer_set$buffers[[radius_key]]
}

buffer_set_meta <- function(buffer_set, id_cols, context = "feature buffer") {
  if (!is.list(buffer_set) || is.null(buffer_set$meta) || is.null(buffer_set$row_col)) {
    stop(context, " must be a feature buffer set created by make_feature_buffer_set().")
  }
  required_cols <- c(id_cols, buffer_set$row_col)
  missing_cols <- setdiff(required_cols, names(buffer_set$meta))
  if (length(missing_cols) > 0L) {
    stop(context, " metadata is missing columns: ", paste(missing_cols, collapse = ", "))
  }
  buffer_set$meta |>
    dplyr::select(dplyr::all_of(required_cols))
}

add_feature_year_column <- function(df, year, id_cols_without_year) {
  year <- unique(as.integer(year))
  if (length(year) != 1L || is.na(year)) {
    stop("Expected exactly one feature year.")
  }
  feature_cols <- setdiff(names(df), id_cols_without_year)
  df |>
    dplyr::mutate(year = year, .after = dplyr::last_col()) |>
    dplyr::select(
      dplyr::all_of(id_cols_without_year),
      year,
      dplyr::all_of(feature_cols)
    )
}

yearly_buffer_mean_terms <- function(value_prefix, radii) {
  paste0(value_prefix, "_", as.numeric(unlist(radii)))
}

extract_yearly_buffer_mean <- function(
  points_sf,
  id_cols,
  feature_year,
  raster_file,
  value_prefix,
  buffer_radii_m,
  buffer_crs = NULL,
  fallback_crs = 5179,
  buffer_set = NULL
) {
  feature_year <- unique(as.integer(feature_year))
  if (length(feature_year) != 1L || is.na(feature_year)) {
    stop("Expected exactly one feature year for ", value_prefix, ".")
  }
  if (length(raster_file) != 1L || is.na(raster_file) || !file.exists(raster_file)) {
    stop("Yearly raster is not available for ", value_prefix, " year ", feature_year)
  }
  buffer_radii_m <- normalize_buffer_radii(buffer_radii_m, context = value_prefix)

  if (is.null(buffer_set)) {
    buffer_set <- make_feature_buffer_set(
      points_sf = points_sf,
      radii = buffer_radii_m,
      id_cols = id_cols,
      buffer_crs = buffer_crs,
      fallback_crs = fallback_crs,
      row_col = ".yearly_buffer_row",
      context = value_prefix
    )
  }

  raster_obj <- terra::rast(raster_file)
  row_col <- buffer_set$row_col
  id_cols_extract <- c(id_cols, row_col)
  meta <- buffer_set_meta(buffer_set, id_cols, context = value_prefix)

  extracted_by_radius <-
    lapply(buffer_radii_m, function(radius_i) {
      buffers <-
        get_feature_buffer(buffer_set, radius_i, id_cols, context = value_prefix)

      chunk_size <- as.integer(Sys.getenv("HUIMORI_EXACTEXTRACTR_CHUNK_SIZE", "20000"))
      if (length(chunk_size) != 1L || is.na(chunk_size) || chunk_size < 1L) {
        chunk_size <- 20000L
      }
      row_chunks <- split(
        seq_len(nrow(buffers)),
        ceiling(seq_len(nrow(buffers)) / chunk_size)
      )
      extracted <-
        lapply(row_chunks, function(idx) {
          buffers_raster_crs <- sf::st_transform(buffers[idx, ], terra::crs(raster_obj))
          out_i <- exactextractr::exact_extract(
            x = raster_obj,
            y = buffers_raster_crs,
            fun = "mean",
            weights = NULL,
            force_df = TRUE,
            append_cols = id_cols_extract
          )
          gc()
          out_i
        }) |>
        dplyr::bind_rows()
      if (!identical(as.integer(extracted[[row_col]]), as.integer(meta[[row_col]]))) {
        stop(value_prefix, " extraction row order changed at radius ", radius_i, ".")
      }
      col_name <- paste0(value_prefix, "_", radius_i)
      tibble::tibble(!!col_name := ifelse(is.nan(extracted$mean), 0, as.numeric(extracted$mean)))
    })

  out <-
    dplyr::bind_cols(
      meta |> dplyr::select(dplyr::all_of(id_cols)),
      do.call(dplyr::bind_cols, extracted_by_radius)
    )
  expected_cols <- c(id_cols, yearly_buffer_mean_terms(value_prefix, buffer_radii_m))
  if (!identical(names(out), expected_cols)) {
    stop(value_prefix, " output columns do not match the fixed radius schema.")
  }
  out
}

extract_grid_static_raster_feature <- function(
  x,
  grid_sf,
  feature_name,
  radius = 1e-6
) {
  grid_meta_cols <- c("gid", "x", "y", "layer")
  missing_grid_cols <- setdiff(grid_meta_cols, names(grid_sf))
  if (length(missing_grid_cols) > 0L) {
    stop(
      feature_name, " grid extraction is missing columns: ",
      paste(missing_grid_cols, collapse = ", ")
    )
  }
  extracted <-
    chopin::extract_at(
      x = x,
      y = grid_sf,
      radius = radius,
      force_df = TRUE
    )
  if (nrow(extracted) != nrow(grid_sf)) {
    stop(feature_name, " extraction row count does not match grid rows.")
  }
  if (!"mean" %in% names(extracted)) {
    stop(feature_name, " extraction did not return a mean column.")
  }

  dplyr::bind_cols(
    sf::st_drop_geometry(grid_sf) |>
      dplyr::select(dplyr::all_of(grid_meta_cols)),
    tibble::tibble(!!feature_name := as.numeric(extracted$mean))
  )
}

grid_landuse_chunk_key <- function(grid_sf) {
  grid_meta <- sf::st_drop_geometry(grid_sf)
  bbox <- round(as.numeric(sf::st_bbox(grid_sf)))
  layer_values <- if ("layer" %in% names(grid_meta)) {
    paste(sort(unique(as.character(grid_meta$layer))), collapse = "-")
  } else {
    "na"
  }
  key <- sprintf(
    "chunk_bbox%s_n%d_gid%s_%s_layer%s",
    paste(bbox, collapse = "_"),
    nrow(grid_meta),
    min(grid_meta$gid, na.rm = TRUE),
    max(grid_meta$gid, na.rm = TRUE),
    layer_values
  )
  gsub("[^A-Za-z0-9_-]+", "_", key)
}

select_landuse_file_for_feature_year <- function(landuse_files, feature_year) {
  landuse_files <- unlist(landuse_files)
  feature_year <- unique(as.integer(feature_year))
  if (length(feature_year) != 1L || is.na(feature_year)) {
    stop("Expected exactly one feature year for previous-year landuse file selection.")
  }
  landuse_year <- feature_year - 1L
  landuse_years <- vapply(
    landuse_files,
    function(path) {
      matches <- regmatches(
        basename(path),
        gregexpr("(?<![0-9])(19|20)[0-9]{2}(?![0-9])", basename(path), perl = TRUE)
      )[[1]]
      matches <- unique(as.integer(matches))
      if (length(matches) != 1L || is.na(matches)) {
        return(NA_integer_)
      }
      matches
    },
    integer(1)
  )
  idx <- which(landuse_years == landuse_year)
  if (length(idx) != 1L) {
    stop(
      "Expected exactly one previous-year landuse raster for feature year ",
      feature_year, " (landuse year ", landuse_year, ")",
      "; matched ", length(idx), " files: ",
      paste(landuse_files[idx], collapse = ", ")
    )
  }
  landuse_files[[idx]]
}

extract_fixed_landuse_fractions <- function(
  landuse_ras,
  points_sf,
  radii,
  id_cols,
  buffer_crs = NULL,
  buffer_set = NULL,
  log_event = function(event, radius = NA_real_, extra = "") NULL
) {
  radii <- normalize_buffer_radii(radii, context = "landuse")

  if (is.null(buffer_set)) {
    if (is.null(buffer_crs)) {
      buffer_crs <- select_landuse_buffer_crs(points_sf)
    }
    buffer_set <- make_feature_buffer_set(
      points_sf = points_sf,
      radii = radii,
      id_cols = id_cols,
      buffer_crs = buffer_crs,
      row_col = ".landuse_row",
      context = "landuse",
      log_event = log_event
    )
  }

  row_col <- buffer_set$row_col
  id_cols_extract <- c(id_cols, row_col)
  meta <- buffer_set_meta(buffer_set, id_cols, context = "landuse")

  extracted_by_radius <-
    lapply(radii, function(radius_i) {
      buffers_landuse_crs <-
        get_feature_buffer(buffer_set, radius_i, id_cols, context = "landuse") |>
        sf::st_transform(terra::crs(landuse_ras))

      landuse_ras_i <- terra::crop(landuse_ras, terra::ext(buffers_landuse_crs))
      log_event("extract_at start", radius = radius_i)
      extracted_i <-
        extract_landuse_fraction_debug(
          x = landuse_ras_i,
          y = buffers_landuse_crs,
          id = id_cols_extract,
          radius = NULL,
          func = "frac",
          .standalone = FALSE,
          log_event = log_event
        )
      log_event("extract_at end", radius = radius_i, extra = paste0("ncol=", ncol(extracted_i)))

      if (!identical(as.integer(extracted_i[[row_col]]), as.integer(meta[[row_col]]))) {
        stop("Landuse extraction row order changed at radius ", radius_i, ".")
      }

      frac_cols <- grep("^frac_", names(extracted_i), value = TRUE)
      extracted_i <- extracted_i[, frac_cols, drop = FALSE]
      names(extracted_i) <- paste0("landuse_", names(extracted_i), "_", radius_i)

      landuse_terms <- paste0("landuse_frac_", landuse_fixed_classes, "_", radius_i)
      missing_landuse_terms <- setdiff(landuse_terms, names(extracted_i))
      for (term in missing_landuse_terms) {
        extracted_i[[term]] <- 0
      }
      extracted_i[, landuse_terms, drop = FALSE]
    })

  df_res <-
    dplyr::bind_cols(
      meta |> dplyr::select(dplyr::all_of(id_cols)),
      do.call(dplyr::bind_cols, extracted_by_radius)
    )

  landuse_cols <- landuse_fixed_terms(radii)
  if (!identical(landuse_cols, setdiff(names(df_res), id_cols))) {
    stop("Landuse output columns do not match the fixed class/radius schema.")
  }
  df_res
}

merged_feature_predictor_terms <- function(radii) {
  c(
    "dsm", "dem", "d_road", "mtpi", "mtpi_1km",
    landuse_fixed_terms(radii),
    yearly_buffer_mean_terms("aod_yearly", radii),
    yearly_buffer_mean_terms("blh_yearly", radii)
  )
}

assert_required_columns <- function(df, cols, label) {
  missing_cols <- setdiff(cols, names(df))
  if (length(missing_cols) > 0L) {
    stop(
      label, " is missing required columns: ",
      paste(missing_cols, collapse = ", ")
    )
  }
  invisible(df)
}

assert_unique_key <- function(df, key, label) {
  assert_required_columns(df, key, label)
  df_key <- if (inherits(df, "sf")) {
    sf::st_drop_geometry(df)
  } else {
    df
  }
  df_key <- df_key[, key, drop = FALSE]
  duplicate_idx <- anyDuplicated(df_key)
  if (duplicate_idx > 0L) {
    stop(
      label, " has duplicated key rows for ",
      paste(key, collapse = ", "), ". First duplicate row: ", duplicate_idx
    )
  }
  invisible(df)
}

assert_single_year <- function(df, label) {
  assert_required_columns(df, "year", label)
  year_i <- unique(as.integer(df$year))
  if (length(year_i) != 1L || is.na(year_i)) {
    stop(label, " branch must contain exactly one year.")
  }
  year_i
}

write_correct_merged_parquet <- function(
  df,
  root_dir = file.path("daehoon", "outputs", "df_feat_correct_merged")
) {
  year_i <- assert_single_year(df, "df_feat_correct_merged")
  out_dir <- file.path(root_dir, paste0("year=", year_i))
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(
    x = df,
    sink = file.path(out_dir, "part.parquet")
  )
  invisible(file.path(out_dir, "part.parquet"))
}

list_process_feature <-
  list(
    ### F01. Distance to the nearest road ####
    targets::tar_target(
      name = df_feat_correct_d_road,
      command = {
        road <- load_filtered_road_for_year(
          road_files = chr_road_files,
          year = unique(sf_monitors_correct_yr$year),
          target_crs = sf::st_crs(sf_monitors_correct_yr)
        )
        extract_nearest_road_distance(
          points_sf = sf_monitors_correct_yr,
          road = road,
          id_cols = c("TMSID", "TMSID2", "year")
        )
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
      command = c(100, 500, 2000, 5000),
      iteration = "list"
    ),
    targets::tar_target(
      name = sf_buffer_correct_yr,
      command = {
        make_feature_buffer_set(
          points_sf = sf_monitors_correct_yr,
          radii = int_landuse_radius,
          id_cols = c("TMSID", "TMSID2", "year"),
          row_col = ".feature_buffer_row",
          context = "correct feature buffer"
        )
      },
      pattern = map(sf_monitors_correct_yr),
      iteration = "list",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_04")
      )
    ),
    targets::tar_target(
      name = df_feat_correct_landuse,
      command = {
        year_i <- unique(as.integer(sf_monitors_correct_yr$year))
        if (length(year_i) != 1L || is.na(year_i)) {
          stop("Expected exactly one year in sf_monitors_correct_yr.")
        }
        landuse_file <- select_landuse_file_for_feature_year(
          landuse_files = chr_landuse_files,
          feature_year = year_i
        )

        df_landuse <- extract_fixed_landuse_fractions(
          landuse_ras = terra::rast(landuse_file),
          points_sf = sf_monitors_correct_yr,
          radii = int_landuse_radius,
          id_cols = c("TMSID", "TMSID2", "year"),
          buffer_set = sf_buffer_correct_yr
        )

        parquet_dir <- file.path("daehoon", "outputs", "landuse_correct_parquet")
        dir.create(parquet_dir, recursive = TRUE, showWarnings = FALSE)
        parquet_file <- file.path(
          parquet_dir,
          sprintf("df_feat_correct_landuse_%d.parquet", year_i)
        )
        arrow::write_parquet(df_landuse, parquet_file)

        df_landuse
      },
      pattern = map(sf_monitors_correct_yr, sf_buffer_correct_yr),
      iteration = "list",
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
            영업상태구분코드 == "01"
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
            영업상태구분코드 == "01"
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
      name = rast_aod_yearly,
      command = {
        build_aod_yearly_from_daily_cubes(
          cubes = rast_aod_daily,
          year = int_aod_year_chunks
        )
      },
      pattern = map(int_aod_year_chunks),
      iteration = "vector",
      format = "file",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_correct_aod_yearly,
      command = {
        extract_yearly_buffer_mean(
          points_sf = sf_monitors_correct_yr,
          id_cols = c("TMSID", "TMSID2", "year"),
          feature_year = int_aod_year_chunks,
          raster_file = rast_aod_yearly,
          value_prefix = "aod_yearly",
          buffer_radii_m = int_landuse_radius,
          buffer_set = sf_buffer_correct_yr
        )
      },
      pattern = map(sf_monitors_correct_yr, sf_buffer_correct_yr, int_aod_year_chunks, rast_aod_yearly),
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
        if (length(chr_file_chelsa) == 0) {
          warning("No CHELSA files found for year ", int_year_chelsa)
          return(data.frame())
        }

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
      name = rast_blh_yearly,
      command = {
        build_blh_yearly_from_daily_cubes(
          cubes = rast_blh_daily,
          year = int_aod_year_chunks
        )
      },
      pattern = map(int_aod_year_chunks),
      iteration = "vector",
      format = "file",
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_correct_blh_yearly,
      command = {
        extract_yearly_buffer_mean(
          points_sf = sf_monitors_correct_yr,
          id_cols = c("TMSID", "TMSID2", "year"),
          feature_year = int_aod_year_chunks,
          raster_file = rast_blh_yearly,
          value_prefix = "blh_yearly",
          buffer_radii_m = int_landuse_radius,
          buffer_set = sf_buffer_correct_yr
        )
      },
      pattern = map(sf_monitors_correct_yr, sf_buffer_correct_yr, int_aod_year_chunks, rast_blh_yearly)
    ),
    ### F11. Merge features ####
    targets::tar_target(
      name = df_feat_correct_merged,
      command = {
        key_cols <- c("TMSID", "TMSID2", "year")
        feature_inputs <-
          list(
            sf_monitors_correct_yr = sf_monitors_correct_yr,
            df_feat_correct_d_road = df_feat_correct_d_road,
            df_feat_correct_dem = df_feat_correct_dem,
            df_feat_correct_dsm = df_feat_correct_dsm,
            df_feat_correct_mtpi = df_feat_correct_mtpi,
            df_feat_correct_mtpi_1km = df_feat_correct_mtpi_1km,
            df_feat_correct_landuse = df_feat_correct_landuse,
            df_feat_correct_aod_yearly = df_feat_correct_aod_yearly,
            df_feat_correct_blh_yearly = df_feat_correct_blh_yearly
          )
        for (input_name in names(feature_inputs)) {
          assert_unique_key(feature_inputs[[input_name]], key_cols, input_name)
        }
        base_n <- nrow(sf_monitors_correct_yr)

        df_res <-
          purrr::reduce(
            feature_inputs,
            .f = collapse::join,
            on = key_cols
          ) %>%
          dplyr::mutate(
            d_road = as.numeric(d_road),
            dsm = as.numeric(dsm),
            dem = as.numeric(dem),
            mtpi = as.numeric(mtpi),
            mtpi_1km = as.numeric(mtpi_1km)
          ) %>%
          sf::st_drop_geometry()
        if (nrow(df_res) != base_n) {
          stop(
            "df_feat_correct_merged row count changed after merge: base=",
            base_n, ", merged=", nrow(df_res)
          )
        }
        names(df_res) <- sub("mean.", "", names(df_res))
        df_res <- df_res %>%
          dplyr::relocate(
            any_of(c("year", "ndays", "PM10", "PM25")),
            .after = TMSID2
          )
        assert_unique_key(df_res, key_cols, "df_feat_correct_merged")
        assert_required_columns(
          df_res,
          merged_feature_predictor_terms(int_landuse_radius),
          "df_feat_correct_merged"
        )
        emitter_cols <- grep("emitt|emission|gw_emission", names(df_res), value = TRUE)
        if (length(emitter_cols) > 0L) {
          stop(
            "df_feat_correct_merged contains emitter columns after emitters removal: ",
            paste(emitter_cols, collapse = ", ")
          )
        }
        write_correct_merged_parquet(df_res)
        df_res
      },
      pattern = map(
        sf_monitors_correct_yr,
        df_feat_correct_d_road,
        df_feat_correct_dem,
        df_feat_correct_dsm,
        df_feat_correct_landuse,
        df_feat_correct_mtpi,
        df_feat_correct_mtpi_1km,
        df_feat_correct_aod_yearly,
        df_feat_correct_blh_yearly
      )
    ),
    targets::tar_target(
      name = df_feat_correct_merged_old,
      command = {
        df_feat_correct_merged
      }
    ),
    # Incorrect addresses
    targets::tar_target(
      name = df_feat_incorrect_d_road,
      command = {
        road <- purrr::map(chr_road_files, \(x) {
          sf::st_read(x, quiet = TRUE) |>
            sf::st_transform(5179)
        }) |>
          dplyr::bind_rows()
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
        base <- sf_monitors_incorrect |>
          sf::st_drop_geometry()

        landuse_names <- names(df_feat_incorrect_landuse[[1]])
        landuse_by_row <- as.data.frame(
          matrix(NA_real_, nrow = nrow(base), ncol = length(landuse_names))
        )
        names(landuse_by_row) <- landuse_names

        for (i in seq_along(df_feat_incorrect_landuse)) {
          idx <- base$year == int_years_spatial[[i]]
          landuse_by_row[idx, ] <- df_feat_incorrect_landuse[[i]][idx, landuse_names, drop = FALSE]
        }

        df_res <-
          base |>
          dplyr::bind_cols(landuse_by_row) |>
          dplyr::mutate(
            d_road = as.numeric(df_feat_incorrect_d_road$d_road) / 1000,
            dsm = as.numeric(df_feat_incorrect_dsm$mean),
            dem = as.numeric(df_feat_incorrect_dem$mean),
            building_density = as.numeric(df_feat_incorrect_building_density$building_density),
            gw_emission = as.numeric(df_feat_incorrect_emittors$gw_emission),
            mtpi = as.numeric(df_feat_incorrect_mtpi$mean),
            mtpi_1km = as.numeric(df_feat_incorrect_mtpi_1km$mean)
          )
        names(df_res) <- sub("mean.", "", names(df_res))
        df_res
      }
    ),
    # Grid point features
    targets::tar_target(
      name = df_feat_grid_d_road,
      command = {
        road <- load_filtered_road_for_year(
          road_files = chr_road_files,
          year = int_aod_year_chunks,
          target_crs = sf::st_crs(list_pred_calc_grid)
        )
        grid_use <-
          list_pred_calc_grid |>
          dplyr::mutate(year = as.integer(int_aod_year_chunks))
        extract_nearest_road_distance(
          points_sf = grid_use,
          road = road,
          id_cols = c("gid", "x", "y", "layer", "year")
        )
      },
      iteration = "list",
      pattern = cross(
        map(list_pred_calc_grid),
        map(int_aod_year_chunks)
      ),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_dsm,
      command = {
        extract_grid_static_raster_feature(
          x = chr_dsm_file,
          grid_sf = list_pred_calc_grid,
          feature_name = "dsm"
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
        extract_grid_static_raster_feature(
          x = chr_dem_file,
          grid_sf = list_pred_calc_grid,
          feature_name = "dem"
        )
      },
      iteration = "list",
      pattern = map(list_pred_calc_grid),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = sf_buffer_grid,
      command = {
        make_feature_buffer_set(
          points_sf = list_pred_calc_grid,
          radii = int_landuse_radius,
          id_cols = c("gid", "x", "y", "layer"),
          row_col = ".feature_buffer_row",
          context = "grid feature buffer"
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
        year_i <- int_aod_year_chunks
        landuse_file <- select_landuse_file_for_feature_year(
          landuse_files = chr_landuse_files,
          feature_year = year_i
        )
        if (length(landuse_file) != 1 || is.na(landuse_file) || !file.exists(landuse_file)) {
          stop("Landuse raster is not available for year ", year_i)
        }
        landuse_ras_full <- terra::rast(landuse_file)
        grid_use <-
          list_pred_calc_grid |>
          dplyr::mutate(year = as.integer(year_i))

        df_landuse <- extract_fixed_landuse_fractions(
          landuse_ras = landuse_ras_full,
          points_sf = grid_use,
          radii = int_landuse_radius,
          id_cols = c("gid", "x", "y", "layer"),
          buffer_set = sf_buffer_grid
        ) |>
          add_feature_year_column(
            year = year_i,
            id_cols_without_year = c("gid", "x", "y", "layer")
          )

        parquet_dir <- file.path("daehoon", "outputs", "landuse_grid_parquet")
        dir.create(parquet_dir, recursive = TRUE, showWarnings = FALSE)
        parquet_file <- file.path(
          parquet_dir,
          sprintf(
            "df_feat_grid_landuse_%s_%d.parquet",
            grid_landuse_chunk_key(list_pred_calc_grid),
            as.integer(year_i)
          )
        )
        arrow::write_parquet(df_landuse, parquet_file)

        df_landuse
      },
      iteration = "list",
      pattern = cross(
        map(list_pred_calc_grid, sf_buffer_grid),
        map(int_aod_year_chunks)
      ),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_mtpi,
      command = {
        mtpi_ras <- terra::rast(chr_mtpi_file)
        extract_grid_static_raster_feature(
          x = mtpi_ras,
          grid_sf = list_pred_calc_grid,
          feature_name = "mtpi"
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
        extract_grid_static_raster_feature(
          x = mtpi_ras,
          grid_sf = list_pred_calc_grid,
          feature_name = "mtpi_1km"
        )
      },
      iteration = "list",
      pattern = map(list_pred_calc_grid),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_static_topo_yearly,
      command = {
        align_static_grid_feature <- function(base, feature, feature_col, feature_name) {
          required_cols <- c("gid", "x", "y", "layer", feature_col)
          assert_required_columns(base, c("gid", "x", "y", "layer"), "df_feat_grid_dem")
          assert_required_columns(feature, required_cols, feature_name)
          if (anyDuplicated(base["gid"]) || anyDuplicated(feature["gid"])) {
            stop("gid must be unique before merging ", feature_name, ".")
          }
          idx <- match(base$gid, feature$gid)
          if (anyNA(idx)) {
            stop(feature_name, " feature is missing gid rows from df_feat_grid_dem.")
          }
          aligned <- feature[idx, , drop = FALSE]
          for (coord_col in c("x", "y", "layer")) {
            if (!identical(as.vector(base[[coord_col]]), as.vector(aligned[[coord_col]]))) {
              stop(feature_name, " feature ", coord_col, " values are not aligned.")
            }
          }
          aligned[, feature_col, drop = FALSE] |>
            dplyr::mutate(dplyr::across(dplyr::all_of(feature_col), as.numeric))
        }

        n_grid <- nrow(df_feat_grid_dem)
        feature_nrows <- c(
          dsm = nrow(df_feat_grid_dsm),
          mtpi = nrow(df_feat_grid_mtpi),
          mtpi_1km = nrow(df_feat_grid_mtpi_1km)
        )
        if (any(feature_nrows != n_grid)) {
          stop(
            "Static grid feature row counts are not aligned: ",
            paste(names(feature_nrows), feature_nrows, sep = "=", collapse = ", ")
          )
        }

        df_res <-
          df_feat_grid_dem |>
          dplyr::select(gid, x, y, layer, dem) |>
          dplyr::mutate(dem = as.numeric(dem)) |>
          dplyr::bind_cols(
            align_static_grid_feature(
              df_feat_grid_dem,
              df_feat_grid_dsm,
              "dsm",
              "df_feat_grid_dsm"
            ),
            align_static_grid_feature(
              df_feat_grid_dem,
              df_feat_grid_mtpi,
              "mtpi",
              "df_feat_grid_mtpi"
            ),
            align_static_grid_feature(
              df_feat_grid_dem,
              df_feat_grid_mtpi_1km,
              "mtpi_1km",
              "df_feat_grid_mtpi_1km"
            )
          ) |>
          dplyr::mutate(year = as.integer(int_aod_year_chunks))

        assert_required_columns(
          df_res,
          c("gid", "x", "y", "layer", "year", "dsm", "dem", "mtpi", "mtpi_1km"),
          "df_feat_grid_static_topo_yearly"
        )
        if (nrow(df_res) != n_grid) {
          stop(
            "df_feat_grid_static_topo_yearly row count changed after merge: base=",
            n_grid, ", merged=", nrow(df_res)
          )
        }
        if (anyDuplicated(df_res[c("gid", "year")])) {
          stop("df_feat_grid_static_topo_yearly must be unique by gid/year.")
        }

        df_res
      },
      iteration = "list",
      pattern = cross(
        map(
          df_feat_grid_dem,
          df_feat_grid_dsm,
          df_feat_grid_mtpi,
          df_feat_grid_mtpi_1km
        ),
        map(int_aod_year_chunks)
      ),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_aod_yearly,
      command = {
        grid_use <-
          list_pred_calc_grid |>
          dplyr::mutate(year = as.integer(int_aod_year_chunks))
        extract_yearly_buffer_mean(
          points_sf = grid_use,
          id_cols = c("gid", "x", "y", "layer"),
          feature_year = int_aod_year_chunks,
          raster_file = rast_aod_yearly,
          value_prefix = "aod_yearly",
          buffer_radii_m = int_landuse_radius,
          buffer_set = sf_buffer_grid
        ) |>
          add_feature_year_column(
            year = int_aod_year_chunks,
            id_cols_without_year = c("gid", "x", "y", "layer")
          )
      },
      iteration = "list",
      pattern = cross(
        map(list_pred_calc_grid, sf_buffer_grid),
        map(int_aod_year_chunks, rast_aod_yearly)
      ),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_blh_yearly,
      command = {
        grid_use <-
          list_pred_calc_grid |>
          dplyr::mutate(year = as.integer(int_aod_year_chunks))
        extract_yearly_buffer_mean(
          points_sf = grid_use,
          id_cols = c("gid", "x", "y", "layer"),
          feature_year = int_aod_year_chunks,
          raster_file = rast_blh_yearly,
          value_prefix = "blh_yearly",
          buffer_radii_m = int_landuse_radius,
          buffer_set = sf_buffer_grid
        ) |>
          add_feature_year_column(
            year = int_aod_year_chunks,
            id_cols_without_year = c("gid", "x", "y", "layer")
          )
      },
      iteration = "list",
      pattern = cross(
        map(list_pred_calc_grid, sf_buffer_grid),
        map(int_aod_year_chunks, rast_blh_yearly)
      ),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
      )
    ),
    targets::tar_target(
      name = df_feat_grid_emittors,
      command = {
        if (!identical(Sys.getenv("COMPUTE_GRID_EMISSIONS"), "true")) {
          warning("Skipping grid emissions; set COMPUTE_GRID_EMISSIONS=true to compute gw_emission.")
          return(
            list_pred_calc_grid |>
              sf::st_drop_geometry() |>
              dplyr::mutate(gw_emission = NA_real_)
          )
        }
        row_group <- ceiling(seq_len(nrow(list_pred_calc_grid)) / 1000)
        purrr::map(
          split(seq_len(nrow(list_pred_calc_grid)), row_group),
          \(idx) {
            gw_emittors(
              input = list_pred_calc_grid[idx, ],
              target = sf_emission_locs,
              clip = sf_korea_watershed,
              wfun = "gaussian",
              bw = 5000,
              dist_method = "geodesic"
            )
          }
        ) |>
          dplyr::bind_rows()
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
        n_grid <- nrow(df_feat_grid_d_road)
        year_i <- unique(as.integer(df_feat_grid_d_road$year))
        if (length(year_i) != 1L || is.na(year_i)) {
          stop("df_feat_grid_d_road must contain exactly one feature year.")
        }
        assert_required_columns(
          df_feat_grid_d_road,
          c("gid", "x", "y", "layer", "year"),
          "df_feat_grid_d_road"
        )
        if (anyDuplicated(df_feat_grid_d_road[c("gid", "year")])) {
          stop("df_feat_grid_d_road must be unique by gid/year.")
        }

        align_dynamic_grid_feature <- function(base, feature, feature_cols, feature_name) {
          required_cols <- c("gid", "x", "y", "layer", "year", feature_cols)
          missing_base_cols <- setdiff(c("gid", "x", "y", "layer", "year"), names(base))
          if (length(missing_base_cols) > 0L) {
            stop(
              "df_feat_grid_d_road is missing columns for ",
              feature_name, " merge: ",
              paste(missing_base_cols, collapse = ", ")
            )
          }
          missing_cols <- setdiff(required_cols, names(feature))
          if (length(missing_cols) > 0L) {
            stop(
              feature_name, " feature is missing columns: ",
              paste(missing_cols, collapse = ", ")
            )
          }
          feature_year <- unique(as.integer(feature$year))
          if (length(feature_year) != 1L || is.na(feature_year) || feature_year != year_i) {
            stop(feature_name, " feature year is not aligned with df_feat_grid_d_road.")
          }
          if (anyDuplicated(base[c("gid", "year")]) || anyDuplicated(feature[c("gid", "year")])) {
            stop("gid/year must be unique before merging ", feature_name, ".")
          }
          idx <- match(
            interaction(base$gid, base$year, drop = TRUE),
            interaction(feature$gid, feature$year, drop = TRUE)
          )
          if (anyNA(idx)) {
            stop(feature_name, " feature is missing gid/year rows from df_feat_grid_d_road.")
          }
          aligned <- feature[idx, , drop = FALSE]
          for (coord_col in c("x", "y", "layer")) {
            if (!identical(as.vector(base[[coord_col]]), as.vector(aligned[[coord_col]]))) {
              stop(feature_name, " feature ", coord_col, " values are not aligned.")
            }
          }
          aligned[, feature_cols, drop = FALSE]
        }

        landuse_cols <- landuse_fixed_terms(int_landuse_radius)
        landuse_aligned <- align_dynamic_grid_feature(
          df_feat_grid_d_road,
          df_feat_grid_landuse,
          landuse_cols,
          "df_feat_grid_landuse"
        )
        aod_aligned <- align_dynamic_grid_feature(
          df_feat_grid_d_road,
          df_feat_grid_aod_yearly,
          yearly_buffer_mean_terms("aod_yearly", int_landuse_radius),
          "df_feat_grid_aod_yearly"
        )
        blh_aligned <- align_dynamic_grid_feature(
          df_feat_grid_d_road,
          df_feat_grid_blh_yearly,
          yearly_buffer_mean_terms("blh_yearly", int_landuse_radius),
          "df_feat_grid_blh_yearly"
        )
        topo_aligned <- align_dynamic_grid_feature(
          df_feat_grid_d_road,
          df_feat_grid_static_topo_yearly,
          c("dsm", "dem", "mtpi", "mtpi_1km"),
          "df_feat_grid_static_topo_yearly"
        )

        df_res <-
          df_feat_grid_d_road |>
          dplyr::bind_cols(
            landuse_aligned,
            aod_aligned,
            blh_aligned,
            topo_aligned
          ) |>
          dplyr::mutate(
            year = as.integer(year_i),
            d_road = as.numeric(d_road)
          )
        if (nrow(df_res) != n_grid) {
          stop(
            "df_feat_grid_merged row count changed after merge: base=",
            n_grid, ", merged=", nrow(df_res)
          )
        }
        assert_required_columns(
          df_res,
          merged_feature_predictor_terms(int_landuse_radius),
          "df_feat_grid_merged"
        )
        if (anyDuplicated(df_res[c("gid", "year")])) {
          stop("df_feat_grid_merged must be unique by gid/year.")
        }

        metadata_cols <-
          intersect(
            c("gid", "x", "y", "layer", "year", "X", "Y", "longitude", "latitude", "lon", "lat"),
            names(df_res)
          )
        feature_cols <- setdiff(names(df_res), metadata_cols)
        df_res |>
          dplyr::select(dplyr::all_of(metadata_cols), dplyr::all_of(feature_cols))
      },
      iteration = "list",
      pattern = map(
        df_feat_grid_d_road,
        df_feat_grid_landuse,
        df_feat_grid_aod_yearly,
        df_feat_grid_blh_yearly,
        df_feat_grid_static_topo_yearly
      ),
      resources = targets::tar_resources(
        crew = targets::tar_resources_crew(controller = "controller_20")
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


## 2026.06.03
### list_pred_calc_grid_old 주석처리 (tar_make 과정에서 오류 일으킴)
