daily_smoke_scope <- function() {
  list(mode = "one-date-one-chunk", year = 2023L, date = "2023-01-01",
       chunk_id = "bbox_1002626_1585240_1003676_1586230_n846_gid_1_846",
       annual_grid_branch = "df_feat_grid_merged_6a39fdc7fc3c4815",
       train_month = "2023-01", pollutants = "PM10,PM25", trees = 20L,
       tree_depth = 3L, min_n = 5L, learn_rate = 0.1, nthread = 40L,
       expected_cells = 846L)
}

daily_smoke_file_snapshot <- function(paths) {
  paths <- unique(as.character(paths))
  if (!length(paths) || anyNA(paths) || !all(file.exists(paths))) stop("Required existing artifact is missing.")
  info <- file.info(paths)
  if (any(info$isdir) || anyNA(info$size) || any(info$size <= 0)) stop("Required artifact is empty/unreadable.")
  hash <- unname(tools::md5sum(paths))
  if (anyNA(hash)) stop("Required artifact could not be read for fingerprinting.")
  data.frame(path = paths, bytes = info$size, mtime = as.numeric(info$mtime), md5 = hash,
             stringsAsFactors = FALSE)
}

read_daily_smoke_inputs <- function(scope = daily_smoke_scope()) {
  if (!identical(scope, daily_smoke_scope())) stop("Only the authorized deterministic smoke scope is supported.")
  meta <- utils::read.delim("_targets/meta/meta", sep = "|", quote = "", colClasses = "character")
  read_names <- character()
  object_names <- character()
  children <- function(parent) {
    row <- meta[meta$name == parent, , drop = FALSE]
    if (nrow(row) != 1L || !nzchar(row$children)) stop("Required target metadata missing: ", parent)
    strsplit(row$children, "*", fixed = TRUE)[[1L]]
  }
  read_object <- function(name, parent = NULL) {
    row <- meta[meta$name == name, , drop = FALSE]
    if (nrow(row) != 1L || nzchar(row$error) ||
        (!is.null(parent) && !name %in% children(parent))) stop("Missing/failed/inactive branch: ", name)
    if (row$format != "file") {
      if (!file.exists(file.path("_targets", "objects", name))) stop("Required object missing: ", name)
      object_names <<- unique(c(object_names, name))
    }
    read_names <<- unique(c(read_names, name))
    targets::tar_read_raw(name, store = "_targets")
  }
  months <- read_object("chr_months_spatial")
  years <- read_object("int_years_spatial")
  mi <- match(scope$train_month, months)
  yi <- match(scope$year, years)
  if (is.na(mi) || is.na(yi)) stop("Pilot month/year absent from stored calendar.")
  annual_terms <- read_object("chr_terms_x")
  daily_model_predictor_terms(annual_terms)
  monitor_names <- children("df_feat_correct_merged_daily")
  annual_names <- children("df_feat_correct_merged")
  if (length(monitor_names) != length(months) || length(annual_names) != length(years)) {
    stop("Stored monitor/calendar branch cardinality mismatch.")
  }
  monitor <- read_object(monitor_names[[mi]], "df_feat_correct_merged_daily")
  annual_monitor <- read_object(annual_names[[yi]], "df_feat_correct_merged")
  daily_check_dates(monitor, month = scope$train_month)
  if (!setequal(monitor$date, daily_month_dates(scope$train_month))) stop("Incomplete January monitor dates.")
  if (!identical(unique(as.integer(annual_monitor$year)), scope$year)) stop("Annual monitor branch/year mismatch.")

  map_name <- "file_grid_raster_cell_412e541219649226"
  maps <- children("file_grid_raster_cell")
  ci <- match(map_name, maps)
  if (is.na(ci)) stop("Selected cell map is not an active branch.")
  cell_map_files <- read_object(map_name, "file_grid_raster_cell")
  grid_names <- children("list_pred_calc_grid")
  contract_names <- children("df_feat_grid_merged_daily")
  if (length(grid_names) != length(maps) || length(contract_names) != length(maps) * length(months)) {
    stop("Stored grid/contract branch cardinality mismatch.")
  }
  # The declared DAG is chunk x synchronized month. Candidate positions are
  # always verified against stored semantic identities before their use.
  contract_name <- contract_names[[(ci - 1L) * length(months) + mi]]
  contract <- read_object(contract_name, "df_feat_grid_merged_daily")
  if (!inherits(contract, "daily_grid_month_contract") ||
      !identical(contract$month, scope$train_month) || !identical(contract$chunk_id, scope$chunk_id) ||
      !identical(unname(contract$cell_map_file), unname(cell_map_files))) {
    stop("Selected stored contract date/chunk/cell-map mismatch.")
  }
  grid_sf <- read_object(grid_names[[ci]], "list_pred_calc_grid")
  if (!inherits(grid_sf, "sf") || !isTRUE(sf::st_crs(grid_sf) == sf::st_crs(5179)) ||
      !identical(daily_grid_chunk_key(grid_sf), scope$chunk_id)) stop("Stored grid chunk identity/CRS mismatch.")
  grid <- as.data.frame(sf::st_drop_geometry(grid_sf))
  if (nrow(grid) != scope$expected_cells) stop("Stored grid chunk count mismatch.")
  annual_grid <- read_object(scope$annual_grid_branch, "df_feat_grid_merged")
  template <- daily_canonical_template()
  annual_raster <- file.path("outputs", "prediction_maps", "rasters", "PM10", "2023_PM10.tif")
  if (!file.exists(annual_raster)) stop("Reference annual raster missing.")
  assert_daily_raster_geometry(terra::rast(annual_raster), template)
  validate_daily_grid_chunk(annual_grid, scope$chunk_id, grid, template)
  map_manifest <- validate_grid_daily_cell_map(cell_map_files, contract$sources)
  cell_map <- read_grid_daily_cell_map(cell_map_files)
  daily_assert_keys(cell_map, "gid")
  if (map_manifest$n_points != scope$expected_cells || !identical(cell_map$gid, grid$gid)) {
    stop("Grid/cell-map row or gid order mismatch.")
  }
  for (source in names(contract$sources)) {
    source_contract <- contract$sources[[source]]
    if (!identical(source_contract$month, scope$train_month) ||
        !identical(source_contract$source, source)) stop("Source contract month/source mismatch.")
    validate_daily_cube(source_contract$cube_files, source, scope$train_month)
    source_raster <- terra::rast(daily_cube_file(source_contract$cube_files))
    projected <- sf::st_transform(grid_sf, terra::crs(source_raster))
    expected <- terra::cellFromXY(source_raster, sf::st_coordinates(projected))
    if (anyNA(expected) || !identical(as.integer(expected), as.integer(cell_map[[paste0(source, "_cell")]]))) {
      stop("Native source-cell mapping differs from grid coordinates: ", source)
    }
  }
  files <- c("_targets/meta/meta", file.path("_targets", "objects", object_names), cell_map_files,
              unlist(lapply(contract$sources, `[[`, "cube_files"), use.names = FALSE), annual_raster)
  snapshot <- daily_smoke_file_snapshot(files)
  list(monitor = monitor, annual_monitor = annual_monitor, annual_grid = annual_grid,
       grid = grid, template = template, contract = contract, snapshot = snapshot,
       manifest = list(scope = scope, branches = read_names, contract_branch = contract_name,
         input_snapshot = snapshot, schema = daily_model_schema(),
         freshness = "UNKNOWN against current DAG; existing selected artifacts validated and fingerprinted; no recomputation"))
}
