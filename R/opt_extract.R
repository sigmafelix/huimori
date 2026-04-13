#' Choose a raster-aligned supertile size
#'
#' Choose a square chunk size in raster pixels and estimate the buffer halo
#' needed around each chunk. When `r` is lon/lat, `max_radius_m` is converted
#' to approximate degrees before the halo is computed.
#'
#' @param r A `terra` raster object or a raster filename.
#' @param max_radius_m Numeric scalar. Largest buffer radius in meters.
#' @param max_cells_in_memory Numeric scalar. Soft limit used when
#'   `enforce_memory_limit` is `TRUE`.
#' @param start_px Integer scalar. Requested starting supertile width in pixels.
#' @param min_px Integer scalar. Minimum allowed supertile width in pixels.
#' @param safety Numeric scalar. Fraction of `max_cells_in_memory` to use as a
#'   safety margin.
#' @param enforce_memory_limit Logical scalar. If `TRUE`, halve `start_px` until
#'   the estimated padded cells fit under `max_cells_in_memory * safety`, without
#'   going below `min_px`.
#'
#' @return A list with `supertile_px`, halo sizes, estimated padded cells, and
#'   whether the raster is lon/lat.
#' @export
opt_choose_supertile_px <- function(r,
                                    max_radius_m,
                                    max_cells_in_memory = 1e8,
                                    start_px = 2048L,
                                    min_px = 1024L,
                                    safety = 0.85,
                                    enforce_memory_limit = FALSE) {
  rr <- terra::rast(r)
  res_xy <- terra::res(rr)
  rx <- abs(res_xy[1])
  ry <- abs(res_xy[2])

  if (terra::is.lonlat(rr)) {
    ext <- as.vector(terra::ext(rr))
    max_abs_lat <- max(abs(ext[3:4]), na.rm = TRUE)
    meters_per_degree_y <- 111320
    meters_per_degree_x <- meters_per_degree_y * max(cos(max_abs_lat * pi / 180), 0.01)
    radius_x <- max_radius_m / meters_per_degree_x
    radius_y <- max_radius_m / meters_per_degree_y
  } else {
    radius_x <- max_radius_m
    radius_y <- max_radius_m
  }

  halo_x <- ceiling(radius_x / rx)
  halo_y <- ceiling(radius_y / ry)

  px <- as.integer(start_px)
  min_px <- as.integer(min_px)
  if (px < min_px) {
    px <- min_px
  }

  est_cells <- function(px) {
    (px + 2L * halo_x) * (px + 2L * halo_y)
  }

  while (enforce_memory_limit && est_cells(px) > max_cells_in_memory * safety && px > min_px) {
    px <- as.integer(px %/% 2L)
    if (px < min_px) {
      px <- min_px
    }
  }

  list(
    supertile_px = px,
    halo_px_x = halo_x,
    halo_px_y = halo_y,
    est_padded_cells = est_cells(px),
    raster_is_lonlat = terra::is.lonlat(rr)
  )
}

#' Assign points to raster-aligned chunks
#'
#' Add tile row, tile column, and chunk id fields to an `sf` point object based
#' on the raster grid and a supertile width in pixels.
#'
#' @param pts_sf An `sf` object containing point geometries.
#' @param r A `terra` raster object or a raster filename.
#' @param supertile_px Integer scalar. Chunk width in raster pixels.
#' @param report_dropped Logical scalar. If `TRUE`, report points outside the
#'   raster extent.
#'
#' @return The input points, filtered to the raster extent, with `tile_col`,
#'   `tile_row`, and `chunk_id` columns.
#' @export
opt_assign_chunks <- function(pts_sf, r, supertile_px, report_dropped = TRUE) {
  xy <- sf::st_coordinates(pts_sf)
  rr <- terra::rast(r)
  cols <- terra::colFromX(rr, xy[, 1])
  rows <- terra::rowFromY(rr, xy[, 2])

  keep <- !is.na(cols) & !is.na(rows)
  if (report_dropped && !all(keep)) {
    message(sum(!keep), " point(s) fall outside the raster extent and will be dropped.")
  }

  pts_sf <- pts_sf[keep, ]
  cols <- cols[keep]
  rows <- rows[keep]

  pts_sf$tile_col <- (cols - 1L) %/% supertile_px
  pts_sf$tile_row <- (rows - 1L) %/% supertile_px
  pts_sf$chunk_id <- sprintf("r%05d_c%05d", pts_sf$tile_row, pts_sf$tile_col)

  pts_sf
}

#' Summarize raster-aligned chunks
#'
#' Summarize the number and balance of chunks produced by
#' [opt_assign_chunks()].
#'
#' @param pts_chunked An `sf` object containing a `chunk_id` column.
#'
#' @return A one-row data frame with chunk count, maximum points per chunk,
#'   median points per chunk, and percentage of points in the four largest
#'   chunks.
#' @export
opt_summarize_chunks <- function(pts_chunked) {
  if (!"chunk_id" %in% names(pts_chunked)) {
    stop("pts_chunked must contain a chunk_id column.", call. = FALSE)
  }

  counts <- as.numeric(table(pts_chunked$chunk_id))
  counts <- sort(counts, decreasing = TRUE)

  data.frame(
    n_chunks = length(counts),
    max_points_per_chunk = max(counts),
    median_points_per_chunk = stats::median(counts),
    top4_points_pct = sum(utils::head(counts, 4L)) / sum(counts) * 100
  )
}

#' Write chunked sf objects to temporary RDS files
#'
#' Split chunked points into one RDS file per chunk so workers can read one
#' chunk each instead of receiving the full point or buffer object through
#' future globals.
#'
#' @param pts_chunked An `sf` object containing a `chunk_id` column.
#' @param chunk_dir Directory where chunk RDS files should be written.
#'
#' @return A named character vector of RDS paths, one per chunk id.
#' @export
opt_write_chunk_files <- function(pts_chunked, chunk_dir = tempfile("huimori_chunks_")) {
  if (!"chunk_id" %in% names(pts_chunked)) {
    stop("pts_chunked must contain a chunk_id column.", call. = FALSE)
  }

  if (!dir.exists(chunk_dir)) {
    dir.create(chunk_dir, recursive = TRUE, showWarnings = FALSE)
  }

  chunk_ids <- sort(unique(pts_chunked$chunk_id))
  paths <- file.path(chunk_dir, paste0(chunk_ids, ".rds"))
  names(paths) <- chunk_ids

  for (chunk_id in chunk_ids) {
    saveRDS(pts_chunked[pts_chunked$chunk_id == chunk_id, ], paths[[chunk_id]])
  }

  paths
}

#' Extract raster summaries for one point chunk
#'
#' Read one chunk file, build circular buffers, and run
#' [exactextractr::exact_extract()] for that chunk.
#'
#' @param chunk_file Path to an RDS file containing an `sf` point chunk.
#' @param r A `terra` raster object or a raster filename.
#' @param radius_m Numeric scalar. Buffer radius in meters.
#' @param id Character vector of identifier columns to keep.
#' @param fun Summary function passed to [exactextractr::exact_extract()].
#' @param gdal_cache_mb Integer scalar. GDAL cache size in MB for this process.
#' @param max_cells_in_memory Numeric scalar passed to
#'   [exactextractr::exact_extract()].
#' @param fill_na_frac Logical scalar. Replace missing fraction columns with
#'   zero when `fun = "frac"`.
#'
#' @return A data frame with id columns and extracted raster summaries.
#' @export
opt_extract_chunk <- function(chunk_file,
                              r,
                              radius_m,
                              id,
                              fun = "mean",
                              gdal_cache_mb = 2048L,
                              max_cells_in_memory = 1e8,
                              fill_na_frac = TRUE) {
  terra::gdalCache(gdal_cache_mb)
  rr <- terra::rast(r)
  pts_chunk <- readRDS(chunk_file)
  keep_cols <- unique(c(id, ".huimori_order", "chunk_id"))
  keep_cols <- keep_cols[keep_cols %in% names(pts_chunk)]
  pts_chunk <- pts_chunk[, keep_cols]

  buf <- sf::st_buffer(pts_chunk, dist = radius_m)
  extracted <- exactextractr::exact_extract(
    x = rr,
    y = buf,
    fun = fun,
    progress = FALSE,
    max_cells_in_memory = max_cells_in_memory
  )

  if (!is.data.frame(extracted)) {
    extracted <- data.frame(setNames(list(extracted), fun), check.names = FALSE)
  }

  out <- cbind(
    sf::st_drop_geometry(buf[, setdiff(names(buf), "chunk_id"), drop = FALSE]),
    extracted
  )

  if (identical(fun, "frac") && fill_na_frac) {
    value_cols <- setdiff(names(out), c(id, ".huimori_order"))
    out[value_cols] <- lapply(out[value_cols], function(x) {
      x[is.na(x)] <- 0
      x
    })
  }

  out
}

#' Extract land-use raster features with raster-aligned point chunks
#'
#' Extract circular-buffer raster summaries for many point locations while
#' avoiding large future exports. Points are transformed to the raster CRS,
#' assigned to raster-aligned chunks, written to one temporary RDS file per
#' chunk, and processed in parallel by reading those chunk files inside workers.
#'
#' This is intended for preprocessed land-use fraction rasters with
#' `fun = "mean"`, and can also be used with categorical rasters and
#' `fun = "frac"`.
#'
#' @param x A raster filename or `terra` raster object.
#' @param y An `sf` point object.
#' @param radius Numeric scalar. Buffer radius in meters.
#' @param id Character vector of identifier columns to keep in the result.
#' @param year Optional integer scalar. If supplied, a `year` column is added to
#'   `y` before extraction.
#' @param fun Summary function passed to [exactextractr::exact_extract()].
#' @param workers Integer scalar. Number of future workers. Use `1` for
#'   sequential extraction. Prefer `1` inside dynamic `targets` branches so the
#'   pipeline scheduler controls parallelism across branches.
#' @param gdal_cache_mb Integer scalar. GDAL cache size in MB per process.
#' @param max_cells_in_memory Numeric scalar passed to
#'   [exactextractr::exact_extract()].
#' @param start_supertile_px,min_supertile_px Integer chunk-size controls passed
#'   to [opt_choose_supertile_px()].
#' @param safety_fraction Numeric scalar passed to [opt_choose_supertile_px()].
#' @param enforce_chunk_memory_limit Logical scalar passed to
#'   [opt_choose_supertile_px()].
#' @param prefix Character scalar used when renaming extracted value columns.
#' @param rename Logical scalar. If `TRUE`, rename extracted value columns as
#'   `{prefix}_{column}_{radius}`.
#' @param chunk_dir Optional directory for temporary chunk RDS files.
#' @param cleanup Logical scalar. Remove temporary chunk files on exit.
#' @param verbose Logical scalar. Print chunk information.
#'
#' @return A data frame containing `id` columns and extracted raster features.
#' @export
opt_extract_landuse_grid <- function(x,
                                     y,
                                     radius,
                                     id = c("gid", "year"),
                                     year = NULL,
                                     fun = "mean",
                                     workers = 1L,
                                     gdal_cache_mb = 2048L,
                                     max_cells_in_memory = 1e8,
                                     start_supertile_px = 2048L,
                                     min_supertile_px = 1024L,
                                     safety_fraction = 0.85,
                                     enforce_chunk_memory_limit = FALSE,
                                     prefix = "landuse",
                                     rename = TRUE,
                                     chunk_dir = tempfile("huimori_landuse_chunks_"),
                                     cleanup = TRUE,
                                     verbose = TRUE) {
  if (!inherits(y, "sf")) {
    stop("y must be an sf object.", call. = FALSE)
  }
  if (!is.null(year)) {
    y[["year"]] <- year
  }
  y[[".huimori_order"]] <- seq_len(nrow(y))
  missing_id <- setdiff(id, names(y))
  if (length(missing_id) > 0) {
    stop("Missing id column(s): ", paste(missing_id, collapse = ", "), call. = FALSE)
  }

  rr <- terra::rast(x)
  y <- sf::st_transform(y, terra::crs(rr))

  chunk_info <- opt_choose_supertile_px(
    r = rr,
    max_radius_m = radius,
    max_cells_in_memory = max_cells_in_memory,
    start_px = start_supertile_px,
    min_px = min_supertile_px,
    safety = safety_fraction,
    enforce_memory_limit = enforce_chunk_memory_limit
  )
  pts_chunked <- opt_assign_chunks(
    pts_sf = y,
    r = rr,
    supertile_px = chunk_info$supertile_px
  )
  chunk_summary <- opt_summarize_chunks(pts_chunked)

  if (verbose) {
    message("Chosen supertile size: ", chunk_info$supertile_px, " pixels")
    message("Number of chunks: ", chunk_summary$n_chunks)
    message("Max points per chunk: ", chunk_summary$max_points_per_chunk)
    message("Median points per chunk: ", chunk_summary$median_points_per_chunk)
    message("Top 4 chunks contain: ", round(chunk_summary$top4_points_pct, 1), "% of points")
  }
  if (chunk_summary$n_chunks < workers) {
    warning(
      "Number of chunks (", chunk_summary$n_chunks,
      ") is below workers (", workers,
      "); worker utilization will be limited.",
      call. = FALSE
    )
  }

  chunk_files <- opt_write_chunk_files(pts_chunked, chunk_dir = chunk_dir)
  if (cleanup) {
    on.exit(unlink(chunk_dir, recursive = TRUE, force = TRUE), add = TRUE)
  }

  if (workers > 1L) {
    old_plan <- future::plan()
    on.exit(future::plan(old_plan), add = TRUE)
    future::plan(future.mirai::mirai_multisession, workers = workers)
    res_list <- future.apply::future_lapply(
      chunk_files,
      FUN = opt_extract_chunk,
      r = x,
      radius_m = radius,
      id = id,
      fun = fun,
      gdal_cache_mb = gdal_cache_mb,
      max_cells_in_memory = max_cells_in_memory,
      future.seed = TRUE
    )
  } else {
    res_list <- lapply(
      chunk_files,
      FUN = opt_extract_chunk,
      r = x,
      radius_m = radius,
      id = id,
      fun = fun,
      gdal_cache_mb = gdal_cache_mb,
      max_cells_in_memory = max_cells_in_memory
    )
  }

  out <- dplyr::bind_rows(res_list)
  if (".huimori_order" %in% names(out)) {
    out <- out[order(out[[".huimori_order"]]), , drop = FALSE]
    out[[".huimori_order"]] <- NULL
    row.names(out) <- NULL
  }
  value_cols <- setdiff(names(out), id)

  if (rename && length(value_cols) > 0) {
    names(out)[match(value_cols, names(out))] <- paste0(prefix, "_", value_cols, "_", radius)
  }

  out
}
