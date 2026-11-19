prepare_xgb_correct_training_data <- function(data, formula, target_year) {
  yvar <- as.character(formula)[2]
  target_year <- as.integer(target_year)
  if (length(target_year) != 1L || is.na(target_year)) {
    stop("target_year must be a single non-missing integer year.")
  }
  if (!"year" %in% names(data)) {
    stop("Training data must contain a year column.")
  }
  if (!yvar %in% names(data)) {
    stop("Training data is missing outcome column: ", yvar)
  }

  out <-
    data |>
    dplyr::filter(year <= target_year) |>
    dplyr::filter(!is.na(.data[[yvar]])) |>
    dplyr::mutate(
      .row_id = dplyr::row_number(),
      site_type = droplevels(site_type),
      latitude = as.double(stringi::stri_extract_first_regex(
        coords_google,
        pattern = "[3-4][0-9]\\.[0-9]{2,8}"
      )),
      longitude = as.double(stringi::stri_extract_last_regex(
        coords_google,
        pattern = "1[2-4][0-9]\\.[0-9]{2,8}"
      ))
    )

  if (nrow(out) < 1L) {
    stop("No training rows remain for ", yvar, " target_year=", target_year, ".")
  }
  year_max <- max(as.integer(out$year), na.rm = TRUE)
  if (is.finite(year_max) && year_max > target_year) {
    stop("Future-year leakage detected: max training year ", year_max, " > target_year ", target_year, ".")
  }
  if (anyNA(out$latitude) || anyNA(out$longitude)) {
    stop("Could not parse coords_google for all training rows in ", yvar, " target_year=", target_year, ".")
  }

  out <-
    out |>
    sf::st_as_sf(coords = c("longitude", "latitude"), crs = 4326) |>
    sf::st_transform(crs = "EPSG:5179")

  attr(out, "target_year") <- target_year
  attr(out, "outcome") <- yvar
  attr(out, "formula") <- formula
  out
}

xgb_spatial_cv_unit_table <- function(data, id_col = "TMSID2", crs = "EPSG:5179") {
  if (!inherits(data, "sf")) {
    stop("Spatial resampling data must be an sf object.")
  }
  if (!".row_id" %in% names(data)) {
    stop("Spatial resampling data must contain .row_id.")
  }

  data_proj <- sf::st_transform(data, crs = crs)
  coords <- sf::st_coordinates(data_proj)
  data_plain <- sf::st_drop_geometry(data_proj)
  if (!id_col %in% names(data_plain)) {
    id_col <- ".row_id"
  }
  unit_id <- as.character(data_plain[[id_col]])
  missing_unit <- is.na(unit_id) | unit_id == ""
  unit_id[missing_unit] <- paste0("row_", data_plain$.row_id[missing_unit])

  tibble::tibble(
    .row_id = data_plain$.row_id,
    unit_id = unit_id,
    center_x_row = as.double(coords[, 1]),
    center_y_row = as.double(coords[, 2])
  ) |>
    dplyr::group_by(unit_id) |>
    dplyr::summarise(
      n_rows = dplyr::n(),
      center_x = stats::median(center_x_row),
      center_y = stats::median(center_y_row),
      .groups = "drop"
    )
}

xgb_kmeans_fold_assignment <- function(data, v = 5L, id_col = "TMSID2", crs = "EPSG:5179", seed = 20260728L, nstart = 100L) {
  v <- as.integer(v)
  if (!identical(v, 5L)) {
    stop("K-means spatial CV currently requires v = 5.")
  }
  units <- xgb_spatial_cv_unit_table(data = data, id_col = id_col, crs = crs)
  if (nrow(units) < v) {
    stop("Need at least 5 unique stations/locations for 5-fold k-means spatial CV.")
  }

  set.seed(as.integer(seed))
  coords <- as.matrix(units[, c("center_x", "center_y")])
  km <- stats::kmeans(coords, centers = v, nstart = as.integer(nstart), iter.max = 100)

  raw_centers <- do.call(
    rbind,
    lapply(seq_len(v), function(cluster_id) {
      idx <- which(km$cluster == cluster_id)
      data.frame(
        cluster_id = cluster_id,
        center_x = mean(units$center_x[idx]),
        center_y = mean(units$center_y[idx]),
        station_count = length(idx),
        row_count = sum(units$n_rows[idx])
      )
    })
  )
  # Stable fold labels: sort k-means centers from north to south, then west to east.
  ordered <- raw_centers[order(-raw_centers$center_y, raw_centers$center_x), , drop = FALSE]
  label_map <- data.frame(cluster_id = ordered$cluster_id, fold_id = paste0("Fold", seq_len(v)))
  units$cluster_id <- km$cluster
  units$fold_id <- label_map$fold_id[match(units$cluster_id, label_map$cluster_id)]

  data_proj <- sf::st_transform(data, crs = crs)
  coords_row <- sf::st_coordinates(data_proj)
  data_plain <- sf::st_drop_geometry(data_proj)
  row_units <- as.character(data_plain[[if (id_col %in% names(data_plain)) id_col else ".row_id"]])
  missing_unit <- is.na(row_units) | row_units == ""
  row_units[missing_unit] <- paste0("row_", data_plain$.row_id[missing_unit])
  rows <-
    tibble::tibble(
      .row_id = data_plain$.row_id,
      unit_id = row_units,
      center_x = as.double(coords_row[, 1]),
      center_y = as.double(coords_row[, 2])
    ) |>
    dplyr::left_join(dplyr::select(units, unit_id, cluster_id, fold_id), by = "unit_id")
  if (anyNA(rows$fold_id)) {
    stop("Some rows were not assigned to a k-means spatial fold.")
  }

  centers <- dplyr::left_join(label_map, raw_centers, by = "cluster_id") |>
    dplyr::arrange(fold_id)
  list(units = units, rows = rows, centers = centers, kmeans = km, label_map = label_map)
}

extract_xgb_spatial_locations <- function(data, id_col = "TMSID2", crs = "EPSG:5179") {
  if (!id_col %in% names(data)) {
    stop("Spatial block data is missing location id column: ", id_col)
  }
  if (inherits(data, "sf")) {
    data_sf <- data
  } else {
    data_plain <- data
    if (!all(c("longitude", "latitude") %in% names(data_plain))) {
      if (!"coords_google" %in% names(data_plain)) {
        stop("Spatial block data must contain sf geometry, longitude/latitude, or coords_google.")
      }
      data_plain$latitude <- as.double(stringi::stri_extract_first_regex(
        data_plain$coords_google,
        pattern = "[3-4][0-9]\\.[0-9]{2,8}"
      ))
      data_plain$longitude <- as.double(stringi::stri_extract_last_regex(
        data_plain$coords_google,
        pattern = "1[2-4][0-9]\\.[0-9]{2,8}"
      ))
    }
    if (anyNA(data_plain$longitude) || anyNA(data_plain$latitude)) {
      stop("Could not derive longitude/latitude for all spatial block rows.")
    }
    data_sf <- sf::st_as_sf(data_plain, coords = c("longitude", "latitude"), crs = 4326)
  }
  data_proj <- sf::st_transform(data_sf, crs = crs)
  coords <- sf::st_coordinates(data_proj)
  data_plain <- sf::st_drop_geometry(data_proj)
  unit_id <- as.character(data_plain[[id_col]])
  if (anyNA(unit_id) || any(unit_id == "")) {
    stop("Spatial block location ids contain missing values in ", id_col, ".")
  }
  tibble::tibble(
    unit_id = unit_id,
    center_x_row = as.double(coords[, 1]),
    center_y_row = as.double(coords[, 2])
  ) |>
    dplyr::group_by(unit_id) |>
    dplyr::summarise(
      n_rows = dplyr::n(),
      center_x = stats::median(center_x_row),
      center_y = stats::median(center_y_row),
      .groups = "drop"
    )
}

make_xgb_spatial_block_spec <- function(
  data,
  v = 5L,
  id_col = "TMSID2",
  crs = "EPSG:5179",
  axis = "x",
  padding_m = 10000
) {
  v <- as.integer(v)
  if (!identical(v, 5L)) {
    stop("Explicit spatial block CV currently requires v = 5.")
  }
  axis <- match.arg(axis, c("x", "y"))
  units <- extract_xgb_spatial_locations(data = data, id_col = id_col, crs = crs)
  if (nrow(units) < v) {
    stop("Need at least 5 unique locations for explicit 5-block spatial CV.")
  }
  coord <- if (identical(axis, "x")) units$center_x else units$center_y
  breaks <- as.double(stats::quantile(coord, probs = seq(0, 1, length.out = v + 1L), type = 1, na.rm = TRUE))
  breaks[[1L]] <- min(coord, na.rm = TRUE) - 1
  breaks[[length(breaks)]] <- max(coord, na.rm = TRUE) + 1
  if (any(diff(breaks) <= 0)) {
    stop("Explicit spatial block breaks are not strictly increasing.")
  }
  units$fold_id <- as.character(cut(
    coord,
    breaks = breaks,
    include.lowest = TRUE,
    labels = paste0("Fold", seq_len(v))
  ))
  if (anyNA(units$fold_id)) {
    stop("Some locations were not assigned to an explicit spatial block.")
  }

  xmin <- min(units$center_x, na.rm = TRUE) - padding_m
  xmax <- max(units$center_x, na.rm = TRUE) + padding_m
  ymin <- min(units$center_y, na.rm = TRUE) - padding_m
  ymax <- max(units$center_y, na.rm = TRUE) + padding_m
  polygons <- lapply(seq_len(v), function(i) {
    if (identical(axis, "x")) {
      x0 <- breaks[[i]]
      x1 <- breaks[[i + 1L]]
      y0 <- ymin
      y1 <- ymax
    } else {
      x0 <- xmin
      x1 <- xmax
      y0 <- breaks[[i]]
      y1 <- breaks[[i + 1L]]
    }
    sf::st_polygon(list(matrix(
      c(x0, y0, x1, y0, x1, y1, x0, y1, x0, y0),
      ncol = 2,
      byrow = TRUE
    )))
  })
  blocks <- sf::st_sf(
    fold_id = paste0("Fold", seq_len(v)),
    geometry = sf::st_sfc(polygons, crs = sf::st_crs(crs))
  )
  centers <- units |>
    dplyr::group_by(fold_id) |>
    dplyr::summarise(
      center_x = mean(center_x),
      center_y = mean(center_y),
      station_count = dplyr::n(),
      row_count = sum(n_rows),
      .groups = "drop"
    )
  structure(
    list(
      units = units,
      blocks = blocks,
      centers = centers,
      breaks = breaks,
      axis = axis,
      id_col = id_col,
      crs = crs,
      label_rule = paste0(
        "Fold labels are explicit monitor-balanced ",
        if (identical(axis, "x")) "west-to-east" else "south-to-north",
        " spatial strips."
      )
    ),
    class = "xgb_spatial_block_spec"
  )
}

xgb_block_fold_assignment <- function(data, block_spec, id_col = "TMSID2", crs = "EPSG:5179") {
  if (!inherits(block_spec, "xgb_spatial_block_spec")) {
    stop("Expected block_spec from make_xgb_spatial_block_spec().")
  }
  units <- block_spec$units
  data_units <- xgb_spatial_cv_unit_table(data = data, id_col = id_col, crs = crs)
  missing_units <- setdiff(data_units$unit_id, units$unit_id)
  if (length(missing_units) > 0L) {
    stop("Spatial block spec is missing location id(s): ", paste(missing_units, collapse = ", "))
  }
  data_units <-
    data_units |>
    dplyr::select(unit_id, n_rows, center_x, center_y) |>
    dplyr::left_join(dplyr::select(units, unit_id, fold_id), by = "unit_id")
  if (anyNA(data_units$fold_id)) {
    stop("Some locations were not assigned to an explicit spatial block.")
  }

  data_proj <- sf::st_transform(data, crs = crs)
  coords_row <- sf::st_coordinates(data_proj)
  data_plain <- sf::st_drop_geometry(data_proj)
  row_units <- as.character(data_plain[[if (id_col %in% names(data_plain)) id_col else ".row_id"]])
  missing_unit <- is.na(row_units) | row_units == ""
  row_units[missing_unit] <- paste0("row_", data_plain$.row_id[missing_unit])
  rows <-
    tibble::tibble(
      .row_id = data_plain$.row_id,
      unit_id = row_units,
      center_x = as.double(coords_row[, 1]),
      center_y = as.double(coords_row[, 2])
    ) |>
    dplyr::left_join(dplyr::select(data_units, unit_id, fold_id), by = "unit_id")
  if (anyNA(rows$fold_id)) {
    stop("Some rows were not assigned to an explicit spatial block.")
  }
  list(
    units = data_units,
    rows = rows,
    centers = block_spec$centers,
    blocks = block_spec$blocks,
    block_spec = block_spec
  )
}

make_xgb_spatial_resamples <- function(
  data,
  v = 5L,
  method = "kmeans",
  id_col = "TMSID2",
  crs = "EPSG:5179",
  seed = 20260728L,
  nstart = 100L,
  block_spec = NULL,
  ...
) {
  if (!inherits(data, "sf")) {
    stop("Spatial resampling data must be an sf object.")
  }
  if (!method %in% c("kmeans", "spatial_blocks")) {
    stop("Unsupported XGB spatial CV method: ", method)
  }

  assignment <- if (identical(method, "spatial_blocks")) {
    xgb_block_fold_assignment(data = data, block_spec = block_spec, id_col = id_col, crs = crs)
  } else {
    xgb_kmeans_fold_assignment(
      data = data,
      v = v,
      id_col = id_col,
      crs = crs,
      seed = seed,
      nstart = nstart
    )
  }
  folds <- paste0("Fold", seq_len(as.integer(v)))
  row_index <- seq_len(nrow(data))
  splits <- lapply(folds, function(fold_id) {
    assessment_index <- row_index[assignment$rows$fold_id == fold_id]
    analysis_index <- setdiff(row_index, assessment_index)
    rsample::make_splits(
      x = list(analysis = analysis_index, assessment = assessment_index),
      data = data
    )
  })
  out <- rsample::manual_rset(splits = splits, ids = folds)
  attr(out, "fold_assignment") <- assignment$rows
  attr(out, "fold_units") <- assignment$units
  attr(out, "fold_centers") <- assignment$centers
  attr(out, "fold_blocks") <- assignment$blocks
  attr(out, "fold_label_map") <- assignment$label_map
  attr(out, "fold_method") <- method
  attr(out, "fold_crs") <- crs
  attr(out, "fold_seed") <- seed
  attr(out, "fold_nstart") <- nstart
  attr(out, "fold_label_rule") <- if (identical(method, "spatial_blocks")) {
    assignment$block_spec$label_rule
  } else {
    "Fold labels are k-means clusters ordered by center Y descending, then X ascending."
  }
  out
}

xgb_spatial_fold_membership <- function(data, resamples) {
  fold_membership <- do.call(
    rbind,
    Map(
      f = function(split, fold_id) {
        rsample::assessment(split) |>
          sf::st_drop_geometry() |>
          dplyr::select(.row_id) |>
          dplyr::mutate(fold_id = fold_id)
      },
      split = resamples$splits,
      fold_id = resamples$id
    )
  )
  if (nrow(fold_membership) != nrow(data)) {
    stop("Assessment fold membership does not cover all rows exactly once.")
  }
  if (any(duplicated(fold_membership$.row_id))) {
    stop("At least one row appears in more than one assessment fold.")
  }
  fold_assignment <- attr(resamples, "fold_assignment", exact = TRUE)
  if (!is.null(fold_assignment) && "unit_id" %in% names(fold_assignment)) {
    fold_membership <-
      fold_membership |>
      dplyr::left_join(
        dplyr::select(
          fold_assignment,
          dplyr::any_of(c(".row_id", "unit_id", "center_x", "center_y", "cluster_id", "sp_index"))
        ),
        by = ".row_id"
      )
  }
  fold_membership
}

validate_xgb_spatial_resamples <- function(data, resamples) {
  membership <- xgb_spatial_fold_membership(data = data, resamples = resamples)
  all_rows <- sf::st_drop_geometry(data)$.row_id
  assignment <- attr(resamples, "fold_assignment", exact = TRUE)
  overlap_count <- sum(vapply(resamples$splits, function(split) {
    length(intersect(rsample::analysis(split)$.row_id, rsample::assessment(split)$.row_id))
  }, integer(1)))
  station_overlap_count <- sum(vapply(as.character(resamples$id), function(fold_id) {
    assessment_units <- unique(assignment$unit_id[assignment$fold_id == fold_id])
    analysis_units <- unique(assignment$unit_id[assignment$fold_id != fold_id])
    length(intersect(analysis_units, assessment_units))
  }, integer(1)))
  data.frame(
    item = c(
      "Fold count",
      "Total rows",
      "Unassigned rows",
      "Duplicate assessment rows",
      "Assessment union complete",
      "Analysis-assessment row overlap",
      "Analysis-assessment station overlap"
    ),
    result = c(
      length(unique(membership$fold_id)),
      length(all_rows),
      sum(!all_rows %in% membership$.row_id),
      sum(duplicated(membership$.row_id)),
      setequal(membership$.row_id, all_rows),
      overlap_count,
      station_overlap_count
    ),
    pass = c(
      length(unique(membership$fold_id)) == 5L,
      length(all_rows) == nrow(data),
      all(all_rows %in% membership$.row_id),
      !any(duplicated(membership$.row_id)),
      setequal(membership$.row_id, all_rows),
      overlap_count == 0L,
      station_overlap_count == 0L
    )
  )
}

plot_xgb_spatial_folds <- function(
  data,
  resamples,
  output_dir = file.path("daehoon", "diagnostics", "annual_xgb_kmeans_lblo"),
  width = 8,
  height = 8,
  dpi = 180
) {
  yvar <- attr(data, "outcome")
  target_year <- attr(data, "target_year")
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  method <- attr(resamples, "fold_method", exact = TRUE)
  method_label <- if (identical(method, "spatial_blocks")) {
    "5block"
  } else {
    "kmeans5"
  }
  out_path <- file.path(output_dir, sprintf("%s_%d_%s_spatial_cv.png", yvar, as.integer(target_year), method_label))
  fold_membership <- xgb_spatial_fold_membership(data = data, resamples = resamples)
  plot_data <- data |>
    dplyr::left_join(fold_membership, by = ".row_id") |>
    sf::st_transform(crs = 4326)
  centers <- attr(resamples, "fold_centers", exact = TRUE)
  centers_sf <- if (!is.null(centers) && all(c("center_x", "center_y") %in% names(centers))) {
    centers |>
      sf::st_as_sf(coords = c("center_x", "center_y"), crs = attr(resamples, "fold_crs", exact = TRUE), remove = FALSE) |>
      sf::st_transform(crs = 4326)
  } else {
    NULL
  }
  blocks <- attr(resamples, "fold_blocks", exact = TRUE)
  blocks_plot <- if (inherits(blocks, "sf")) sf::st_transform(blocks, crs = 4326) else NULL
  fold_palette <- c(Fold1 = "#0072B2", Fold2 = "#009E73", Fold3 = "#E69F00", Fold4 = "#D55E00", Fold5 = "#CC79A7")
  plot_obj <- ggplot2::ggplot()
  if (!is.null(blocks_plot)) {
    plot_obj <- plot_obj +
      ggplot2::geom_sf(data = blocks_plot, ggplot2::aes(fill = fold_id), color = "black", alpha = 0.16, linewidth = 0.45) +
      ggplot2::geom_sf_text(data = sf::st_centroid(blocks_plot), ggplot2::aes(label = fold_id), size = 3.6)
  }
  plot_obj <-
    plot_obj +
    ggplot2::geom_sf(data = plot_data, ggplot2::aes(color = fold_id), size = 1.8, alpha = 0.9) +
    ggplot2::scale_color_manual(values = fold_palette, drop = FALSE) +
    ggplot2::scale_fill_manual(values = fold_palette, drop = FALSE) +
    ggplot2::coord_sf() +
    ggplot2::labs(
      title = sprintf(
        "%s %d %s spatial CV",
        yvar,
        as.integer(target_year),
        if (identical(method, "spatial_blocks")) {
          "explicit 5-block"
        } else {
          "k-means k=5"
        }
      ),
      color = "Fold",
      fill = "Fold"
    ) +
    ggplot2::theme_minimal()
  if (is.null(blocks_plot) && !is.null(centers_sf)) {
    plot_obj <- plot_obj +
      ggplot2::geom_sf(data = centers_sf, ggplot2::aes(fill = fold_id), shape = 24, color = "black", size = 4)
  }
  ggplot2::ggsave(out_path, plot_obj, width = width, height = height, dpi = dpi)
  c(map_png = out_path)
}

write_xgb_spatial_fold_diagnostics <- function(
  data,
  resamples,
  output_dir = file.path("daehoon", "diagnostics", "annual_xgb_kmeans_lblo")
) {
  yvar <- attr(data, "outcome")
  target_year <- as.integer(attr(data, "target_year"))
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  method <- attr(resamples, "fold_method", exact = TRUE)
  method_label <- if (identical(method, "spatial_blocks")) {
    "5block"
  } else {
    "kmeans5"
  }
  prefix <- file.path(output_dir, sprintf("%s_%d_%s", yvar, target_year, method_label))
  membership <- xgb_spatial_fold_membership(data = data, resamples = resamples)
  data_plain <- sf::st_drop_geometry(data)
  assign_table <-
    data_plain |>
    dplyr::left_join(membership, by = ".row_id") |>
    dplyr::select(dplyr::any_of(c(".row_id", "TMSID", "TMSID2", "year", yvar, "site_type", "unit_id", "fold_id", "cluster_id", "sp_index", "center_x", "center_y")))
  units <- attr(resamples, "fold_units", exact = TRUE)
  centers <- attr(resamples, "fold_centers", exact = TRUE)
  fold_summary <-
    units |>
    dplyr::group_by(fold_id) |>
    dplyr::summarise(
      station_count = dplyr::n(),
      assessment_rows = sum(n_rows),
      mean_center_distance_m = mean(sqrt((center_x - mean(center_x))^2 + (center_y - mean(center_y))^2)),
      max_center_distance_m = max(sqrt((center_x - mean(center_x))^2 + (center_y - mean(center_y))^2)),
      center_x = mean(center_x),
      center_y = mean(center_y),
      .groups = "drop"
    )
  if (!is.null(centers) && all(c("fold_id", "center_x", "center_y") %in% names(centers))) {
    fold_summary <-
      fold_summary |>
      dplyr::left_join(
        dplyr::select(centers, fold_id, block_center_x = center_x, block_center_y = center_y),
        by = "fold_id"
      )
  }
  validation <- validate_xgb_spatial_resamples(data = data, resamples = resamples)
  paths <- c(
    assignment_csv = paste0(prefix, "_station_assignment.csv"),
    row_assignment_csv = paste0(prefix, "_row_assignment.csv"),
    centers_csv = paste0(prefix, "_cluster_centers.csv"),
    fold_summary_csv = paste0(prefix, "_fold_summary.csv"),
    validation_csv = paste0(prefix, "_validation.csv")
  )
  utils::write.csv(units, paths[["assignment_csv"]], row.names = FALSE)
  utils::write.csv(assign_table, paths[["row_assignment_csv"]], row.names = FALSE)
  utils::write.csv(centers, paths[["centers_csv"]], row.names = FALSE)
  utils::write.csv(fold_summary, paths[["fold_summary_csv"]], row.names = FALSE)
  utils::write.csv(validation, paths[["validation_csv"]], row.names = FALSE)
  blocks <- attr(resamples, "fold_blocks", exact = TRUE)
  if (inherits(blocks, "sf")) {
    block_path <- paste0(prefix, "_blocks.gpkg")
    sf::st_write(blocks, block_path, delete_dsn = TRUE, quiet = TRUE)
    paths <- c(paths, blocks_gpkg = block_path)
  }
  plot_paths <- plot_xgb_spatial_folds(data = data, resamples = resamples, output_dir = output_dir)
  list(paths = c(paths, plot_paths), validation = validation, fold_summary = fold_summary, centers = centers)
}

flatten_workflow_list <- function(x) {
  if (inherits(x, "workflow")) {
    return(list(x))
  }
  if (!is.list(x)) {
    stop("Expected a workflow or list of workflows.")
  }
  unlist(lapply(x, flatten_workflow_list), recursive = FALSE)
}

predict_grid_with_matching_year_models <- function(grid_data, fitted_models, chr_terms_x) {
  df_combined <- sf::st_drop_geometry(grid_data)
  required_key_cols <- c("gid", "x", "y", "layer", "year")
  missing_key_cols <- setdiff(required_key_cols, names(df_combined))
  if (length(missing_key_cols) > 0L) {
    stop("df_feat_grid_merged is missing prediction key columns: ", paste(missing_key_cols, collapse = ", "))
  }
  missing_terms <- setdiff(chr_terms_x, names(df_combined))
  if (length(missing_terms) > 0L) {
    stop("df_feat_grid_merged is missing training predictors: ", paste(missing_terms, collapse = ", "))
  }

  grid_year <- unique(as.integer(df_combined$year))
  if (length(grid_year) != 1L || is.na(grid_year)) {
    stop("df_feat_grid_merged branch must contain exactly one prediction year.")
  }

  models <- flatten_workflow_list(fitted_models)
  model_year <- vapply(models, function(x) as.integer(attr(x, "target_year")), integer(1))
  selected <- models[model_year == grid_year]
  if (length(selected) < 1L) {
    stop("No fitted XGBoost model found for target_year=", grid_year, ".")
  }

  outcomes <- vapply(selected, function(x) as.character(attr(x, "outcome")), character(1))
  if (anyNA(outcomes) || any(outcomes == "")) {
    stop("All fitted XGBoost workflows must carry an outcome attribute.")
  }
  duplicated_outcomes <- unique(outcomes[duplicated(outcomes)])
  if (length(duplicated_outcomes) > 0L) {
    stop(
      "Multiple fitted XGBoost workflows found for target_year=",
      grid_year,
      " and outcome(s): ",
      paste(duplicated_outcomes, collapse = ", ")
    )
  }

  metadata_cols <-
    intersect(
      c("gid", "x", "y", "layer", "year", "X", "Y", "longitude", "latitude", "lon", "lat"),
      names(df_combined)
    )
  fitted <- df_combined[, metadata_cols, drop = FALSE]
  for (i in seq_along(selected)) {
    yvar <- outcomes[[i]]
    pred <- predict(selected[[i]], df_combined)
    if (nrow(pred) != nrow(df_combined)) {
      stop("Prediction row count changed for ", yvar, " target_year=", grid_year, ".")
    }
    fitted[[yvar]] <- pred$.pred
  }

  missing_pollutants <- setdiff(c("PM10", "PM25"), names(fitted))
  if (length(missing_pollutants) > 0L) {
    stop(
      "Missing pollutant predictions for target_year=",
      grid_year,
      ": ",
      paste(missing_pollutants, collapse = ", ")
    )
  }
  fitted
}
