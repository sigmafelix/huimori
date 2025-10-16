
#' Rasterize Frequency Calculation
#'
#' Computes the frequency of each unique value (class) in a raster within
#'   a moving window defined by a matrix.
#'
#' @param ras A `SpatRaster` object from the `terra` package.
#'   The raster to process.
#' @param mat A matrix defining the moving window
#'   (e.g., a kernel for focal operations). If `NULL`, defaults may apply.
#'
#' @return A list of `SpatRaster` objects, each representing the count of
#'   non-zero occurrences of a class within the moving window for
#'   each unique value in `ras`.
#'
#' @details
#' For each unique value in the raster, a mask is created.
#' The function then applies a focal operation to count the number of
#' non-zero cells within the window for each class.
#'
#' @examples
#' library(terra)
#' r <- rast(matrix(sample(1:3, 100, replace = TRUE), 10, 10))
#' mat <- matrix(1, 3, 3)
#' freq_list <- rasterize_freq(r, mat)
#'
#' @importFrom terra values focalCpp
#' @import Rcpp
#' @export
rasterize_freq <-
  function(ras, mat = NULL) {
    vec_cl <- sort(unique(terra::values(ras, na.rm = TRUE)))
    n_classes <- length(vec_cl)

    Rcpp::cppFunction('
      NumericMatrix count_classes_cpp(NumericVector x, NumericVector classes, 
                                      size_t ni, size_t nw) {
        int nc = classes.size();
        NumericMatrix out(ni, nc);
        size_t count = 0;
        
        for(size_t i = 0; i < ni; i++) {
          size_t end = count + nw;
          for (size_t j = count; j < end; j++) {
            if (!NumericVector::is_na(x[j])) {
              for(int k = 0; k < nc; k++) {
                if(x[j] == classes[k]) {
                  out(i, k) += 1;
                  break;
                }
              }
            }
          }
          count = end;
        }
        return out;
      }
    ')

    # Process once instead of n_classes times
    result_matrix <- terra::focalCpp(
      x = ras, 
      w = mat, 
      fun = function(x, ni, nw) count_classes_cpp(x, vec_cl, ni, nw),
      fillvalue = NA
    )

    # Convert matrix columns to separate rasters
    out_list <- lapply(1:n_classes, function(i) {
      r <- terra::rast(ras)
      terra::values(r) <- result_matrix[, i] / sum(mat)
      return(r)
    })

    out_stack <- do.call(c, out_list)
    names(out_stack) <- sprintf("class_%02d", vec_cl)
    return(out_stack)
  }
  # function(ras, mat = NULL) {
  #   vec_cl <- sort(unique(terra::values(ras)))
  #   out_list <- vector("list", length(vec_cl))

  #   Rcpp::cppFunction(
  #     '
  #       NumericVector count_nonzero(NumericVector x, size_t ni, size_t nw) {
  #         NumericVector out(ni);
  #         size_t count = 0;
  #         for(size_t i = 0; i < ni; i++) {
  #           size_t end = count + nw;
  #           double v = 0;
  #           for (size_t j = count; j < end; j++) {
  #             v += x[j];
  #           }
  #           out[i] = v;
  #           count = end;
  #         }
  #         return out;
  #       }
  #     '
  #   )

  #   for (i in seq_along(vec_cl)) {
  #     cl <- vec_cl[i]
  #     mask <- ras == cl
  #     # focal returns NA for NA window, so mask is logical (0/1)
  #     counted <-
  #       terra::focalCpp(x = mask, w = mat, fun = count_nonzero, fillvalue = 0)
  #     out_list[[i]] <- counted / sum(mat)
  #   }
  #   out_stack <- do.call(c, out_list)
  #   names(out_stack) <- sprintf("class_%02d", vec_cl)
  #   return(out_stack)
  # }


#' Whitebox implementation of frequency rasterization
#' @param ras A `SpatRaster` object from the `terra` package.
#' @param window_size Size of the moving window (must be odd).
#' @return A `SpatRaster` object with frequency rasters for each class.
#' @details This function uses the WhiteboxTools library to perform
#' a mean filter on binary masks for each class in the input raster.
#' @importFrom terra rast writeRaster values
#' @importFrom whitebox wbt_mean_filter
#' @export
rasterize_freq_whitebox <- function(ras, window_size = 67) {
  vec_cl <- sort(unique(terra::values(ras, na.rm = TRUE)))

  # Save temporary input
  temp_in <- tempfile(fileext = ".tif")
  terra::writeRaster(ras, temp_in, overwrite = TRUE)

  out_list <- lapply(vec_cl, function(cl) {
    # Create binary mask
    temp_mask <- tempfile(fileext = ".tif")
    temp_out <- tempfile(fileext = ".tif")

    mask <- ras == cl
    terra::writeRaster(mask, temp_mask, overwrite = TRUE)

    # Use majority filter (very fast)
    whitebox::wbt_mean_filter(
      input = temp_mask,
      output = temp_out,
      filterx = window_size,
      filtery = window_size
    )

    return(terra::rast(temp_out))
  })

  out_stack <- do.call(c, out_list)
  names(out_stack) <- sprintf("class_%02d", vec_cl)
  return(out_stack)
}


#' Make a circular binary mask
#' @param nrow Number of rows in the mask
#' @param ncol Number of columns in the mask
#' @return A binary matrix with a circular pattern
#' @details The function creates a binary matrix of size `nrow` x `ncol`
#' with a circular pattern of 1s in the center and 0s elsewhere.
#' The circle is defined such that it fits within the matrix dimensions.
#' @export
make_binary_mask <-
  function(nrow, ncol) {
    if (!all(c(nrow, ncol) %% 2 == 1)) {
      stop("Both nrow and ncol must be odd numbers.")
    }
    mat <- matrix(0, nrow = nrow, ncol = ncol)
    center_row <- ceiling(nrow / 2)
    center_col <- ceiling(ncol / 2)
    radius <- min(center_row, center_col) - 1
    for (i in 1:nrow) {
      for (j in 1:ncol) {
        # Use Euclidean distance for a circular mask
        if (sqrt((i - center_row)^2 + (j - center_col)^2) <= radius) {
          mat[i, j] <- 1
        }
      }
    }
    xcent <- ceiling(ncol / 2)
    ycent <- ceiling(nrow / 2)
    mat[xcent - 1, 1] <- mat[1, ycent - 1] <-
      mat[xcent - 1, ncol] <- mat[nrow, ycent - 1] <-
      mat[xcent + 1, 1] <- mat[1, ycent + 1] <-
      mat[xcent + 1, ncol] <- mat[nrow, ycent + 1] <- 1
    mat
  }



#' Geographical smoothing of a raster
#'
#' Smoothing filter for a raster object. It may be useful
#' for calculating relative altitude or other derivations
#'
#' @param x A `SpatRaster` object from the `terra` package.
#'  The raster to smooth.
#' @param mat A matrix defining the smoothing kernel.
#' If `NULL`, a default kernel is used.
#' @return A `SpatRaster` object with smoothed values.
#' @details
#' The function applies a focal operation to smooth the raster values
#' using the specified kernel. If no kernel is provided, a default
#' kernel is used.
#'
#' @examples
#' library(terra)
#' r <- rast(matrix(rnorm(100), 10, 10))
#' mat <- matrix(1, 3, 3)
#' smoothed_r <- detect_lower(r, mat)
#'
#' @importFrom terra focal
#' @export
lower_filter <-
  function(x, mat = NULL, rad) {
    matsize <- 2 * rad + 1
    if (is.null(mat)) {
      mat <- matrix(0, matsize, matsize)
      for (i in c(1, matsize)) {
        mat[i, ] <- 1
        mat[, i] <- 1
      }
      mat[rad + 1, rad + 1] <- 1
    }
    denom <- 4 * (2 * rad + 1) - 4
    islower <-
      function(w) {
        vec_idx <- rad * (2 * rad + 1) + rad
        res <- sum(w[which(w != 0)] > w[vec_idx], na.rm = TRUE) / denom
        return(res)
      }
    smoothed <- terra::focal(x = x, w = mat, fun = islower, expand = TRUE)
    return(smoothed)
  }


#' Geographically weighted emittors with a spatial extent
#' 
#' This function takes three mandatory inputs: input points,
#' emittor points, and a spatial polygon to delimit the emittors
#' inside of it. From each input point, all the emittor points
#' are firstly subsetted by the spatial polygon, and the clipped
#' emittors are spatially weighted depending on the Euclidean or
#' geodesic distance from the focal point with optional arguments
#' for the weighting function (default: Gaussian; with options for
#' exponential, tricube, epanechnikov) and optional weights to add
#' more weights for the emission factor with respect to each emittor's
#' classification.
#'
#' @param input sf
#' @param target sf
#' @param clip sf st_POLYGON
#' @param wfun character
#' @param bw numeric
#' @param weight numeric
#' @param dist_method character
#' @return A data.frame with weighted emissions for each input point
#' @importFrom sf st_intersection st_transform st_is_longlat st_distance
#' @importFrom dplyr mutate select
#' @importFrom stats dnorm
#' @importFrom purrr list_rbind map
#' @examples
#' library(sf)
#' library(dplyr)
#'
#' # Example input points (monitoring stations)
#' input <- st_as_sf(data.frame(
#'   id = 1:3,
#'   lon = c(126.9780, 127.0010, 126.9900),
#'   lat = c(37.5665, 37.5700, 37.5800)
#' ), coords = c("lon", "lat"), crs = 4326) %>%
#'   st_transform(5179)
#'
#' # Example target points (emittor locations with emissions)
#' target <- st_as_sf(data.frame(
#'   id = 1:5,
#'   lon = c(126.9800, 127.0020, 126.9950, 127.0100, 126.9850),
#'   lat = c(37.5650, 37.5680, 37.5750, 37.5720, 37.5780),
#'   emission = c(100, 150, 200, 250, 300)
#' ), coords = c("lon", "lat"), crs = 4326) %>%
#'   st_transform(5179) %>%
#'   mutate(emission = emission)
#'
#' # Example clipping polygon (a simple square around the input points)
#' clip <- st_as_sf(data.frame(
#'   id = 1,
#'   geometry = st_sfc(st_polygon(list(rbind(
#'     c(126.9750, 37.5600),
#'     c(127.0150, 37.5600),
#'     c(127.0150, 37.5850),
#'     c(126.9750, 37.5850),
#'     c(126.9750, 37.5600)
#'   ))))
#' ), crs = 4326) %>%
#'   st_transform(5179)
#'
#' # Calculate geographically weighted emissions
#' result <- purrr::map(seq_len(nrow(input)), \(x) gw_emittors(
#'   input = input[x, ],
#'   target = target,
#'   clip = clip,
#'   wfun = "gaussian",
#'   bw = 1000,
#'   dist_method = "geodesic"
#' )) |>
#' purrr::list_rbind()
#' print(result)
#' @export
gw_emittors <-
  function(input, target, clip,
           wfun = c("gaussian", "exponential", "tricube", "epanechnikov"),
           bw = 1000, weight = NULL,
           dist_method = c("euclidean", "geodesic")) {
    wfun <- match.arg(wfun)
    dist_method <- match.arg(dist_method)
    if (is.null(weight)) {
      weight <- rep(1, nrow(target))
    }
    if (any(is.na(weight))) {
      stop("weight contains NA values.")
    }
    if (any(weight < 0)) {
      stop("weight contains negative values.")
    }
    if (!all(sf::st_is_longlat(input) == sf::st_is_longlat(target))) {
      stop("input and target must have the same coordinate reference system.")
    }
    if (!all(sf::st_is_longlat(input) == sf::st_is_longlat(clip))) {
      stop("input and clip must have the same coordinate reference system.")
    }
    if (!all(sf::st_is_longlat(target) == sf::st_is_longlat(clip))) {
      stop("target and clip must have the same coordinate reference system.")
    }
    if (sf::st_is_longlat(input)) {
      input <- sf::st_transform(input, 5179)
    }
    if (sf::st_is_longlat(target)) {
      target <- sf::st_transform(target, 5179)
    }
    if (sf::st_is_longlat(clip)) {
      clip <- sf::st_transform(clip, 5179)
    }
    sf::st_agr(input) <- "constant"
    sf::st_agr(target) <- "constant"
    sf::st_agr(clip) <- "constant"

    # Prepare output
    gw_emission <- rep(NA_real_, nrow(input))

    # Intersect input with clip
    input_in_clip <- sf::st_intersection(input, clip)
    idx_in_clip <- as.integer(rownames(input_in_clip))

    if (length(idx_in_clip) > 0) {
      # Subset target by clip
      target_clip <- target[clip, ]
      if (nrow(target_clip) > 0) {
        # Compute distances between all input points and all target points
        dists <- sf::st_distance(input[idx_in_clip, ], target_clip)
        dists <- as.matrix(dists)
        # For each input point
        for (i in seq_along(idx_in_clip)) {
          dist_vec <- as.numeric(dists[i, ])
          if (!all(dist_vec > bw)) {
            if (wfun == "gaussian") {
              w <- dnorm(dist_vec / bw)
            } else if (wfun == "exponential") {
              w <- exp(-dist_vec / bw)
            } else if (wfun == "tricube") {
              w <- (1 - (dist_vec / bw)^3)^3
              w[dist_vec > bw] <- 0
            } else if (wfun == "epanechnikov") {
              w <- 0.75 * (1 - (dist_vec / bw)^2)
              w[dist_vec > bw] <- 0
            }
            w <- w * weight[as.integer(rownames(target_clip))]
            if (sum(w) != 0) {
              gw_emission[idx_in_clip[i]] <- sum(target_clip$emission * w) / sum(w)
            }
          }
        }
      }
    }
    input$gw_emission <- gw_emission
    return(sf::st_drop_geometry(input))
  }