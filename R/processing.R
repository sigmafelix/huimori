
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
#' @importFrom terra values focal
#' @export
rasterize_freq <-
  function(ras, mat = NULL) {
    vec_cl <- sort(unique(terra::values(ras)))
    out_list <- vector("list", length(vec_cl))
    for (i in seq_along(vec_cl)) {
      cl <- vec_cl[i]
      mask <- ras == cl
      # focal returns NA for NA window, so mask is logical (0/1)
      counted <- terra::focal(x = mask, w = mat, fun = sum, na.rm = TRUE)
      out_list[[i]] <- counted / sum(mat)
    }
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

