
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
    mask <- Map(function(x) ras * (ras == x), vec_cl)
    counted <- Map(function(x) {
      terra::focal(x = x, w = mat, fun = \(k) sum(k != 0))
    }, mask)

    rcounted <- Reduce(c, counted)
    rcountedw <- rcounted / sum(mat)
    names(rcountedw) <- sprintf("class_%02d", vec_cl)
    return(rcountedw)
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

