
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
