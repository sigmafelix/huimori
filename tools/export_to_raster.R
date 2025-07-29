# Export predictions to raster files
for (i in 1:18) {
  rng_target <- 1:593 + (i * 593) - 593
  message("Processing target: ", min(rng_target), " to ", max(rng_target))
  ras_pred_annual <- targets::tar_read(workflow_fit_correct, rng_target)
  tyear <- unique(ras_pred_annual[["year"]])
  ras_pred_annual <- ras_pred_annual[, c(3, 4, 1)]
  cname <- names(ras_pred_annual)[3]
  fname <- file.path(
    "/mnt/s/Korea",
    "predictions",
    paste0(tolower(cname), "_pred_", tyear, "_annual.tif")
  )
  message("Defining Raster: ", fname)
  res_ras <- terra::rast(ras_pred_annual, crs = "EPSG:5179", type = "xyz")
  message("Writing Raster: ", fname)
  terra::writeRaster(
    x = res_ras,
    filename = fname,
    overwrite = TRUE,
    datatype = "FLT4S",
    gdal = c("COMPRESS=DEFLATE")
  )
}
