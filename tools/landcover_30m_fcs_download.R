#
library(terra)

range <- c(124.5, 131.9, 32.8, 38.9)
years <- seq(2009, 2022)

for (y in years) {
  url <- sprintf(
    "https://s3.openlandmap.org/arco/lc_glc.fcs30d_c_30m_s_%d0101_%d1231_go_epsg.4326_v20231026.tif",
    y, y)
  message("Reading GLC-FCS30D data for year: ", y)
  luras <- terra::rast(url, win = range, vsi = TRUE)
  message("Writing GLC-FCS30D data for year: ", y)
  writeRaster(
    luras,
    file.path("/mnt/s/Korea/landuse", sprintf("lc_glc_fcs30d_30m_%d.tif", y)),
    overwrite = TRUE,
    datatype = "INT8U", # Unsigned 8-bit integer (since the highest value is 210)
    gdal = c("COMPRESS=DEFLATE")
  )
  message("Finished exporting.")
}

