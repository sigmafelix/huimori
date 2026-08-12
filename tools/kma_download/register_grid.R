# Register the bare attribute matrix (kma_download_test.nc) onto the per-cell
# WGS84 coordinates (sfc_grid_latlon.nc).
#
# Both files share the same 2049x2049 Lambert Conformal grid, so registration
# is an element-wise pairing: cell [i,j] of `data` sits at lon[i,j], lat[i,j].
# ncdf4 reads dims in column-major order, but both files use the same layout,
# so the pairing stays consistent (no transpose needed for registration).
#
# Output: sfc_grid_georef_R.nc -- CF-compliant NetCDF where the temperature
# field carries 2D lon/lat coordinates (curvilinear grid).

library(ncdf4)

DATA_FILE  <- "kma_download_test.nc"
COORD_FILE <- "sfc_grid_latlon.nc"
OUT_FILE   <- "sfc_grid_georef_R.nc"
FILL_RAW   <- -9990   # -999.0 after scaling = off-grid / missing

# --- read coordinates ---
gc_nc <- nc_open(COORD_FILE)
lon <- ncvar_get(gc_nc, "lon")
lat <- ncvar_get(gc_nc, "lat")
nc_close(gc_nc)

# --- read + decode the attribute matrix ---
dc_nc <- nc_open(DATA_FILE)
raw   <- ncvar_get(dc_nc, "data")
scale <- ncatt_get(dc_nc, "data", "data_scale")$value  # value = data / data_scale
unit  <- ncatt_get(dc_nc, "data", "unit")$value
title <- ncatt_get(dc_nc, 0, "title")$value
nc_close(dc_nc)

if (!all(dim(lon) == dim(raw)))
  stop(sprintf("grid mismatch: coords %s vs data %s",
               paste(dim(lon), collapse="x"), paste(dim(raw), collapse="x")))

value <- ifelse(raw == FILL_RAW, NA, raw / scale)

# --- write georeferenced CF NetCDF ---
# Define index dimensions; lon/lat are 2D coordinate variables over them.
dimx <- ncdim_def("x", "", seq_len(dim(lon)[1]), create_dimvar = FALSE)
dimy <- ncdim_def("y", "", seq_len(dim(lon)[2]), create_dimvar = FALSE)

vlon <- ncvar_def("lon", "degrees_east",  list(dimx, dimy), prec = "float", compression = 9)
vlat <- ncvar_def("lat", "degrees_north", list(dimx, dimy), prec = "float", compression = 9)
vtmp <- ncvar_def("ta",  unit, list(dimx, dimy), missval = NA, prec = "float", compression = 9)

out <- nc_create(OUT_FILE, list(vlon, vlat, vtmp))
ncvar_put(out, vlon, lon)
ncvar_put(out, vlat, lat)
ncvar_put(out, vtmp, value)

ncatt_put(out, "lon", "standard_name", "longitude")
ncatt_put(out, "lat", "standard_name", "latitude")
ncatt_put(out, "ta",  "long_name", "surface air temperature")
ncatt_put(out, "ta",  "coordinates", "lon lat")   # CF: attaches the 2D geolocation
ncatt_put(out, 0, "Conventions", "CF-1.8")
ncatt_put(out, 0, "title", title)
ncatt_put(out, 0, "grid_mapping_note",
          "curvilinear grid; each cell georeferenced via lon/lat")
nc_close(out)

n_valid <- sum(!is.na(value))
cat(sprintf("wrote %s: %s grid, %d valid cells, range %.1f..%.1f %s\n",
            OUT_FILE, paste(dim(value), collapse="x"), n_valid,
            min(value, na.rm=TRUE), max(value, na.rm=TRUE), unit))
