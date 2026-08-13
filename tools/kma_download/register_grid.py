"""
Register the bare attribute matrix (kma_download_test.nc) onto the per-cell
WGS84 coordinates (sfc_grid_latlon.nc).

Both files share the same 2049x2049 Lambert Conformal grid, so registration is
an element-wise pairing: cell [j,i] of `data` sits at lon[j,i], lat[j,i].

Output: sfc_grid_georef.nc  -- a CF-compliant NetCDF where the temperature
field carries 2D lon/lat coordinates (curvilinear grid).
"""
import netCDF4 as nc
import numpy as np

DATA_FILE   = "kma_download_test.nc"
COORD_FILE  = "sfc_grid_latlon.nc"
OUT_FILE    = "sfc_grid_georef.nc"

# --- read coordinates ---
with nc.Dataset(COORD_FILE) as g:
    lon = np.asarray(g.variables["lon"][:], dtype="float32")
    lat = np.asarray(g.variables["lat"][:], dtype="float32")

# --- read + decode the attribute matrix ---
with nc.Dataset(DATA_FILE) as d:
    v = d.variables["data"]
    raw   = np.asarray(v[:])
    scale = float(v.data_scale)          # value = data / data_scale
    unit  = v.unit
    title = d.title

if lon.shape != raw.shape:
    raise ValueError(f"grid mismatch: coords {lon.shape} vs data {raw.shape}")

FILL_RAW = -9990                          # -999.0 after scaling = off-grid / missing
value = np.where(raw == FILL_RAW, np.nan, raw / scale).astype("float32")

# --- write georeferenced CF NetCDF ---
with nc.Dataset(OUT_FILE, "w", format="NETCDF4") as out:
    out.createDimension("y", lon.shape[0])
    out.createDimension("x", lon.shape[1])

    vlon = out.createVariable("lon", "f4", ("y", "x"))
    vlon.standard_name = "longitude"
    vlon.units = "degrees_east"
    vlon[:] = lon

    vlat = out.createVariable("lat", "f4", ("y", "x"))
    vlat.standard_name = "latitude"
    vlat.units = "degrees_north"
    vlat[:] = lat

    vtmp = out.createVariable("ta", "f4", ("y", "x"),
                              fill_value=np.float32(np.nan))
    vtmp.long_name = "surface air temperature"
    vtmp.units = unit
    vtmp.coordinates = "lon lat"          # CF: attaches the 2D geolocation
    vtmp[:] = value

    out.Conventions = "CF-1.8"
    out.title = title
    out.grid_mapping_note = "curvilinear grid; each cell georeferenced via lon/lat"

n_valid = int(np.isfinite(value).sum())
print(f"wrote {OUT_FILE}: {value.shape} grid, {n_valid} valid cells, "
      f"range {np.nanmin(value):.1f}..{np.nanmax(value):.1f} {unit}")
