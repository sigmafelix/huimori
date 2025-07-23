library(dbplyr)
library(RPostgres)
library(rpostgis)
library(terra)
library(DBI)
library(sf)
sf::sf_use_s2(FALSE)

chr_dir_data <-
  if (Sys.getenv("USER") == "isong") {
      file.path("/mnt/s", "Korea")
  } else {
      file.path(Sys.getenv("HOME"), "Documents")
  }

chr_dir_git <-
  file.path(Sys.getenv("HOME"), "GitHub", "histmap-ko")


## Export a raster to PostGIS
## Should set PGPASSWORD environment variable before proceeding!
mycon <- dbConnect(
  RPostgres::Postgres(),
  dbname = "geodb",
  host = "localhost",
  port = 5432,
  user = "isong",
  password = Sys.getenv("PGPASSWORD")
)

rpostgis::pgPostGIS(
  conn = mycon,
  topology = TRUE,
  sfcgal = TRUE,
  raster = TRUE
)

chr_dem_file <-
  file.path(
    chr_dir_data,
    "elevation",
    "kngii_2022_merged_res30d.tif"
  )

rpostgis::pgWriteRast(
  conn = mycon,
  name = c("public", "kngii_2022_30m"),
  raster = terra::rast(chr_dem_file)
)


chr_dsm_file <-
  file.path(
    chr_dir_data,
    "elevation",
    "copernicus_korea_30m.tif"
  )


rpostgis::pgWriteRast(
  conn = mycon,
  name = c("public", "copernicus_korea_30m"),
  raster = terra::rast(chr_dsm_file)
)

chr_file_emission_locs <-
  file.path(
    chr_dir_data,
    "emission",
    "data",
    "emission_location.gpkg"
  )

rpostgis::pgWriteGeom(
  conn = mycon,
  name = c("public", "emission_location"),
  data.obj = sf::st_read(chr_file_emission_locs),
  overwrite = TRUE
)

chr_asos_file <-
  file.path(chr_dir_data, "weather", "data", "asos_2010_2023.parquet")

RPostgres::dbWriteTable(
  conn = mycon,
  name = c("public", "asos_2010_2023"),
  value = nanoparquet::read_parquet(chr_asos_file),
  overwrite = TRUE
)

export_raster_to_postgis <-
  function(raster_file, table_name, conn) {
    raster <- terra::rast(raster_file)
    raster <- terra::project(raster, "EPSG:5179")
    terra::writeRaster(
      raster,
      filename = table_name,
      format = "PostGIS",
      overwrite = TRUE,
      datatype = "INT2",
      options = c(
        "RASTER_TABLE=TRUE",
        "RASTER_COLUMN=raster",
        "RASTER_ID=rid",
        "RASTER_SRID=5179"
      )
    )
    dbExecute(
      conn,
      paste0(
        "ALTER TABLE ", table_name,
        " ADD COLUMN id SERIAL PRIMARY KEY;"
      )
    )
    dbExecute(
      conn,
      paste0(
        "ALTER TABLE ", table_name,
        " ALTER COLUMN id SET NOT NULL;"
      )
    )
    dbExecute(
      conn,
      paste0(
        "ALTER TABLE ", table_name,
        " ADD COLUMN created_at TIMESTAMP DEFAULT NOW();"
      )
    )
    dbExecute(
      conn,
      paste0(
        "ALTER TABLE ", table_name,
        " ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();"
      )
    )
    dbExecute(
      conn,
      paste0(
        "CREATE INDEX idx_", table_name,
        "_geom ON ", table_name,
        " USING GIST (geom);"
      )
    )
    dbExecute(
      conn,
      paste0(
        "CREATE INDEX idx_", table_name,
        "_raster ON ", table_name,
        " USING GIST (raster);"
      )
    )
  }