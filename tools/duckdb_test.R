# Load required libraries
library(duckdb)
library(terra)
library(arrow)

# Example file paths (replace with your actual files)
raster_file <- "example.tif"
vector_parquet <- "vector_data.parquet"

# Read raster with terra
r <- rast(raster_file)

# Read vector data from parquet using arrow
vec <- read_parquet(vector_parquet)

# Convert vector data to SpatVector (assuming WKT geometry column named 'geometry')
if (!requireNamespace("sf", quietly = TRUE)) install.packages("sf")
library(sf)
vec_sf <- st_as_sf(vec, wkt = "geometry")
vec_sv <- vect(vec_sf)

# Extract raster values for each vector feature
vals <- extract(r, vec_sv, fun = mean, na.rm = TRUE)

# Combine with vector attributes
result <- cbind(as.data.frame(vec_sv), raster_mean = vals[,2])

# Optionally, write to DuckDB
con <- dbConnect(duckdb::duckdb(), dbdir = "summary.duckdb")
dbWriteTable(con, "vector_raster_summary", result, overwrite = TRUE)

# Query example: average raster value by a vector attribute (e.g., 'id')
dbGetQuery(con, "SELECT id, AVG(raster_mean) as avg_raster FROM vector_raster_summary GROUP BY id")

dbDisconnect(con, shutdown = TRUE)



# 
# Directly import Parquet file into DuckDB and run a simple query
con2 <- dbConnect(duckdb::duckdb(), dbdir = "summary.duckdb")
dbExecute(con2, "CREATE TABLE IF NOT EXISTS vector_data AS SELECT * FROM read_parquet('~/Documents/airquality/outdoor/sites_airkorea_2010_2023_spt_yd.parquet')")
dbGetQuery(con2, "SELECT COUNT(*) AS n_rows FROM vector_data")
dbDisconnect(con2, shutdown = TRUE)