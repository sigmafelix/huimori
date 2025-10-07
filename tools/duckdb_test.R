# Load required libraries
library(duckdb)
library(terra)
library(nanoparquet)

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
dbExecute(
    con2,
    "CREATE TABLE
    IF NOT EXISTS vector_data AS
    SELECT * FROM read_parquet('~/Documents/airquality/outdoor/sites_airkorea_2010_2023_spt_yd.parquet')
    "
)
dbGetQuery(con2, "SELECT COUNT(*) AS n_rows FROM vector_data")
dbDisconnect(con2, shutdown = TRUE)


# Open DuckDB connection (read-only, no import)
con_time <- dbConnect(duckdb::duckdb(), dbdir = "summary.duckdb")

# Example: Run a spatial query directly on the Parquet file using DuckDB's spatial extension
# (Assuming 'geometry' column in WKT and a raster summary table exists)
dbExecute(con_time, "INSTALL spatial; LOAD spatial;")

# Example query: Join vector parquet with raster summary by 'id' and filter by spatial predicate (e.g., within a bounding box)
query <- "
COPY (
    SELECT v.*, ST_AsText(ST_Point(v.lon, v.lat)) AS geom
    FROM read_parquet(
        '~/Documents/airquality/outdoor/sites_airkorea_2010_2023_spt_yd.parquet'
    ) AS v
    WHERE ST_Within(ST_Point(v.lon, v.lat), ST_GeomFromText('POLYGON((126 37, 127 37, 127 38, 126 38, 126 37))')) AND CAST(v.date AS TIMESTAMP) = '2023-06-01 15:00:00'
    ) TO 'spatial_query_result.parquet' (FORMAT PARQUET);
"
# query <- "
# SELECT v.*, r.raster_mean
# FROM read_parquet(
#     '~/Documents/airquality/outdoor/sites_airkorea_2010_2023_spt_yd.parquet'
# ) AS v
# LEFT JOIN vector_raster_summary AS r ON v.id = r.id
# WHERE ST_Within(ST_Point(v.lon, v.lat), ST_GeomFromText('POLYGON((126 37, 127 37, 127 38, 126 38, 126 37))')) AND CAST(v.date AS DATE) = '2023-06-01'
# "

# Run query and collect result into R
spatial_result <- dbExecute(con_time, query)

# Save result as a new Parquet file
write_parquet(spatial_result, "spatial_query_result.parquet")

dbDisconnect(con_time, shutdown = TRUE)

# Demonstrate time information query using DuckDB

# Connect to DuckDB
con_time <- dbConnect(duckdb::duckdb(), dbdir = "summary.duckdb")

# Example: Assume 'vector_data' table has a 'timestamp' column in ISO8601 format
# Query: Count records per year, removing time zone info by casting to VARCHAR
time_query <- "
SELECT 
    strftime('%Y', CAST(date AS TIMESTAMP)) AS year,
    COUNT(*) AS n_records
FROM read_parquet(
    '~/Documents/airquality/outdoor/sites_airkorea_2010_2023_spt_yd.parquet'
)
GROUP BY year
ORDER BY year
"

time_summary <- dbGetQuery(con_time, time_query)
print(time_summary)

dbDisconnect(con_time, shutdown = TRUE)

# Export filtered data from DuckDB to Parquet
con_export <- dbConnect(duckdb::duckdb(), dbdir = "summary.duckdb")

dbExecute(
    con_export,
    "COPY (
            SELECT *
            FROM vector_data
            WHERE id IS NOT NULL
    ) TO 'filtered_vector_data.parquet' (FORMAT PARQUET);"
)

dbDisconnect(con_export, shutdown = TRUE)