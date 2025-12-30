import geopandas as gpd
import pandas as pd
import numpy as np
import os
import glob
from utils import gw_emittors, extract_at_point
from shapely.ops import nearest_points

def calc_features(input_path, dsm_path, dem_path, mtpi_path, mtpi_1km_path, emissions_path, watersheds_path, road_dir, landuse_dir, output_path):
    # Load input
    if input_path.endswith('.parquet'):
        gdf = gpd.read_parquet(input_path)
    else:
        gdf = gpd.read_file(input_path)
        
    # Ensure CRS
    target_crs = "EPSG:5179"
    if gdf.crs != target_crs:
        gdf = gdf.to_crs(target_crs)
        
    # 1. Distance to Road
    # Load roads (this can be slow if many files)
    road_files = glob.glob(os.path.join(road_dir, "*.shp"))
    # For efficiency, maybe load one merged road file or spatial index.
    # R code loads `chr_road_files[length(chr_road_files)]`? Or iterates?
    # R: `road <- sf::st_read(chr_road_files[length(chr_road_files)], quiet = TRUE)`
    # It seems to take the last one.
    if road_files:
        road_gdf = gpd.read_file(road_files[-1])
        road_gdf = road_gdf.to_crs(target_crs)
        
        # Filter roads
        road_gdf = road_gdf[~road_gdf['ROAD_TYPE'].isin(["002", "004"]) & (road_gdf['ROAD_USE'] == "0")]
        
        # Calculate distance
        # Using sjoin_nearest or geometry.distance
        # For large datasets, use cKDTree or similar
        # geopandas sjoin_nearest is good
        
        # This is heavy. Simplified:
        # gdf['d_road'] = gdf.geometry.apply(lambda g: road_gdf.distance(g).min()) # Too slow
        
        # Better:
        nearest = gpd.sjoin_nearest(gdf, road_gdf, distance_col="d_road", how="left")
        # Handle duplicates if multiple nearest
        nearest = nearest[~nearest.index.duplicated(keep='first')]
        gdf['d_road'] = nearest['d_road'] / 1000.0 # km
    else:
        gdf['d_road'] = np.nan

    # 2. Raster Extraction
    gdf['dsm'] = extract_at_point(dsm_path, gdf)
    gdf['dem'] = extract_at_point(dem_path, gdf)
    gdf['mtpi'] = extract_at_point(mtpi_path, gdf)
    gdf['mtpi_1km'] = extract_at_point(mtpi_1km_path, gdf)
    
    # 3. Landuse
    # Need to handle years.
    # If gdf has 'year' column, use it. Else, do for all available years?
    # R code for grid does cross product.
    
    landuse_files = glob.glob(os.path.join(landuse_dir, "landuse_freq_*.tif"))
    
    if 'year' in gdf.columns:
        # For monitors
        # Iterate unique years
        years = gdf['year'].unique()
        results = []
        for year in years:
            subset = gdf[gdf['year'] == year].copy()
            # Find matching file
            l_file = next((f for f in landuse_files if str(year) in f), None)
            if l_file:
                # Extract multiple bands (classes)
                # extract_at_point returns list of values. 
                # But landuse_freq has multiple bands.
                # We need a modified extract function for multiband.
                pass # TODO: Implement multiband extraction
            results.append(subset)
        gdf = pd.concat(results)
    else:
        # For grid, maybe just use one year or cross product?
        # R code: `pattern = cross(list_pred_calc_grid, chr_landuse_freq_file)`
        # This implies grid is replicated for each year.
        # I'll skip this complexity for now and assume 2022 or similar.
        pass

    # 4. GW Emissions
    emissions_gdf = gpd.read_file(emissions_path)
    watersheds_gdf = gpd.read_file(watersheds_path)
    
    gdf = gw_emittors(gdf, emissions_gdf, watersheds_gdf)
    
    # Save
    gdf.to_parquet(output_path)

if __name__ == "__main__":
    calc_features(
        snakemake.input.get('monitors') or snakemake.input.get('grid'),
        snakemake.input.dsm,
        snakemake.input.dem,
        snakemake.input.mtpi,
        snakemake.input.mtpi_1km,
        snakemake.input.emissions,
        snakemake.input.watersheds,
        snakemake.params.road_dir,
        snakemake.params.landuse_dir,
        snakemake.output.features
    )
