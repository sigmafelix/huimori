import geopandas as gpd
import numpy as np
from shapely.geometry import Point
import sys

def prepare_grid(reference_path, output_path, resolution):
    print(f"Loading reference from {reference_path}...")
    # Load reference to get bounds
    ref_gdf = gpd.read_file(reference_path)
    
    # Ensure projected CRS for metric resolution
    target_crs = "EPSG:5179"
    if ref_gdf.crs != target_crs:
        ref_gdf = ref_gdf.to_crs(target_crs)
        
    minx, miny, maxx, maxy = ref_gdf.total_bounds
    print(f"Bounds: {minx}, {miny}, {maxx}, {maxy}")
    print(f"Generating grid with resolution {resolution}...")
    
    # Generate grid points
    # Use centroids of the grid cells
    x = np.arange(minx + resolution/2, maxx, resolution)
    y = np.arange(miny + resolution/2, maxy, resolution)
    
    # Create meshgrid
    xx, yy = np.meshgrid(x, y)
    
    # Flatten
    xx = xx.flatten()
    yy = yy.flatten()
    
    # Create points
    points = [Point(x, y) for x, y in zip(xx, yy)]
    
    # Create GeoDataFrame
    grid = gpd.GeoDataFrame(geometry=points, crs=target_crs)
    print(f"Generated {len(grid)} points. Filtering by reference shape...")
    
    # Filter points within the reference shape
    # Using sjoin is generally faster than within() loop
    # We only keep geometry to save space/time
    grid = gpd.sjoin(grid, ref_gdf[['geometry']], how='inner', predicate='intersects')
    
    # Clean up columns
    grid = grid[['geometry']].reset_index(drop=True)
    
    print(f"Saving {len(grid)} points to {output_path}...")
    # Save
    grid.to_parquet(output_path)

if __name__ == "__main__":
    prepare_grid(
        snakemake.input.reference,
        snakemake.output.grid_parquet,
        float(snakemake.params.resolution)
    )
