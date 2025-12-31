import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial.distance import cdist
from shapely.geometry import Point
import rasterio
from rasterio.mask import mask

def gw_emittors(input_gdf, target_gdf, clip_gdf, wfun="gaussian", bw=1000, weight=None, emission_field = None, dist_method="geodesic"):
    """
    Calculate geographically weighted emissions.
    """
    # Ensure CRS match (EPSG:5179 recommended for Korea)
    target_crs = "EPSG:5179"
    if input_gdf.crs != target_crs:
        input_gdf = input_gdf.to_crs(target_crs)
    if target_gdf.crs != target_crs:
        target_gdf = target_gdf.to_crs(target_crs)
    if clip_gdf.crs != target_crs:
        clip_gdf = clip_gdf.to_crs(target_crs)
        
    # Intersect input with clip
    # We only care about inputs inside the clip
    input_in_clip = gpd.clip(input_gdf, clip_gdf)
    
    if input_in_clip.empty:
        input_gdf['gw_emission'] = 0
        return input_gdf
    
    # Subset target by clip
    target_clip = gpd.clip(target_gdf, clip_gdf)
    
    if target_clip.empty:
        input_gdf['gw_emission'] = 0
        return input_gdf
    
    if emission_field is not None:
        if emission_field not in target_clip.columns:
            raise UserWarning(f"emission_field '{emission_field}' not found in target_gdf columns. Initiating with ones...")
            target_clip['emission'] = 1
        else:
            target_clip = target_clip.rename(columns={emission_field: 'emission'})
    else:
        target_clip['emission'] = 1

    # Calculate distances
    coords_input = np.array([(p.x, p.y) for p in input_in_clip.geometry])
    coords_target = np.array([(p.x, p.y) for p in target_clip.geometry])
    
    # Using Euclidean distance on projected coordinates (EPSG:5179 is metric)
    dists = cdist(coords_input, coords_target)
    
    gw_emissions = []
    
    if weight is None:
        weight = np.ones(len(target_clip))
    
    # Ensure 'emission' column exists
    if 'emission' not in target_clip.columns:
        # Try to find a column that looks like emission or use 1?
        # The R code assumes 'emission' column exists in target
        raise ValueError("Target GDF must have 'emission' column")

    target_emissions = target_clip['emission'].values
    
    for i in range(len(input_in_clip)):
        dist_vec = dists[i]
        # Filter by bandwidth to speed up
        mask_bw = dist_vec <= bw
        
        if not np.any(mask_bw):
            gw_emissions.append(0)
            continue
            
        d_sub = dist_vec[mask_bw]
        w_sub = weight[mask_bw]
        e_sub = target_emissions[mask_bw]
        
        if wfun == "gaussian":
            # R's dnorm(x) is standard normal density
            w = (1 / np.sqrt(2 * np.pi)) * np.exp(-0.5 * (d_sub / bw)**2)
        elif wfun == "exponential":
            w = np.exp(-d_sub / bw)
        elif wfun == "tricube":
            w = (1 - (d_sub / bw)**3)**3
        elif wfun == "epanechnikov":
            w = 0.75 * (1 - (d_sub / bw)**2)
        else:
            raise ValueError(f"Unknown wfun: {wfun}")
            
        w = w * w_sub
        
        if np.sum(w) == 0:
            gw_emissions.append(0)
        else:
            gw_emissions.append(np.sum(e_sub * w) / np.sum(w))
            
    # Assign back
    # We need to map back to the original input_gdf
    # We can use the index if it was preserved
    input_in_clip['gw_emission'] = gw_emissions
    
    # Join back to original
    result = input_gdf.copy()
    result['gw_emission'] = 0 # Default to 0
    result.update(input_in_clip[['gw_emission']])
    
    return result

def extract_at_point(raster_path, gdf, col_name="value"):
    """
    Extract raster values at points.
    Returns a list of values (if single band) or a DataFrame (if multi-band).
    """
    with rasterio.open(raster_path) as src:
        # Reproject points if needed
        if gdf.crs != src.crs:
            gdf = gdf.to_crs(src.crs)
            
        coords = [(x, y) for x, y in zip(gdf.geometry.x, gdf.geometry.y)]
        
        # sample returns a generator
        sampled = list(src.sample(coords))
        
        if src.count == 1:
            return [x[0] for x in sampled]
        else:
            # Multi-band
            cols = [src.descriptions[i] or f"band_{i+1}" for i in range(src.count)]
            return pd.DataFrame(sampled, columns=cols, index=gdf.index)
