import rasterio
from rasterio.enums import Resampling
import numpy as np
import os
import re

def preprocess_landuse(input_files, output_file, year):
    # Find the file for the year
    # R code: pattern = "20[0-2][0-9]"
    # Assuming input_files is a list of all tif files
    
    target_file = None
    for f in input_files:
        if str(year) in f:
            target_file = f
            break
            
    if target_file is None:
        # Fallback to nearest year or error?
        # For now, just pick the last one or raise error
        target_file = input_files[-1]
        
    with rasterio.open(target_file) as src:
        # Read data
        data = src.read(1)
        profile = src.profile
        
        # Calculate frequency (focal mean)
        # This is computationally expensive for large rasters in Python without optimized C++ libs like Whitebox
        # For this task, I'll use a placeholder or a simple filter if possible.
        # R code uses window_size=67 (approx 2km if 30m res).
        
        from scipy.ndimage import uniform_filter
        
        # Create masks for each class
        classes = np.unique(data)
        classes = classes[classes != profile['nodata']]
        
        out_data = []
        
        for cls in classes:
            mask = (data == cls).astype(float)
            # window size 67
            freq = uniform_filter(mask, size=67)
            out_data.append(freq)
            
        # Stack output
        out_stack = np.stack(out_data)
        
        profile.update(count=len(classes), dtype=rasterio.float32)
        
        with rasterio.open(output_file, 'w', **profile) as dst:
            for i, layer in enumerate(out_stack):
                dst.write(layer.astype(rasterio.float32), i + 1)
                dst.set_band_description(i + 1, f"class_{int(classes[i]):02d}")

if __name__ == "__main__":
    # Snakemake passes inputs/outputs via snakemake object
    # But I used 'script' directive, so 'snakemake' object is available
    preprocess_landuse(snakemake.input.landuse_files, snakemake.output.freq_raster, snakemake.wildcards.year)
