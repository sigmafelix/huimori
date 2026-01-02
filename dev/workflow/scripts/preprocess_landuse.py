import rasterio
from rasterio.enums import Resampling
import numpy as np
import os
import re
from whitebox.whitebox_tools import WhiteboxTools
wbt = WhiteboxTools()

def preprocess_landuse(input_raster, output_dir, year):
    """
    Calculates the fraction of each land use class within a neighborhood using WhiteboxTools.
    This is equivalent to the focal mean approach but optimized.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Reclassify to isolate each class (binary masks)
    # We need to know the classes first. 
    # Since WBT works on files, we might need to inspect the raster first with rasterio
    classes = []
    with rasterio.open(input_raster) as src:
        data = src.read(1)
        nodata = src.nodata
        unique_vals = np.unique(data)
        classes = [c for c in unique_vals if c != nodata]
    
    fraction_files = []
    
    for cls in classes:
        cls_int = int(cls)
        # Create a binary raster for this class: 1 where class matches, 0 otherwise
        # Reclass string format: "class_val;1;0" (assign 1 to class_val, 0 to everything else)
        # Actually WBT Reclass is complex. Easier to use EqualTo.
        
        binary_raster = os.path.join(output_dir, f"binary_{year}_{cls_int}.tif")
        
        # wbt.equal_to(input1, input2, output)
        # input2 can be a constant value
        wbt.equal_to(
            input1=input_raster, 
            input2=str(cls_int), 
            output=binary_raster
        )
        
        # Calculate focal mean (frequency) on the binary raster
        # Window size 67x67 (approx 2km at 30m resolution)
        freq_raster = os.path.join(output_dir, f"frac_{year}_class_{cls_int}.tif")
        
        # wbt.mean_filter(input, output, filterx, filtery)
        wbt.mean_filter(
            i=binary_raster, 
            output=freq_raster, 
            filterx=67, 
            filtery=67
        )
        
        fraction_files.append(freq_raster)
        
        # Clean up binary raster
        if os.path.exists(binary_raster):
            os.remove(binary_raster)
            
    return fraction_files

# calculate_class_fractions("/mnt/s/Korea/landuse/glc_fcs30d/lc_glc_fcs30d_30m_2010.tif", "/mnt/s/Projects", 2010)


# def preprocess_landuse(input_files, output_file, year):
#     # Find the file for the year
#     # R code: pattern = "20[0-2][0-9]"
#     # Assuming input_files is a list of all tif files
    
#     target_file = None
#     for f in input_files:
#         if str(year) in f:
#             target_file = f
#             break
            
#     if target_file is None:
#         # Fallback to nearest year or error?
#         # For now, just pick the last one or raise error
#         target_file = input_files[-1]
        
#     with rasterio.open(target_file) as src:
#         # Read data
#         data = src.read(1)
#         profile = src.profile
        
#         # Calculate frequency (focal mean)
#         # This is computationally expensive for large rasters in Python without optimized C++ libs like Whitebox
#         # For this task, I'll use a placeholder or a simple filter if possible.
#         # R code uses window_size=67 (approx 2km if 30m res).
        
#         from scipy.ndimage import uniform_filter
        
#         # Create masks for each class
#         classes = np.unique(data)
#         classes = classes[classes != profile['nodata']]
        
#         out_data = []
        
#         for cls in classes:
#             mask = (data == cls).astype(float)
#             # window size 67
#             freq = uniform_filter(mask, size=67)
#             out_data.append(freq)
            
#         # Stack output
#         out_stack = np.stack(out_data)
        
#         profile.update(count=len(classes), dtype=rasterio.float32)
        
#         with rasterio.open(output_file, 'w', **profile) as dst:
#             for i, layer in enumerate(out_stack):
#                 dst.write(layer.astype(rasterio.float32), i + 1)
#                 dst.set_band_description(i + 1, f"class_{int(classes[i]):02d}")

if __name__ == "__main__":
    # Snakemake passes inputs/outputs via snakemake object
    # But I used 'script' directive, so 'snakemake' object is available
    preprocess_landuse(snakemake.input.landuse_files, snakemake.output.freq_raster, snakemake.wildcards.year)


