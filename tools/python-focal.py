"""
Focal Class Frequency Analysis with Real GeoTIFF Data
======================================================
Processes a GeoTIFF raster to calculate the focal frequency of each class
using a circular 67x67 window, outputting separate layers for each class.
"""

import numpy as np
from numba import jit, prange
import rasterio
from rasterio.transform import from_bounds
from pathlib import Path
import time
from typing import Tuple, Optional
import matplotlib.pyplot as plt


# ============================================================================
# Create Circular Window
# ============================================================================

def create_circular_window(window_size: int = 67) -> np.ndarray:
    """
    Create a circular window mask.
    
    Parameters:
    -----------
    window_size : int
        Size of the window (should be odd)
        
    Returns:
    --------
    np.ndarray : Boolean array where True = inside circle
    """
    if window_size % 2 == 0:
        window_size += 1  # Make odd if even
    
    center = window_size // 2
    y, x = np.ogrid[:window_size, :window_size]
    
    # Calculate distance from center
    dist_from_center = np.sqrt((x - center)**2 + (y - center)**2)
    
    # Create circular mask
    radius = window_size / 2
    circular_mask = dist_from_center <= radius
    
    return circular_mask


# ============================================================================
# Focal Class Frequency with Circular Window
# ============================================================================

@jit(nopython=True, parallel=True)
def _focal_class_freq_circular(data: np.ndarray,
                               classes: np.ndarray,
                               circular_mask: np.ndarray,
                               window_size: int) -> np.ndarray:
    """
    Count frequency of each class in a circular focal window.
    
    Parameters:
    -----------
    data : np.ndarray
        Input raster data (2D array)
    classes : np.ndarray
        Array of unique class values
    circular_mask : np.ndarray
        Boolean mask defining circular window
    window_size : int
        Size of the window
        
    Returns:
    --------
    np.ndarray : Shape (n_classes, rows, cols) with frequencies [0-1]
    """
    rows, cols = data.shape
    n_classes = len(classes)
    hw = window_size // 2  # half window
    result = np.zeros((n_classes, rows, cols), dtype=np.float32)
    
    # Process each pixel
    for i in prange(hw, rows - hw):
        for j in range(hw, cols - hw):
            # Count each class within circular window
            class_counts = np.zeros(n_classes, dtype=np.int32)
            total_valid = 0
            
            # Iterate over window
            for wi in range(window_size):
                for wj in range(window_size):
                    # Check if this position is within the circle
                    if circular_mask[wi, wj]:
                        row_idx = i - hw + wi
                        col_idx = j - hw + wj
                        val = data[row_idx, col_idx]
                        
                        if not np.isnan(val):
                            total_valid += 1
                            # Find which class this belongs to
                            for c_idx in range(n_classes):
                                if val == classes[c_idx]:
                                    class_counts[c_idx] += 1
                                    break
            
            # Calculate frequencies
            if total_valid > 0:
                for c_idx in range(n_classes):
                    result[c_idx, i, j] = class_counts[c_idx] / total_valid
            else:
                for c_idx in range(n_classes):
                    result[c_idx, i, j] = np.nan
    
    return result


def focal_class_frequency_circular(data: np.ndarray,
                                   window_size: int = 67,
                                   circular_mask: Optional[np.ndarray] = None) -> Tuple[np.ndarray, np.ndarray]:
    """
    Calculate frequency of each class in a circular focal window.
    
    Parameters:
    -----------
    data : np.ndarray
        Input raster data (2D array with integer classes)
    window_size : int
        Size of the moving window (default: 67)
    circular_mask : np.ndarray, optional
        Pre-computed circular mask (will be created if not provided)
        
    Returns:
    --------
    Tuple of (result_array, classes)
        result_array: shape (n_classes, rows, cols), values are frequencies [0-1]
        classes: array of unique class values found in the data
    """
    # Get unique classes (excluding NaN)
    classes = np.unique(data[~np.isnan(data)])
    classes = np.sort(classes)
    
    print(f"Found {len(classes)} unique classes: {classes}")
    
    # Create circular mask if not provided
    if circular_mask is None:
        circular_mask = create_circular_window(window_size)
        n_cells_in_circle = np.sum(circular_mask)
        print(f"Created circular window: {window_size}x{window_size} ({n_cells_in_circle} cells)")
    
    # Run focal operation
    print("Processing focal operation...")
    result = _focal_class_freq_circular(data, classes, circular_mask, window_size)
    
    return result, classes


# ============================================================================
# GeoTIFF I/O Functions
# ============================================================================

def read_geotiff(filepath: str) -> Tuple[np.ndarray, dict]:
    """
    Read a GeoTIFF file.
    
    Parameters:
    -----------
    filepath : str
        Path to the GeoTIFF file
        
    Returns:
    --------
    Tuple of (data, metadata)
        data: 2D numpy array
        metadata: dictionary with rasterio profile
    """
    print(f"\nReading GeoTIFF: {filepath}")
    
    with rasterio.open(filepath) as src:
        data = src.read(1).astype(float)  # Read first band
        
        # Replace nodata with NaN
        if src.nodata is not None:
            data[data == src.nodata] = np.nan
        
        metadata = src.profile.copy()
        
        print(f"  Shape: {data.shape}")
        print(f"  CRS: {src.crs}")
        print(f"  Transform: {src.transform}")
        print(f"  Data type: {data.dtype}")
        print(f"  NoData value: {src.nodata}")
        
        # Data statistics
        valid_data = data[~np.isnan(data)]
        if len(valid_data) > 0:
            print(f"  Value range: [{np.min(valid_data):.2f}, {np.max(valid_data):.2f}]")
            print(f"  Unique values: {len(np.unique(valid_data))}")
        
    return data, metadata


def save_multiband_geotiff(data_stack: np.ndarray,
                           output_path: str,
                           metadata: dict,
                           class_names: np.ndarray,
                           descriptions: Optional[list] = None) -> None:
    """
    Save multi-band GeoTIFF with one band per class.
    
    Parameters:
    -----------
    data_stack : np.ndarray
        Shape (n_classes, rows, cols)
    output_path : str
        Output file path
    metadata : dict
        Rasterio profile from input file
    class_names : np.ndarray
        Array of class values
    descriptions : list, optional
        Band descriptions
    """
    n_bands = data_stack.shape[0]
    
    # Update metadata for multi-band output
    metadata.update({
        'count': n_bands,
        'dtype': 'float32',
        'nodata': np.nan,
        'compress': 'lzw'
    })
    
    print(f"\nSaving multi-band GeoTIFF: {output_path}")
    print(f"  Number of bands: {n_bands}")
    
    with rasterio.open(output_path, 'w', **metadata) as dst:
        for i in range(n_bands):
            band_num = i + 1
            dst.write(data_stack[i].astype('float32'), band_num)
            
            # Set band description
            if descriptions and i < len(descriptions):
                dst.set_band_description(band_num, descriptions[i])
            else:
                dst.set_band_description(band_num, f"Class_{int(class_names[i])}_frequency")
    
    print(f"  ✓ Saved successfully")


def save_separate_geotiffs(data_stack: np.ndarray,
                           output_dir: str,
                           metadata: dict,
                           class_names: np.ndarray,
                           basename: str = "class") -> None:
    """
    Save each class frequency as a separate GeoTIFF.
    
    Parameters:
    -----------
    data_stack : np.ndarray
        Shape (n_classes, rows, cols)
    output_dir : str
        Output directory path
    metadata : dict
        Rasterio profile from input file
    class_names : np.ndarray
        Array of class values
    basename : str
        Base name for output files
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Update metadata for single-band output
    metadata.update({
        'count': 1,
        'dtype': 'float32',
        'nodata': np.nan,
        'compress': 'deflate'
    })
    
    print(f"\nSaving separate GeoTIFFs to: {output_dir}")
    
    for i, class_val in enumerate(class_names):
        filename = f"{basename}_{int(class_val):02d}_frequency.tif"
        filepath = output_path / filename
        
        with rasterio.open(filepath, 'w', **metadata) as dst:
            dst.write(data_stack[i].astype('float32'), 1)
            dst.set_band_description(1, f"Frequency of class {int(class_val)}")
        
        print(f"  ✓ Saved: {filename}")


# ============================================================================
# Visualization
# ============================================================================

def visualize_results(original_data: np.ndarray,
                     freq_stack: np.ndarray,
                     class_names: np.ndarray,
                     sample_size: int = 500,
                     output_path: Optional[str] = None) -> None:
    """
    Visualize original data and class frequencies.
    
    Parameters:
    -----------
    original_data : np.ndarray
        Original raster data
    freq_stack : np.ndarray
        Frequency results (n_classes, rows, cols)
    class_names : np.ndarray
        Class values
    sample_size : int
        Size of subset to visualize (for large rasters)
    output_path : str, optional
        Path to save figure
    """
    n_classes = len(class_names)
    
    # Take a sample if data is too large
    if original_data.shape[0] > sample_size:
        row_start = original_data.shape[0] // 2 - sample_size // 2
        col_start = original_data.shape[1] // 2 - sample_size // 2
        original_sample = original_data[row_start:row_start+sample_size, 
                                       col_start:col_start+sample_size]
        freq_sample = freq_stack[:, row_start:row_start+sample_size,
                                 col_start:col_start+sample_size]
    else:
        original_sample = original_data
        freq_sample = freq_stack
    
    # Create figure
    n_cols = min(3, n_classes + 1)
    n_rows = (n_classes + 2) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(5*n_cols, 5*n_rows))
    axes = axes.flatten() if n_classes > 0 else [axes]
    
    # Plot original
    im = axes[0].imshow(original_sample, cmap='tab10', interpolation='nearest')
    axes[0].set_title('Original Classes', fontsize=12, fontweight='bold')
    axes[0].axis('off')
    plt.colorbar(im, ax=axes[0], fraction=0.046, pad=0.04)
    
    # Plot each class frequency
    for i, class_val in enumerate(class_names):
        ax = axes[i + 1]
        im = ax.imshow(freq_sample[i], cmap='YlOrRd', vmin=0, vmax=1, 
                      interpolation='nearest')
        ax.set_title(f'Class {int(class_val)} Frequency', fontsize=12, fontweight='bold')
        ax.axis('off')
        plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label='Frequency')
    
    # Hide extra subplots
    for i in range(n_classes + 1, len(axes)):
        axes[i].axis('off')
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"\n✓ Visualization saved: {output_path}")
    
    plt.show()


# ============================================================================
# Main Processing Function
# ============================================================================

def process_geotiff(input_path: str,
                   output_path: Optional[str] = None,
                   output_dir: Optional[str] = None,
                   window_size: int = 67,
                   visualize: bool = True,
                   save_separate: bool = False) -> Tuple[np.ndarray, np.ndarray]:
    """
    Main function to process a GeoTIFF and calculate focal class frequencies.
    
    Parameters:
    -----------
    input_path : str
        Path to input GeoTIFF file
    output_path : str, optional
        Path for multi-band output GeoTIFF
    output_dir : str, optional
        Directory for separate output GeoTIFFs (one per class)
    window_size : int
        Size of circular window (default: 67)
    visualize : bool
        Whether to create visualization
    save_separate : bool
        Whether to save separate files for each class
        
    Returns:
    --------
    Tuple of (freq_stack, class_names)
        freq_stack: Array with shape (n_classes, rows, cols)
        class_names: Array of class values
    """
    print("=" * 80)
    print("FOCAL CLASS FREQUENCY ANALYSIS")
    print("=" * 80)
    
    # Read input
    data, metadata = read_geotiff(input_path)
    
    # Create circular window
    circular_mask = create_circular_window(window_size)
    
    # Warmup Numba (optional, for timing accuracy)
    print("\nWarming up Numba compiler...")
    sample = data[:100, :100].copy()
    _ = focal_class_frequency_circular(sample, window_size, circular_mask)
    print("  ✓ Compilation complete")
    
    # Process full raster
    print(f"\nProcessing full raster with {window_size}x{window_size} circular window...")
    start_time = time.time()
    
    freq_stack, class_names = focal_class_frequency_circular(
        data, window_size, circular_mask
    )
    
    elapsed = time.time() - start_time
    print(f"  ✓ Processing complete in {elapsed:.2f} seconds")
    
    # Print statistics
    print("\nOutput Statistics:")
    print("-" * 80)
    for i, class_val in enumerate(class_names):
        layer = freq_stack[i]
        valid = layer[~np.isnan(layer)]
        if len(valid) > 0:
            print(f"  Class {int(class_val):2d}: "
                  f"mean={np.mean(valid):.4f}, "
                  f"min={np.min(valid):.4f}, "
                  f"max={np.max(valid):.4f}")
    
    # Save outputs
    if output_path:
        descriptions = [f"Frequency of class {int(c)}" for c in class_names]
        save_multiband_geotiff(freq_stack, output_path, metadata, 
                              class_names, descriptions)
    
    if save_separate and output_dir:
        save_separate_geotiffs(freq_stack, output_dir, metadata, class_names)
    
    # Visualize
    if visualize:
        print("\nGenerating visualization...")
        viz_path = None
        if output_path:
            viz_path = str(Path(output_path).with_suffix('.png'))
        visualize_results(data, freq_stack, class_names, output_path=viz_path)
    
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    
    return freq_stack, class_names


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # ========================================================================
    # CONFIGURE YOUR PATHS HERE
    # ========================================================================
    years = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022]
    # Input GeoTIFF path
    
    for y in years:
        input_geotiff = f"/mnt/s/Korea/landuse/glc_fcs30d/lc_glc_fcs30d_30m_{y}.tif"
        
        # Output options
        output_multiband = f"/mnt/s/Korea/landuse/glc_freq_{y}.tif"
        output_directory = f"/mnt/s/Korea/landuse/separate_layers/{y}"
        
        # ========================================================================
        # PROCESS THE RASTER
        # ========================================================================
        
        freq_stack, class_names = process_geotiff(
            input_path=input_geotiff,
            output_path=output_multiband,        # Multi-band output
            output_dir=output_directory,         # Separate files (optional)
            window_size=67,                      # Circular 67x67 window
            visualize=False,                      # Create visualization
            save_separate=False                   # Save separate TIFFs per class
        )
        
        # ========================================================================
        # ACCESS RESULTS
        # ========================================================================
        
        print("\nAccessing results in Python:")
        print(f"  freq_stack.shape: {freq_stack.shape}")
        print(f"  class_names: {class_names}")
        
        # Get frequency layer for specific class
        class_1_frequency = freq_stack[0]  # First class
        print(f"  Class {int(class_names[0])} frequency shape: {class_1_frequency.shape}")
        
        # Example: Find pixels where class 1 has >50% frequency
        high_freq_pixels = class_1_frequency > 0.5
        print(f"  Pixels with >50% of class {int(class_names[0])}: "
              f"{np.sum(high_freq_pixels)} pixels")