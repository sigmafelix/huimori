use crate::error::{FocalMeanError, Result};
use gdal::Dataset;
use log::{debug, info};
use std::process::Command;

/// Validate compression type
pub fn validate_compression(compression: &str) -> Result<()> {
    let valid_types = ["DEFLATE", "LZW", "ZSTD", "NONE"];
    if !valid_types.contains(&compression) {
        return Err(FocalMeanError::InvalidCompression(compression.to_string()));
    }
    Ok(())
}

/// Validate tile size (must be multiple of 16)
pub fn validate_tile_size(tile_size: usize) -> Result<()> {
    if tile_size == 0 || tile_size % 16 != 0 {
        return Err(FocalMeanError::InvalidTileSize(tile_size));
    }
    Ok(())
}

/// Create dataset options for tiled output
pub fn create_dataset_options(compression: &str, tile_size: usize) -> Vec<String> {
    vec![
        format!("COMPRESS={}", compression),
        "TILED=YES".to_string(),
        format!("BLOCKXSIZE={}", tile_size),
        format!("BLOCKYSIZE={}", tile_size),
        "BIGTIFF=IF_SAFER".to_string(),
    ]
}

/// Build overviews using GDAL's internal overview generation
pub fn build_overviews(dataset: &mut Dataset) -> Result<()> {
    info!("Building overviews for COG...");

    // Calculate overview levels (powers of 2)
    let width = dataset.raster_size().0;
    let height = dataset.raster_size().1;
    let min_dim = width.min(height) as usize;

    let mut overview_levels: Vec<i32> = Vec::new();
    let mut level = 2;

    // Create overviews until minimum dimension is < 256
    while (min_dim / level) >= 256 {
        overview_levels.push(level as i32);
        level *= 2;
    }

    if overview_levels.is_empty() {
        debug!("Raster too small for overviews ({}x{}), skipping", width, height);
        return Ok(());
    }

    info!("Creating {} overview levels: {:?}", overview_levels.len(), overview_levels);

    // Build overviews using AVERAGE resampling (good for continuous data like fractions)
    // Empty bands list means build for all bands
    dataset
        .build_overviews("AVERAGE", &overview_levels, &[])
        .map_err(|e| FocalMeanError::CogCreationFailed(format!("Failed to build overviews: {}", e)))?;

    info!("Overviews created successfully");
    Ok(())
}

/// Convert to COG using gdal_translate if available
pub fn convert_to_cog(input_path: &str, output_path: &str, compression: &str, tile_size: usize) -> Result<()> {
    info!("Converting to Cloud-Optimized GeoTIFF using gdal_translate...");

    let output = Command::new("gdal_translate")
        .arg("-of")
        .arg("COG")
        .arg("-co")
        .arg(format!("COMPRESS={}", compression))
        .arg("-co")
        .arg(format!("BLOCKSIZE={}", tile_size))
        .arg("-co")
        .arg("BIGTIFF=IF_SAFER")
        .arg(input_path)
        .arg(output_path)
        .output();

    match output {
        Ok(result) => {
            if result.status.success() {
                info!("COG conversion successful");
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&result.stderr);
                Err(FocalMeanError::CogCreationFailed(format!(
                    "gdal_translate failed: {}",
                    stderr
                )))
            }
        }
        Err(e) => {
            // If gdal_translate is not available, fall back to manual method
            debug!("gdal_translate not available ({}), using manual COG creation", e);
            Ok(())
        }
    }
}

/// Validate that output is COG-compatible
pub fn validate_cog_structure(dataset: &Dataset) -> Result<()> {
    let (width, _height) = dataset.raster_size();

    // Check if tiled
    let rasterband = dataset.rasterband(1)?;
    let block_size = rasterband.block_size();

    if block_size.0 == width as usize && block_size.1 == 1 {
        return Err(FocalMeanError::CogCreationFailed(
            "Output is not tiled (scanline format detected)".to_string()
        ));
    }

    debug!("COG validation: tiled=yes, block_size={}x{}", block_size.0, block_size.1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_compression_valid() {
        assert!(validate_compression("DEFLATE").is_ok());
        assert!(validate_compression("LZW").is_ok());
        assert!(validate_compression("ZSTD").is_ok());
        assert!(validate_compression("NONE").is_ok());
    }

    #[test]
    fn test_validate_compression_invalid() {
        assert!(validate_compression("INVALID").is_err());
        assert!(validate_compression("jpeg").is_err());
    }

    #[test]
    fn test_validate_tile_size_valid() {
        assert!(validate_tile_size(256).is_ok());
        assert!(validate_tile_size(512).is_ok());
        assert!(validate_tile_size(1024).is_ok());
    }

    #[test]
    fn test_validate_tile_size_invalid() {
        assert!(validate_tile_size(0).is_err());
        assert!(validate_tile_size(100).is_err());
        assert!(validate_tile_size(513).is_err());
    }

    #[test]
    fn test_create_dataset_options() {
        let opts = create_dataset_options("DEFLATE", 512);
        assert_eq!(opts.len(), 5);
        assert!(opts.contains(&"COMPRESS=DEFLATE".to_string()));
        assert!(opts.contains(&"TILED=YES".to_string()));
        assert!(opts.contains(&"BLOCKXSIZE=512".to_string()));
        assert!(opts.contains(&"BLOCKYSIZE=512".to_string()));
    }
}
