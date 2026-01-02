// Library exports for testing and reuse

pub mod chunking;
pub mod cli;
pub mod cog;
pub mod crs;
pub mod error;
pub mod focal;
pub mod io;

// Re-export commonly used types
pub use chunking::{ChunkBounds, ChunkGrid};
pub use cog::{build_overviews, create_dataset_options, validate_compression, validate_tile_size};
pub use crs::{calculate_radius_in_cells, detect_radius_mode, RadiusMode};
pub use error::{FocalMeanError, Result};
pub use focal::calculate_focal_means;
pub use io::{
    create_output_dataset, extract_classes_from_dataset, extract_central_region,
    extract_metadata_from_dataset, read_input_raster, read_padded_chunk, write_chunk_to_band,
    write_multiband_output, RasterMetadata,
};
