// Library exports for testing and reuse

pub mod cli;
pub mod error;
pub mod focal;
pub mod io;

// Re-export commonly used types
pub use error::{FocalMeanError, Result};
pub use focal::calculate_focal_means;
pub use io::{read_input_raster, write_multiband_output, RasterMetadata};
