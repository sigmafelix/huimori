use thiserror::Error;

#[derive(Error, Debug)]
pub enum FocalMeanError {
    #[error("GDAL error: {0}")]
    Gdal(#[from] gdal::errors::GdalError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Array shape error: {0}")]
    ShapeError(#[from] ndarray::ShapeError),

    #[error("Invalid radius: {0} meters (must be positive)")]
    InvalidRadius(f64),

    #[error("No valid classes found in input raster")]
    NoValidClasses,

    #[error("Input raster has invalid dimensions: {0}x{1}")]
    InvalidDimensions(usize, usize),

    #[error("Pixel size is non-positive: {0}")]
    InvalidPixelSize(f64),

    #[error("Invalid nodata value")]
    InvalidNodata,

    #[error("Invalid chunk size: {0} (must be positive)")]
    InvalidChunkSize(usize),

    #[error("Invalid chunk bounds: output=[{0},{1}]-[{2},{3}], read=[{4},{5}]-[{6},{7}]")]
    InvalidChunkBounds(usize, usize, usize, usize, usize, usize, usize, usize),

    #[error("COG creation failed: {0}")]
    CogCreationFailed(String),

    #[error("CRS error: {0}")]
    CrsError(String),

    #[error("Invalid compression type: {0}")]
    InvalidCompression(String),

    #[error("Invalid tile size: {0} (must be multiple of 16)")]
    InvalidTileSize(usize),
}

pub type Result<T> = std::result::Result<T, FocalMeanError>;
