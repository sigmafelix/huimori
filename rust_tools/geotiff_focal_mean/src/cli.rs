use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "focal-mean")]
#[command(about = "Calculate class fractions from categorical raster using focal mean")]
#[command(version)]
#[command(author = "Huimori Project")]
pub struct Args {
    /// Input GeoTIFF path (categorical raster)
    #[arg(short, long, value_name = "FILE")]
    pub input: String,

    /// Output GeoTIFF path (multi-band float32)
    #[arg(short, long, value_name = "FILE")]
    pub output: String,

    /// Focal radius in meters (e.g., 2000 for 2km) or cells (if --radius-in-cells is set)
    #[arg(short, long, value_name = "METERS")]
    pub radius: f64,

    /// Override nodata value (default: read from input)
    #[arg(long, value_name = "VALUE")]
    pub nodata: Option<f64>,

    /// Number of threads (default: all available)
    #[arg(short, long, value_name = "N")]
    pub threads: Option<usize>,

    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,

    /// Chunk size for processing (pixels). Default: 2000. Set to 0 to disable chunking.
    #[arg(long, default_value = "2000", value_name = "PIXELS")]
    pub chunksize: usize,

    /// Create Cloud-Optimized GeoTIFF (COG) with overviews
    #[arg(long)]
    pub cog: bool,

    /// Compression type for output (DEFLATE, LZW, ZSTD, NONE)
    #[arg(long, default_value = "DEFLATE", value_name = "TYPE")]
    pub compression: String,

    /// Tile size for COG output (pixels, must be multiple of 16)
    #[arg(long, default_value = "512", value_name = "PIXELS")]
    pub tile_size: usize,

    /// Interpret radius as number of cells instead of meters
    #[arg(long)]
    pub radius_in_cells: bool,
}
