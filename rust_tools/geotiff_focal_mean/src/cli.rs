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

    /// Focal radius in meters (e.g., 2000 for 2km)
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
}
