use clap::Parser;
use env_logger::Env;
use log::{info, warn};

mod cli;
mod error;
mod focal;
mod io;

use cli::Args;
use error::Result;

fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logger
    let log_level = if args.verbose { "debug" } else { "info" };
    env_logger::Builder::from_env(Env::default().default_filter_or(log_level)).init();

    info!("=== GeoTIFF Focal Mean Calculator ===");

    // Set thread pool size if specified
    if let Some(n_threads) = args.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(n_threads)
            .build_global()
            .expect("Failed to build thread pool");
        info!("Using {} threads", n_threads);
    } else {
        info!("Using all available threads");
    }

    // Validate radius
    if args.radius <= 0.0 {
        return Err(error::FocalMeanError::InvalidRadius(args.radius));
    }

    // Read input raster
    info!("Reading input raster: {}", args.input);
    let (data, metadata) = io::read_input_raster(&args.input)?;

    info!("Raster size: {}x{}", metadata.width, metadata.height);
    info!(
        "Pixel size: {:.6} × {:.6}",
        metadata.pixel_width, metadata.pixel_height
    );

    // Warn if pixels are not square
    if (metadata.pixel_width - metadata.pixel_height).abs() > 1e-9 {
        warn!(
            "Non-square pixels detected ({:.6} x {:.6}), using width for calculations",
            metadata.pixel_width, metadata.pixel_height
        );
    }

    // Determine nodata value
    let nodata_value = if let Some(nd) = args.nodata {
        Some(nd as i32)
    } else {
        metadata.nodata.map(|v| v as i32)
    };

    if let Some(nd) = nodata_value {
        info!("Using nodata value: {}", nd);
    } else {
        info!("No nodata value specified");
    }

    // Calculate focal means
    info!("Calculating focal means (radius: {} m)", args.radius);
    let (class_fractions, classes) =
        focal::calculate_focal_means(&data, metadata.pixel_width, args.radius, nodata_value)?;

    info!("Found {} classes: {:?}", classes.len(), classes);

    // Write output
    info!("Writing output: {}", args.output);
    io::write_multiband_output(&args.output, &class_fractions, &classes, &metadata)?;

    info!("=== Done! ===");
    Ok(())
}
