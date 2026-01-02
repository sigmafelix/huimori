use clap::Parser;
use env_logger::Env;
use log::{info, warn};

mod chunking;
mod cli;
mod cog;
mod crs;
mod error;
mod focal;
mod io;

use chunking::ChunkGrid;
use cli::Args;
use crs::{calculate_radius_in_cells, detect_radius_mode};
use error::{FocalMeanError, Result};
use gdal::{Dataset, Metadata};

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

    // Validate arguments
    if args.radius <= 0.0 {
        return Err(FocalMeanError::InvalidRadius(args.radius));
    }

    if args.cog {
        cog::validate_compression(&args.compression)?;
        cog::validate_tile_size(args.tile_size)?;
    }

    // Open input dataset and extract metadata
    info!("Opening input raster: {}", args.input);
    let input_dataset = Dataset::open(&args.input)?;
    let metadata = io::extract_metadata_from_dataset(&input_dataset)?;

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

    // Detect CRS and calculate radius in cells
    let radius_mode = detect_radius_mode(&metadata.projection, args.radius_in_cells);
    let radius_in_cells =
        calculate_radius_in_cells(args.radius, metadata.pixel_width, radius_mode);

    // Decide between chunked and full-raster processing
    let use_chunking = args.chunksize > 0
        && (metadata.width > args.chunksize || metadata.height > args.chunksize);

    if use_chunking {
        info!("Using chunked processing (chunk size: {})", args.chunksize);
        process_chunked(
            &input_dataset,
            &metadata,
            &args,
            radius_in_cells,
            nodata_value,
        )?;
    } else {
        info!("Using full-raster processing (no chunking)");
        process_full_raster(
            &input_dataset,
            &metadata,
            &args,
            radius_in_cells,
            nodata_value,
        )?;
    }

    info!("=== Done! ===");
    Ok(())
}

/// Process the entire raster at once (legacy method, for backward compatibility)
fn process_full_raster(
    _input_dataset: &Dataset,
    metadata: &io::RasterMetadata,
    args: &Args,
    radius_in_cells: usize,
    nodata_value: Option<i32>,
) -> Result<()> {
    // Read entire raster
    info!("Reading entire raster into memory...");
    let (data, _) = io::read_input_raster(&args.input)?;

    // Calculate focal means
    info!("Calculating focal means...");
    let (class_fractions, classes) = focal::calculate_focal_means(&data, radius_in_cells, nodata_value)?;

    info!("Found {} classes: {:?}", classes.len(), classes);

    // Write output
    info!("Writing output: {}", args.output);

    if args.cog {
        // Create with COG options
        let options = cog::create_dataset_options(&args.compression, args.tile_size);
        let mut dataset =
            io::create_output_dataset(&args.output, metadata, classes.len(), options)?;

        dataset.set_geo_transform(&metadata.geotransform)?;
        dataset.set_projection(&metadata.projection)?;

        // Write bands
        for (i, (band_data, &class_value)) in class_fractions.iter().zip(&classes).enumerate() {
            let band_index = i + 1;
            let mut raster_band = dataset.rasterband(band_index)?;

            let band_slice = band_data.as_slice().expect("Array must be contiguous");
            let mut buffer =
                gdal::raster::Buffer::new((metadata.width, metadata.height), band_slice.to_vec());

            raster_band.write((0, 0), (metadata.width, metadata.height), &mut buffer)?;
            raster_band.set_description(&format!("class_{:02}", class_value))?;
            raster_band.set_no_data_value(Some(f64::NAN))?;
        }

        // Build overviews
        cog::build_overviews(&mut dataset)?;
    } else {
        // Standard output
        io::write_multiband_output(&args.output, &class_fractions, &classes, metadata)?;
    }

    Ok(())
}

/// Process the raster in chunks
fn process_chunked(
    input_dataset: &Dataset,
    metadata: &io::RasterMetadata,
    args: &Args,
    radius_in_cells: usize,
    nodata_value: Option<i32>,
) -> Result<()> {
    // Extract classes by scanning the dataset
    let classes = io::extract_classes_from_dataset(input_dataset, nodata_value)?;
    info!("Found {} classes: {:?}", classes.len(), classes);

    // Create chunk grid
    let chunk_grid = ChunkGrid::new(
        metadata.width,
        metadata.height,
        args.chunksize,
        radius_in_cells,
    );

    info!(
        "Processing {} chunks ({}x{} grid)",
        chunk_grid.total_chunks, chunk_grid.num_chunks_x, chunk_grid.num_chunks_y
    );

    // Create output dataset with appropriate options
    let options = if args.cog {
        cog::create_dataset_options(&args.compression, args.tile_size)
    } else {
        vec![]
    };

    let mut output_dataset =
        io::create_output_dataset(&args.output, metadata, classes.len(), options)?;

    output_dataset.set_geo_transform(&metadata.geotransform)?;
    output_dataset.set_projection(&metadata.projection)?;

    // Set band descriptions and nodata
    for (i, &class_value) in classes.iter().enumerate() {
        let band_index = i + 1;
        let mut raster_band = output_dataset.rasterband(band_index)?;
        raster_band.set_description(&format!("class_{:02}", class_value))?;
        raster_band.set_no_data_value(Some(f64::NAN))?;
    }

    // Process each chunk
    for (chunk_idx, bounds) in chunk_grid.iter() {
        info!(
            "Processing chunk {}/{} ({:.1}%)",
            chunk_idx + 1,
            chunk_grid.total_chunks,
            (chunk_idx + 1) as f64 / chunk_grid.total_chunks as f64 * 100.0
        );

        // Read padded chunk
        let chunk_data = io::read_padded_chunk(input_dataset, &bounds)?;

        // Calculate focal means for this chunk
        let (chunk_fractions, _) =
            focal::calculate_focal_means(&chunk_data, radius_in_cells, nodata_value)?;

        // Extract central region (remove padding) and write to output
        for (band_idx, chunk_fraction) in chunk_fractions.iter().enumerate() {
            let central_region = io::extract_central_region(chunk_fraction, &bounds)?;
            io::write_chunk_to_band(
                &mut output_dataset,
                band_idx + 1,
                &central_region,
                &bounds,
            )?;
        }
    }

    // Build overviews if COG is requested
    if args.cog {
        cog::build_overviews(&mut output_dataset)?;
    }

    info!("Successfully processed all {} chunks", chunk_grid.total_chunks);
    Ok(())
}
