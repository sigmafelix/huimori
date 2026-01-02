use crate::chunking::ChunkBounds;
use crate::error::{FocalMeanError, Result};
use gdal::cpl::CslStringList;
use gdal::raster::RasterBand;
use gdal::{Dataset, DriverManager, Metadata};
use log::{debug, info};
use ndarray::Array2;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct RasterMetadata {
    pub width: usize,
    pub height: usize,
    pub geotransform: [f64; 6],
    pub projection: String,
    pub nodata: Option<f64>,
    pub pixel_width: f64,
    pub pixel_height: f64,
}

/// Read input categorical raster and metadata
pub fn read_input_raster(path: &str) -> Result<(Array2<i32>, RasterMetadata)> {
    info!("Opening input raster: {}", path);
    let dataset = Dataset::open(path)?;

    let rasterband: RasterBand = dataset.rasterband(1)?;

    let width = rasterband.x_size() as usize;
    let height = rasterband.y_size() as usize;

    if width == 0 || height == 0 {
        return Err(FocalMeanError::InvalidDimensions(width, height));
    }

    let nodata = rasterband.no_data_value();

    // Get geotransform for pixel size calculation
    let geotransform = dataset.geo_transform()?;
    let pixel_width = geotransform[1].abs();
    let pixel_height = geotransform[5].abs();

    if pixel_width <= 0.0 {
        return Err(FocalMeanError::InvalidPixelSize(pixel_width));
    }

    debug!("Raster dimensions: {}x{}", width, height);
    debug!("Pixel size: {:.6} x {:.6}", pixel_width, pixel_height);

    // Read entire raster into ndarray
    let buffer = rasterband.read_as::<i32>((0, 0), (width, height), (width, height), None)?;
    // Buffer implements IntoIterator, so we can collect into a Vec
    let data_vec: Vec<i32> = buffer.into_iter().collect();
    let data = Array2::from_shape_vec((height, width), data_vec)?;

    let metadata = RasterMetadata {
        width,
        height,
        geotransform,
        projection: dataset.projection(),
        nodata,
        pixel_width,
        pixel_height,
    };

    Ok((data, metadata))
}

/// Write multi-band output GeoTIFF
pub fn write_multiband_output(
    path: &str,
    bands: &[Array2<f32>],
    classes: &[i32],
    metadata: &RasterMetadata,
) -> Result<()> {
    info!("Creating output raster: {}", path);

    let driver = DriverManager::get_driver_by_name("GTiff")?;

    let mut dataset = driver.create_with_band_type::<f32, _>(
        path,
        metadata.width,
        metadata.height,
        bands.len(),
    )?;

    // Set geotransform and projection
    dataset.set_geo_transform(&metadata.geotransform)?;
    dataset.set_projection(&metadata.projection)?;

    // Write each band
    for (i, (band_data, &class_value)) in bands.iter().zip(classes).enumerate() {
        let band_index = i + 1;
        debug!("Writing band {} for class {}", band_index, class_value);

        let mut raster_band = dataset.rasterband(band_index)?;

        // Convert Array2 to Buffer for GDAL
        // GDAL expects data in row-major order (which is how Array2 stores it)
        let band_slice = band_data.as_slice().expect("Array must be contiguous");
        let mut buffer = gdal::raster::Buffer::new(
            (metadata.width, metadata.height),
            band_slice.to_vec(),
        );

        raster_band.write((0, 0), (metadata.width, metadata.height), &mut buffer)?;

        // Set band description (e.g., "class_01")
        raster_band.set_description(&format!("class_{:02}", class_value))?;

        // Set nodata for output bands (use NaN for float32)
        raster_band.set_no_data_value(Some(f64::NAN))?;
    }

    info!("Successfully wrote {} bands to output", bands.len());
    Ok(())
}

/// Extract metadata from a dataset without reading all data
pub fn extract_metadata_from_dataset(dataset: &Dataset) -> Result<RasterMetadata> {
    let rasterband: RasterBand = dataset.rasterband(1)?;

    let width = rasterband.x_size() as usize;
    let height = rasterband.y_size() as usize;

    if width == 0 || height == 0 {
        return Err(FocalMeanError::InvalidDimensions(width, height));
    }

    let nodata = rasterband.no_data_value();
    let geotransform = dataset.geo_transform()?;
    let pixel_width = geotransform[1].abs();
    let pixel_height = geotransform[5].abs();

    if pixel_width <= 0.0 {
        return Err(FocalMeanError::InvalidPixelSize(pixel_width));
    }

    Ok(RasterMetadata {
        width,
        height,
        geotransform,
        projection: dataset.projection(),
        nodata,
        pixel_width,
        pixel_height,
    })
}

/// Scan dataset to extract unique classes without loading entire raster
pub fn extract_classes_from_dataset(dataset: &Dataset, nodata: Option<i32>) -> Result<Vec<i32>> {
    info!("Scanning raster to identify unique classes...");
    let rasterband = dataset.rasterband(1)?;
    let width = rasterband.x_size() as usize;
    let height = rasterband.y_size() as usize;

    let mut classes: HashSet<i32> = HashSet::new();

    // Read in chunks to avoid loading entire raster
    let scan_chunk_size = 2000;
    for y_offset in (0..height).step_by(scan_chunk_size) {
        let y_size = scan_chunk_size.min(height - y_offset);

        for x_offset in (0..width).step_by(scan_chunk_size) {
            let x_size = scan_chunk_size.min(width - x_offset);

            let buffer = rasterband.read_as::<i32>(
                (x_offset as isize, y_offset as isize),
                (x_size, y_size),
                (x_size, y_size),
                None,
            )?;

            for value in buffer.data() {
                if let Some(nd) = nodata {
                    if *value == nd {
                        continue;
                    }
                }
                classes.insert(*value);
            }
        }
    }

    let mut class_vec: Vec<i32> = classes.into_iter().collect();
    class_vec.sort_unstable();

    if class_vec.is_empty() {
        return Err(FocalMeanError::NoValidClasses);
    }

    info!("Found {} unique classes: {:?}", class_vec.len(), class_vec);
    Ok(class_vec)
}

/// Read a padded chunk from the input raster
pub fn read_padded_chunk(
    dataset: &Dataset,
    bounds: &ChunkBounds,
) -> Result<Array2<i32>> {
    let rasterband = dataset.rasterband(1)?;

    let read_width = bounds.read_width();
    let read_height = bounds.read_height();

    debug!(
        "Reading chunk: offset=({},{}), size=({},{})",
        bounds.read_x_min, bounds.read_y_min, read_width, read_height
    );

    let buffer = rasterband.read_as::<i32>(
        (bounds.read_x_min as isize, bounds.read_y_min as isize),
        (read_width, read_height),
        (read_width, read_height),
        None,
    )?;

    let data_vec: Vec<i32> = buffer.into_iter().collect();
    let data = Array2::from_shape_vec((read_height, read_width), data_vec)?;

    Ok(data)
}

/// Extract the central region from a padded array
pub fn extract_central_region(
    padded_data: &Array2<f32>,
    bounds: &ChunkBounds,
) -> Result<Array2<f32>> {
    let read_height = bounds.read_height();
    let read_width = bounds.read_width();

    // Verify dimensions match
    let (array_height, array_width) = padded_data.dim();
    if array_height != read_height || array_width != read_width {
        return Err(FocalMeanError::InvalidChunkBounds(
            bounds.output_x_min,
            bounds.output_y_min,
            bounds.output_x_max,
            bounds.output_y_max,
            bounds.read_x_min,
            bounds.read_y_min,
            bounds.read_x_max,
            bounds.read_y_max,
        ));
    }

    // Extract the central region (excluding padding)
    let y_start = bounds.pad_top;
    let y_end = read_height - bounds.pad_bottom;
    let x_start = bounds.pad_left;
    let x_end = read_width - bounds.pad_right;

    let central = padded_data.slice(ndarray::s![y_start..y_end, x_start..x_end]);

    Ok(central.to_owned())
}

/// Write a chunk to a specific band in the output dataset
pub fn write_chunk_to_band(
    dataset: &mut Dataset,
    band_index: usize,
    chunk_data: &Array2<f32>,
    bounds: &ChunkBounds,
) -> Result<()> {
    let mut raster_band = dataset.rasterband(band_index)?;

    let output_width = bounds.output_width();
    let output_height = bounds.output_height();

    // Verify dimensions
    let (array_height, array_width) = chunk_data.dim();
    if array_height != output_height || array_width != output_width {
        return Err(FocalMeanError::InvalidChunkBounds(
            bounds.output_x_min,
            bounds.output_y_min,
            bounds.output_x_max,
            bounds.output_y_max,
            bounds.read_x_min,
            bounds.read_y_min,
            bounds.read_x_max,
            bounds.read_y_max,
        ));
    }

    let chunk_slice = chunk_data.as_slice().expect("Array must be contiguous");
    let mut buffer = gdal::raster::Buffer::new((output_width, output_height), chunk_slice.to_vec());

    raster_band.write(
        (bounds.output_x_min as isize, bounds.output_y_min as isize),
        (output_width, output_height),
        &mut buffer,
    )?;

    debug!(
        "Wrote chunk to band {} at ({},{}) size {}x{}",
        band_index, bounds.output_x_min, bounds.output_y_min, output_width, output_height
    );

    Ok(())
}

/// Create an output dataset with specified options
pub fn create_output_dataset(
    path: &str,
    metadata: &RasterMetadata,
    num_bands: usize,
    options: Vec<String>,
) -> Result<Dataset> {
    info!("Creating output dataset: {}", path);

    let driver = DriverManager::get_driver_by_name("GTiff")?;

    // Convert Vec<String> to CslStringList
    let dataset = if options.is_empty() {
        driver.create_with_band_type::<f32, _>(
            path,
            metadata.width,
            metadata.height,
            num_bands,
        )?
    } else {
        let mut gdal_options = CslStringList::new();
        for opt in options {
            gdal_options.add_string(&opt)?;
        }

        driver.create_with_band_type_with_options::<f32, _>(
            path,
            metadata.width,
            metadata.height,
            num_bands,
            &gdal_options,
        )?
    };

    Ok(dataset)
}
