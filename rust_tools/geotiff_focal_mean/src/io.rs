use crate::error::{FocalMeanError, Result};
use gdal::raster::RasterBand;
use gdal::{Dataset, DriverManager, Metadata};
use log::{debug, info};
use ndarray::Array2;

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
