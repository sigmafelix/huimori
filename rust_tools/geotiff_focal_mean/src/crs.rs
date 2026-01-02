use gdal::spatial_ref::SpatialRef;
use log::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RadiusMode {
    Meters, // Radius specified in meters
    Cells,  // Radius specified in number of cells
}

/// Detect if the CRS uses meters as linear unit
pub fn detect_radius_mode(projection_wkt: &str, force_cells: bool) -> RadiusMode {
    if force_cells {
        info!("Radius mode: CELLS (forced by --radius-in-cells flag)");
        return RadiusMode::Cells;
    }

    // Parse the spatial reference
    let spatial_ref = match SpatialRef::from_wkt(projection_wkt) {
        Ok(sr) => sr,
        Err(e) => {
            warn!(
                "Failed to parse projection WKT, defaulting to CELLS mode: {}",
                e
            );
            return RadiusMode::Cells;
        }
    };

    // Check if CRS is geographic (lat/lon in degrees)
    if spatial_ref.is_geographic() {
        info!("Geographic CRS detected (lat/lon), using radius as CELLS");
        return RadiusMode::Cells;
    }

    // Check if CRS is projected
    if spatial_ref.is_projected() {
        // Get the linear unit (meters = 1.0)
        let linear_units = spatial_ref.linear_units();

        // Check if unit is meters (value close to 1.0)
        // Most metric projected systems have linear_units around 1.0
        if (linear_units - 1.0).abs() < 0.01 {
            info!(
                "Projected CRS with meter units detected (units={:.6}), using radius as METERS",
                linear_units
            );
            return RadiusMode::Meters;
        } else {
            warn!(
                "Projected CRS with non-meter units detected (units={:.6}), using radius as CELLS",
                linear_units
            );
            return RadiusMode::Cells;
        }
    }

    // Default to cells for unknown cases
    warn!("Unknown CRS type, defaulting to radius as CELLS");
    RadiusMode::Cells
}

/// Calculate radius in cells based on mode
pub fn calculate_radius_in_cells(
    radius_value: f64,
    pixel_size: f64,
    mode: RadiusMode,
) -> usize {
    match mode {
        RadiusMode::Meters => {
            let cells = (radius_value / pixel_size).ceil() as usize;
            info!(
                "Radius: {} meters = {} cells (pixel size: {:.6} m)",
                radius_value, cells, pixel_size
            );
            cells
        }
        RadiusMode::Cells => {
            let cells = radius_value.ceil() as usize;
            info!("Radius: {} cells (specified directly)", cells);
            cells
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meters_mode_calculation() {
        let mode = RadiusMode::Meters;
        let cells = calculate_radius_in_cells(2000.0, 30.0, mode);
        assert_eq!(cells, 67); // ceil(2000/30) = 67
    }

    #[test]
    fn test_cells_mode_calculation() {
        let mode = RadiusMode::Cells;
        let cells = calculate_radius_in_cells(67.0, 30.0, mode);
        assert_eq!(cells, 67); // Direct specification
    }

    #[test]
    fn test_force_cells() {
        // Even with empty WKT, force_cells should override
        let mode = detect_radius_mode("", true);
        assert_eq!(mode, RadiusMode::Cells);
    }
}
