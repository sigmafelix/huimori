use crate::error::{FocalMeanError, Result};
use log::{debug, info};
use ndarray::Array2;
use rayon::prelude::*;
use std::collections::HashSet;

/// Extract unique classes from the raster, excluding nodata
fn extract_classes(data: &Array2<i32>, nodata: Option<i32>) -> Vec<i32> {
    let mut classes: HashSet<i32> = HashSet::new();

    for &value in data.iter() {
        if let Some(nd) = nodata {
            if value == nd {
                continue;
            }
        }
        classes.insert(value);
    }

    let mut class_vec: Vec<i32> = classes.into_iter().collect();
    class_vec.sort_unstable();
    class_vec
}

/// Calculate focal mean for all classes in the raster
pub fn calculate_focal_means(
    data: &Array2<i32>,
    pixel_size: f64,
    radius_meters: f64,
    nodata: Option<i32>,
) -> Result<(Vec<Array2<f32>>, Vec<i32>)> {
    if radius_meters <= 0.0 {
        return Err(FocalMeanError::InvalidRadius(radius_meters));
    }

    // Convert radius from meters to cells
    let window_cells = (radius_meters / pixel_size).ceil() as usize;
    info!(
        "Using window radius of {} cells ({} meters at {} meter resolution)",
        window_cells, radius_meters, pixel_size
    );

    // Extract unique classes
    let classes = extract_classes(data, nodata);
    if classes.is_empty() {
        return Err(FocalMeanError::NoValidClasses);
    }

    info!("Found {} unique classes: {:?}", classes.len(), classes);

    // Process each class in parallel
    let class_fractions: Vec<Array2<f32>> = classes
        .par_iter()
        .map(|&class_value| {
            debug!("Processing class {}", class_value);
            calculate_class_fraction(data, class_value, window_cells, nodata)
        })
        .collect();

    Ok((class_fractions, classes))
}

/// Calculate the fraction of a specific class in the focal window for each pixel
fn calculate_class_fraction(
    data: &Array2<i32>,
    target_class: i32,
    window_radius: usize,
    nodata: Option<i32>,
) -> Array2<f32> {
    let (nrows, ncols) = data.dim();

    // Process rows in parallel
    let rows: Vec<Vec<f32>> = (0..nrows)
        .into_par_iter()
        .map(|row| {
            (0..ncols)
                .map(|col| {
                    compute_window_fraction(
                        data,
                        row,
                        col,
                        target_class,
                        window_radius,
                        nodata,
                    )
                })
                .collect()
        })
        .collect();

    // Flatten the rows into a single vector
    let flat_data: Vec<f32> = rows.into_iter().flatten().collect();

    // Create Array2 from the flattened data
    Array2::from_shape_vec((nrows, ncols), flat_data).expect("Shape mismatch")
}

/// Compute the fraction of target_class within a square window centered at (center_row, center_col)
/// This matches scipy.ndimage.uniform_filter behavior
fn compute_window_fraction(
    data: &Array2<i32>,
    center_row: usize,
    center_col: usize,
    target_class: i32,
    radius: usize,
    nodata: Option<i32>,
) -> f32 {
    let (nrows, ncols) = data.dim();
    let mut class_count = 0u32;
    let mut valid_count = 0u32;

    // Define square window bounds
    let row_min = center_row.saturating_sub(radius);
    let row_max = (center_row + radius + 1).min(nrows);
    let col_min = center_col.saturating_sub(radius);
    let col_max = (center_col + radius + 1).min(ncols);

    for r in row_min..row_max {
        for c in col_min..col_max {
            let value = data[[r, c]];

            // Skip nodata values
            if let Some(nd) = nodata {
                if value == nd {
                    continue;
                }
            }

            valid_count += 1;
            if value == target_class {
                class_count += 1;
            }
        }
    }

    // Return fraction (exclude nodata from denominator)
    if valid_count > 0 {
        class_count as f32 / valid_count as f32
    } else {
        // If no valid cells in window, return 0.0
        0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::arr2;

    #[test]
    fn test_window_fraction_all_same_class() {
        // 3x3 raster, all class 1
        let data = arr2(&[[1, 1, 1], [1, 1, 1], [1, 1, 1]]);
        let fraction = compute_window_fraction(&data, 1, 1, 1, 1, None);
        assert!((fraction - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_window_fraction_with_nodata() {
        // Test nodata exclusion from denominator
        let data = arr2(&[[1, 2, -9999], [2, 1, 2], [1, 2, 1]]);
        let fraction = compute_window_fraction(&data, 1, 1, 1, 1, Some(-9999));
        // 4 cells are class 1: (0,0), (1,1), (2,0), (2,2)
        // 4 cells are class 2: (0,1), (1,0), (1,2), (2,1)
        // 1 nodata cell: (0,2)
        // Fraction = 4/8 = 0.5
        assert!((fraction - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_window_fraction_mixed() {
        // 5x5 raster with class 1 and class 2
        let data = arr2(&[
            [1, 1, 2, 2, 2],
            [1, 1, 2, 2, 2],
            [1, 1, 1, 2, 2],
            [1, 1, 1, 1, 2],
            [1, 1, 1, 1, 1],
        ]);
        // At center (2,2), with radius 2, should include most cells
        let fraction_class1 = compute_window_fraction(&data, 2, 2, 1, 2, None);
        let fraction_class2 = compute_window_fraction(&data, 2, 2, 2, 2, None);

        // Both fractions should sum to approximately 1.0
        assert!((fraction_class1 + fraction_class2 - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_extract_classes() {
        let data = arr2(&[[1, 2, 3], [2, 3, -9999], [1, 1, 2]]);
        let classes = extract_classes(&data, Some(-9999));
        assert_eq!(classes, vec![1, 2, 3]);
    }

    #[test]
    fn test_extract_classes_no_nodata() {
        let data = arr2(&[[1, 2, 3], [2, 3, 1], [1, 1, 2]]);
        let classes = extract_classes(&data, None);
        assert_eq!(classes, vec![1, 2, 3]);
    }
}
