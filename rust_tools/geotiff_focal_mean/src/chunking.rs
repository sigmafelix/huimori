use log::debug;

#[derive(Debug, Clone)]
pub struct ChunkBounds {
    // Output bounds (what we write to output)
    pub output_x_min: usize,
    pub output_y_min: usize,
    pub output_x_max: usize,
    pub output_y_max: usize,

    // Read bounds (including padding for focal operations)
    pub read_x_min: usize,
    pub read_y_min: usize,
    pub read_x_max: usize,
    pub read_y_max: usize,

    // Padding amounts (for extracting output from padded data)
    pub pad_left: usize,
    pub pad_right: usize,
    pub pad_top: usize,
    pub pad_bottom: usize,
}

impl ChunkBounds {
    pub fn read_width(&self) -> usize {
        self.read_x_max - self.read_x_min
    }

    pub fn read_height(&self) -> usize {
        self.read_y_max - self.read_y_min
    }

    pub fn output_width(&self) -> usize {
        self.output_x_max - self.output_x_min
    }

    pub fn output_height(&self) -> usize {
        self.output_y_max - self.output_y_min
    }
}

pub struct ChunkGrid {
    raster_width: usize,
    raster_height: usize,
    chunk_size: usize,
    padding: usize,
    pub num_chunks_x: usize,
    pub num_chunks_y: usize,
    pub total_chunks: usize,
}

impl ChunkGrid {
    pub fn new(
        raster_width: usize,
        raster_height: usize,
        chunk_size: usize,
        padding: usize,
    ) -> Self {
        // Calculate number of chunks needed (ceiling division)
        let num_chunks_x = (raster_width + chunk_size - 1) / chunk_size;
        let num_chunks_y = (raster_height + chunk_size - 1) / chunk_size;
        let total_chunks = num_chunks_x * num_chunks_y;

        debug!(
            "ChunkGrid: {}x{} raster, chunk_size={}, padding={} → {}x{} chunks ({} total)",
            raster_width, raster_height, chunk_size, padding, num_chunks_x, num_chunks_y, total_chunks
        );

        Self {
            raster_width,
            raster_height,
            chunk_size,
            padding,
            num_chunks_x,
            num_chunks_y,
            total_chunks,
        }
    }

    pub fn iter(&self) -> ChunkIterator<'_> {
        ChunkIterator::new(self)
    }

    pub fn get_chunk_bounds(&self, chunk_idx: usize) -> ChunkBounds {
        // Convert linear index to 2D coordinates
        let chunk_y = chunk_idx / self.num_chunks_x;
        let chunk_x = chunk_idx % self.num_chunks_x;

        // Calculate output bounds (what we'll write to output)
        let output_x_min = chunk_x * self.chunk_size;
        let output_y_min = chunk_y * self.chunk_size;
        let output_x_max = ((chunk_x + 1) * self.chunk_size).min(self.raster_width);
        let output_y_max = ((chunk_y + 1) * self.chunk_size).min(self.raster_height);

        // Calculate read bounds (including padding for focal operations)
        let read_x_min = output_x_min.saturating_sub(self.padding);
        let read_y_min = output_y_min.saturating_sub(self.padding);
        let read_x_max = (output_x_max + self.padding).min(self.raster_width);
        let read_y_max = (output_y_max + self.padding).min(self.raster_height);

        // Calculate actual padding (may be less at edges)
        let pad_left = output_x_min - read_x_min;
        let pad_top = output_y_min - read_y_min;
        let pad_right = read_x_max - output_x_max;
        let pad_bottom = read_y_max - output_y_max;

        debug!(
            "Chunk {} ({}, {}): output=[{}-{}, {}-{}], read=[{}-{}, {}-{}], padding=[L:{} R:{} T:{} B:{}]",
            chunk_idx, chunk_x, chunk_y,
            output_x_min, output_x_max, output_y_min, output_y_max,
            read_x_min, read_x_max, read_y_min, read_y_max,
            pad_left, pad_right, pad_top, pad_bottom
        );

        ChunkBounds {
            output_x_min,
            output_y_min,
            output_x_max,
            output_y_max,
            read_x_min,
            read_y_min,
            read_x_max,
            read_y_max,
            pad_left,
            pad_right,
            pad_top,
            pad_bottom,
        }
    }
}

pub struct ChunkIterator<'a> {
    grid: &'a ChunkGrid,
    current_idx: usize,
}

impl<'a> ChunkIterator<'a> {
    fn new(grid: &'a ChunkGrid) -> Self {
        Self {
            grid,
            current_idx: 0,
        }
    }
}

impl<'a> Iterator for ChunkIterator<'a> {
    type Item = (usize, ChunkBounds);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx < self.grid.total_chunks {
            let bounds = self.grid.get_chunk_bounds(self.current_idx);
            let idx = self.current_idx;
            self.current_idx += 1;
            Some((idx, bounds))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_grid_simple() {
        // 4000x4000 raster with 2000 chunk size and 67 padding
        let grid = ChunkGrid::new(4000, 4000, 2000, 67);
        assert_eq!(grid.num_chunks_x, 2);
        assert_eq!(grid.num_chunks_y, 2);
        assert_eq!(grid.total_chunks, 4);
    }

    #[test]
    fn test_chunk_bounds_interior() {
        // Interior chunk should have full padding
        let grid = ChunkGrid::new(4000, 4000, 2000, 67);
        let bounds = grid.get_chunk_bounds(0); // Top-left chunk

        assert_eq!(bounds.output_x_min, 0);
        assert_eq!(bounds.output_x_max, 2000);
        assert_eq!(bounds.read_x_min, 0); // Can't pad beyond edge
        assert_eq!(bounds.read_x_max, 2067); // 2000 + 67
        assert_eq!(bounds.pad_left, 0); // At edge
        assert_eq!(bounds.pad_right, 67);
    }

    #[test]
    fn test_chunk_bounds_edge() {
        // Edge chunk (not at raster boundary)
        let grid = ChunkGrid::new(4000, 4000, 2000, 67);
        let bounds = grid.get_chunk_bounds(3); // Bottom-right chunk

        assert_eq!(bounds.output_x_min, 2000);
        assert_eq!(bounds.output_x_max, 4000);
        assert_eq!(bounds.output_y_min, 2000);
        assert_eq!(bounds.output_y_max, 4000);

        // Can pad left/top but not right/bottom (at raster edge)
        assert_eq!(bounds.read_x_min, 1933); // 2000 - 67
        assert_eq!(bounds.read_x_max, 4000); // Can't exceed raster
        assert_eq!(bounds.pad_left, 67);
        assert_eq!(bounds.pad_right, 0); // At raster edge
    }

    #[test]
    fn test_chunk_iterator() {
        let grid = ChunkGrid::new(4000, 4000, 2000, 67);
        let chunks: Vec<_> = grid.iter().collect();

        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0].0, 0); // Chunk index
        assert_eq!(chunks[3].0, 3);
    }
}
