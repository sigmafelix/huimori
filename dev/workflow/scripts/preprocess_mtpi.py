import rasterio
from rasterio.enums import Resampling

def preprocess_mtpi(input_file, output_file):
    with rasterio.open(input_file) as src:
        # Aggregate by factor 11
        # new shape
        new_height = src.height // 11
        new_width = src.width // 11
        
        data = src.read(
            out_shape=(src.count, new_height, new_width),
            resampling=Resampling.average
        )
        
        transform = src.transform * src.transform.scale(
            (src.width / data.shape[-1]),
            (src.height / data.shape[-2])
        )
        
        profile = src.profile
        profile.update({
            'height': new_height,
            'width': new_width,
            'transform': transform
        })
        
        with rasterio.open(output_file, 'w', **profile) as dst:
            dst.write(data)

if __name__ == "__main__":
    preprocess_mtpi(snakemake.input.mtpi, snakemake.output.mtpi_1km)
