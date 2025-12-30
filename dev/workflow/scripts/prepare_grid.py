import geopandas as gpd

def prepare_grid(input_path, output_path):
    gdf = gpd.read_file(input_path)
    gdf.to_parquet(output_path)

if __name__ == "__main__":
    prepare_grid(snakemake.input.grid, snakemake.output.grid_parquet)
