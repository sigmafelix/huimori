library(terra)
library(rgee)

rgee::ee_Initialize()

range <- c(124.5, 131.9, 32.8, 38.9)
years <- seq(2009, 2022)


## rgee initialization
rgee::ee_install_set_pyenv(
  py_path = file.path(
    Sys.getenv("HOME"),
    ".local/share/uv/python",
    "cpython-3.13.2+freethreaded-linux-x86_64-gnu",
    "bin",
    "python3.13t"
  ),
  py_env = "rgee-py"
)

rgee::ee_install()
rgee::ee_Initialize()
rgee::ee_install_upgrade()

# var dataset = ee.ImageCollection('LANDSAT/COMPOSITES/C02/T1_L2_8DAY_EVI')
#                   .filterDate('2023-01-01', '2023-12-31');
# var colorized = dataset.select('EVI');
