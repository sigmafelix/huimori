import cdsapi
import calendar
import os

client = cdsapi.Client()

dataset = "reanalysis-era5-land"

variables = [
    "2m_temperature",
    "surface_net_solar_radiation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_pressure",
    "total_precipitation"
]

# 2018년부터 2024년까지 설정
years = range(2009, 2017)
times = [f"{h:02d}:00" for h in range(24)]

# 공간 범위: [North, West, South, East] (한반도 인근)
area = [39, 124, 33, 132]

for year in years:
    for month in range(1, 13):
        _, n_days = calendar.monthrange(year, month)
        days = [f"{d:02d}" for d in range(1, n_days + 1)]

        out_file = f"ERA5_Land_{year}_{month:02d}.nc"

        if os.path.exists(out_file):
            print(f"{out_file} already exists, skipping.")
            continue

        request = {
            "variable": variables,
            "year": str(year),
            "month": f"{month:02d}",
            "day": days,
            "time": times,
            "area": area,
            "format": "netcdf" 
        }

        print(f"Downloading {out_file} ...")
        try:
            client.retrieve(dataset, request, out_file)
        except Exception as e:
            print(f"Error downloading {out_file}: {e}")