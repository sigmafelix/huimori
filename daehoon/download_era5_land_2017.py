import cdsapi
import os

client = cdsapi.Client()

out_file = "ERA5_Land_2017_12.nc"

# 이미 파일이 있는지 확인
if os.path.exists(out_file):
    print(f"{out_file}이(가) 이미 존재합니다. 다운로드를 건너뜁니다.")
else:
    request = {
        "dataset": "reanalysis-era5-land",
        "variable": [
            "2m_temperature",
            "surface_net_solar_radiation",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "surface_pressure",
            "total_precipitation"
        ],
        "year": "2017",
        "month": "12",
        "day": [f"{d:02d}" for d in range(1, 32)], # 1일부터 31일까지
        "time": [f"{h:02d}:00" for h in range(24)], # 00시부터 23시까지
        "area": [39, 124, 33, 132], # 한반도 영역
        "format": "netcdf"
    }

    print(f"Downloading {out_file} for KST alignment...")
    try:
        client.retrieve("reanalysis-era5-land", request, out_file)
        print("다운로드 완료.")
    except Exception as e:
        print(f"에러 발생: {e}")