# huimori 코드베이스 컨텍스트

## 분석 범위

이 문서는 현재 작업 디렉터리의 코드베이스를 정적 분석한 결과이다. `daehoon/` 이하 디렉터리는 파일 목록 조회, 검색, 내용 확인에서 모두 제외했다. 단, 활성 코드에 남아 있는 외부/임시 출력 경로 성격의 문자열은 코드 동작을 이해하는 데 필요한 범위에서만 간접적으로 해석했다.

## 프로젝트 성격

`huimori`는 한국 전역의 대기오염, 특히 `PM10`과 `PM25` 농도 예측을 위한 R 패키지 겸 `targets` 기반 재현 파이프라인이다. 패키지 메타데이터는 `DESCRIPTION`, export 목록은 `NAMESPACE`, 핵심 함수는 `R/` 아래에 있고, 실제 대규모 분석 DAG는 `_targets.R`와 `inst/targets/*.R`에 집중되어 있다.

주요 기술 스택은 다음과 같다.

- 파이프라인: `targets`, `tarchetypes`, `crew`
- 공간/래스터: `sf`, `terra`, `raster`, `chopin`, `exactextractr`
- 데이터 처리: `dplyr`, `data.table`, `collapse`, `nanoparquet`, `readxl`, `arrow`
- 모델링: `tidymodels`, `workflows`, `recipes`, `tune`, `finetune`, `xgboost`, `bonsai`, `lightgbm`
- 원격/기후 자료: `rgee`, ERA5, AOD, CHELSA
- 출력: GeoTIFF/COG, PNG, CSV metadata

## 상위 구조

- `_targets.R`: 전체 `targets` DAG 진입점. 패키지 로드, `crew` controller 설정, `tar_option_set()`, `R/`와 `inst/targets` 소스 로드, 최종 target list 반환을 담당한다.
- `inst/targets/`: 파이프라인 target 정의의 본체. 환경 설정, 입력 파일 경로, 관측소 처리, feature 추출, 모델 튜닝/예측, 지도 출력 단계가 파일별로 나뉜다.
- `R/helpers.R`: 날짜 보정, 일별/연별 관측값 집계, 예측 grid 생성, GEE 기반 wind/building density 추출 등 보조 함수.
- `R/processing.R`: landuse frequency rasterization, binary mask, terrain filter, geographically weighted emission 계산 등 공간 처리 함수.
- `R/models.R`: TMB 계열 함수와 `fit_tidy_xgb()`, `fit_tidy_lgb()`, tuning 결과 후처리 함수.
- `R/xgb_cumulative.R`: 연도 누적 학습 데이터 구성, 공간 k-means CV, XGBoost fold diagnostics, 연도별 모델과 grid branch 매칭 예측.
- `R/export_prediction_maps.R`: 예측 chunk를 연간 raster로 mosaic하고 COG/PNG/metadata를 쓰며 결과를 검증하는 함수군.
- `tools/`, `dev/workflow/`, `rust_tools/`: 보조 스크립트와 대체/실험 워크플로. 현재 `_targets.R`의 활성 DAG에는 직접 포함되지 않는다.
- `outputs/prediction_maps/`: 이미 생성된 예측 지도 산출물 일부가 있다.

## `_targets.R` 실행 구조

`_targets.R`는 다음 순서로 동작한다.

1. `targets`, `tarchetypes`, `terra`, `chopin`, `crew`, `sf`를 로드하고 `sf::sf_use_s2(FALSE)`를 설정한다.
2. local `crew` controller를 worker 수별로 만든다: `controller_01`, `controller_04`, `controller_08`, `controller_10`, `controller_15`, `controller_20`.
3. `tar_option_set()`에서 기본 패키지 목록, `format = "qs"`, group controller, `error = "continue"`, worker storage/retrieval/deployment, transient memory 정책을 설정한다.
4. `tar_source("R")`로 패키지 내부 함수를 모두 로드한다.
5. `inst/targets`의 활성 파일을 로드한다.
6. target list를 반환한다.

활성 로드 파일은 다음 6개이다.

- `inst/targets/1_init_targets.R`
- `inst/targets/2_pin_files.R`
- `inst/targets/3_process_feature.R`
- `inst/targets/3_1_process_feature_daily.R`
- `inst/targets/4_tune_models.R`
- `inst/targets/6_export_prediction_maps.R`

`inst/targets/5_fit_tmb.R`는 현재 `_targets.R`에서 로드되지 않는다. 파일 내부에도 비어 있는 인자와 오래된 target 이름이 남아 있어 활성 실행 흐름의 일부로 보기는 어렵다.

최종 DAG에 포함되는 top-level list는 다음 순서이다.

```r
list_configs
list_basefiles
list_process_site
list_process_split
list_process_feature
list_process_site_daily
list_process_feature_daily
list_fit_models
list_tune_models
list_tune_eval
list_export_prediction_maps
```

`list_pred_process`는 `4_tune_models.R`에 정의되어 있지만 `_targets.R` 반환 list에 포함되지 않아 현재 비활성이다.

## `inst/targets` 파일별 역할

### `1_init_targets.R`

환경과 시간 범위를 정의한다.

- `chr_dir_data`: 사용자명에 따라 대용량 데이터 root를 선택한다.
- `chr_dir_git`: 사용자명에 따라 별도 git/data root를 선택한다.
- `chr_date_range`: 현재 예측 범위는 `2015-01-01`부터 `2023-12-31`.
- `int_years_spatial`: `2015:2023` 연간 branch 축.
- `chr_months_spatial`: `chr_date_range`에서 월 단위 `YYYY-MM` branch 축을 만든다.

이 파일의 target들은 이후 모든 파일 경로, 연도별 feature, 월별 daily feature, 모델링 branch의 공통 인덱스 역할을 한다.

### `2_pin_files.R`

원천 데이터와 기본 공간 객체 경로를 target으로 고정한다.

주요 입력은 다음 범주이다.

- 관측소 이력 xlsx: `chr_monitors_file`
- AirKorea 측정 parquet: `chr_measurement_file`
- landuse raster 목록: `chr_landuse_files`
- DEM/DSM raster: `chr_dem_file`, `chr_dsm_file`
- 도로망 shapefile 목록: `chr_road_files`
- ASOS parquet/site xlsx: `chr_asos_file`, `chr_asos_site_file`
- 한국 경계: `sf_korea_all`, GADM level 0을 EPSG:5179로 변환
- 유역, MTPI, 배출원, AOD, ERA5, CHELSA 경로

특이 사항:

- `chr_landuse_files`는 예측 연도의 전년도 landuse raster를 찾는 방식이다.
- `chr_landuse_freq_file`은 사전 계산된 landuse frequency raster 경로를 만들며 `cue = tar_cue(mode = "never")`로 사실상 재계산을 억제한다.
- `chr_mtpi_1km_file`은 90m MTPI를 1km 수준으로 aggregate한 raster를 실제로 생성/저장한다.

### `3_process_feature.R`

가장 큰 파일이며, 연간 관측소 처리, 연간 feature 생성, 예측 grid 분할, grid feature 생성까지 담당한다. 파일 앞부분에는 target list가 있고, 중간 이후에는 feature 추출용 helper 함수들이 함께 정의된다.

#### `list_process_site`

관측 자료와 관측소 이력 좌표를 정리한다.

주요 흐름:

1. `dt_measurements`
   - parquet 측정자료를 `data.table`로 읽는다.
   - `datehour`와 `date`에 9시간을 더해 KST 기준으로 보정한다.
   - `SO2`, `CO`, `O3`, `NO2`, `PM10`, `PM25`의 음수값을 `NA`로 바꾼다.

2. `sf_monitors_base`
   - 관측소 이력 xlsx를 읽고 광화학/중금속/산성/유해 측정망을 제외한다.
   - `TMSID` suffix, 중복 이력, 시작/종료일 결측을 정리한다.
   - `coords_google`에서 lon/lat을 파싱하고, Korean site type label을 영문 factor로 매핑한다.
   - 동일 `TMSID`의 위치 변경 이력에 대해 `TMSID2`를 `A`, `B`, ... suffix로 생성한다.

3. `sf_monitors_correct`
   - `huimori::summarize_annual()`로 연간 PM 관측값을 만든다.
   - 관측소 위치 변경 이력을 연도 범위로 확장한다.
   - lon/lat을 EPSG:5179 `sf` point로 변환하고 연간 관측값을 결합한다.

4. `sf_monitors_correct_yr`
   - `sf_monitors_correct`를 `int_years_spatial` 기준으로 연도별 branch한다.

5. `sf_monitors_correct_full`
   - 관측소 운영기간을 일 단위 grid로 확장한다.

6. `sf_monitors_incorrect`
   - 원본/과거 좌표를 사용하는 비교용 관측소 객체를 만든다.

7. `dt_asos`, `df_feat_incorrect_wind_daily`, `df_feat_incorrect_wind_annual`, `df_feat_incorrect_building_density`, `ras_landuse_freq`
   - ASOS 자료, GEE wind/building density, landuse frequency 등 보조 feature를 만든다.
   - GEE Python 모듈이 없으면 wind는 빈 data frame, building density는 `NA` feature를 반환하도록 방어한다.

#### `list_process_split`

예측 grid를 만든다.

- `int_grid_size`: 현재 30m.
- `int_size_split`: 현재 70.
- `sf_grid_size`: 한국 경계를 단순화한 뒤 `auto_grid()`로 grid/chunk shell을 만든다.
- `sf_grid_size_group`: `CGRIDID` 기준 group branch.
- `list_pred_calc_grid`: 각 group bbox를 30m raster template로 만들고 `gid`, `x`, `y`, `layer`를 가진 EPSG:5179 point grid로 변환한다. 이후 grid feature와 예측의 핵심 branch 단위이다.
- `sf_grid_correct_split`, `int_split_grid_ids`: chopin 기반 split grid. 현재 구버전 `list_pred_calc_grid_old`는 주석 처리되어 있다.

#### feature helper 함수군

`3_process_feature.R` 중간에는 target command에서 쓰는 비export helper가 다수 정의된다.

- 도로: `select_road_file_for_year()`, `standardize_road_columns()`, `load_filtered_road_for_year()`, `extract_nearest_road_distance()`
- landuse: `landuse_fixed_classes`, `landuse_fixed_terms()`, `select_landuse_file_for_feature_year()`, `extract_fixed_landuse_fractions()`
- buffer: `make_feature_buffer_set()`, `get_feature_buffer()`, `buffer_set_meta()`, CRS/radius 검증 함수
- yearly raster buffer mean: `yearly_buffer_mean_terms()`, `extract_yearly_buffer_mean()`
- grid static feature alignment: `extract_grid_static_raster_feature()`, `grid_landuse_chunk_key()`
- merge validation: `merged_feature_predictor_terms()`, `assert_required_columns()`, `assert_unique_key()`, `assert_single_year()`

핵심 설계는 row order와 key uniqueness를 강하게 검증하는 것이다. `TMSID/TMSID2/year` 또는 `gid/year`가 중복되거나, feature merge 후 row 수가 바뀌면 즉시 실패한다.

#### `list_process_feature`

연간 관측소 feature와 예측 grid feature를 만든다.

관측소 correct 좌표 feature:

- `df_feat_correct_d_road`: 연도별 도로망에서 고속국도/도시고속화도로 등 일부 유형을 제외하고 가장 가까운 도로 거리 계산.
- `df_feat_correct_dsm`, `df_feat_correct_dem`: DSM/DEM point extraction.
- `int_landuse_radius`: 현재 `100`, `500`, `2000`, `5000` m.
- `sf_buffer_correct_yr`: 연도별 관측소 buffer set 생성.
- `df_feat_correct_landuse`: 전년도 landuse raster에서 고정 class schema의 fraction feature 추출.
- `df_feat_correct_mtpi`, `df_feat_correct_mtpi_1km`: MTPI point extraction.
- `sf_emission_locs`, `sf_korea_watershed`, `sf_feat_nemittors`, `df_feat_correct_emittors`: 배출원 관련 산출물을 만들지만, 현재 최종 correct merge에는 포함되지 않는다.
- `chr_aod_date_seq`, `chr_aod_date_chunks`: AOD 처리용 날짜 chunk. 과거 daily AOD extraction target은 주석 처리되어 있다.
- `int_aod_year_chunks`, `rast_year_aod`: 연도별 AOD median raster 생성.
- `df_feat_correct_aod_yearly`: AOD annual raster에서 radius별 mean 추출.
- `df_feat_correct_chelsa`: CHELSA 연간 median 변수 추출. 현재 최종 merge에는 포함되지 않는다.
- `rast_era5_blh`, `df_feat_correct_blh_yearly`: ERA5 BLH annual median raster와 radius별 mean 추출.
- `df_feat_correct_merged`: correct 좌표의 최종 연간 학습 feature table. key는 `TMSID`, `TMSID2`, `year`. 예측변수 schema는 `dsm`, `dem`, `d_road`, `mtpi`, `mtpi_1km`, landuse fraction 25 classes x 4 radii, `aod_yearly_*`, `blh_yearly_*`로 구성된다. 총 predictor 개수는 후속 `chr_terms_x`에서 113개로 검증된다.
- `df_feat_correct_merged_old`: 현재는 `df_feat_correct_merged` alias이다.

incorrect 좌표 비교 branch:

- `df_feat_incorrect_d_road`, `df_feat_incorrect_dsm`, `df_feat_incorrect_dem`, `df_feat_incorrect_landuse`, `df_feat_incorrect_mtpi`, `df_feat_incorrect_mtpi_1km`, `df_feat_incorrect_emittors`, `df_feat_incorrect_merged`
- correct branch 대비 검증과 schema 고정이 약하며, 후속 모델 target 일부에서 비교용으로 남아 있다.

grid feature:

- `df_feat_grid_d_road`: `list_pred_calc_grid` chunk와 연도 cross product로 도로 거리 계산.
- `df_feat_grid_dsm`, `df_feat_grid_dem`, `df_feat_grid_mtpi`, `df_feat_grid_mtpi_1km`: grid chunk별 static raster feature.
- `sf_buffer_grid`: grid chunk별 radius buffer set.
- `df_feat_grid_landuse`: grid x year 단위 전년도 landuse fraction.
- `df_feat_grid_static_topo_yearly`: static topo feature를 year branch에 맞게 복제/정렬.
- `df_feat_grid_aod_yearly`, `df_feat_grid_blh_yearly`: grid x year 단위 annual raster buffer mean.
- `df_feat_grid_emittors`: 환경변수로 명시적으로 켜지지 않으면 계산하지 않고 `NA`를 반환한다. 현재 최종 grid merge에는 포함되지 않는다.
- `df_feat_grid_merged`: 예측용 최종 grid feature table. key는 `gid`, `year`이고 metadata로 `gid`, `x`, `y`, `layer`, `year`를 유지한다. 학습 predictor schema와 동일한 feature set을 요구한다.

### `3_1_process_feature_daily.R`

일별/monthly branch feature를 정의한다. 현재 `_targets.R`에 포함되어 활성 DAG에 들어가지만, `4_tune_models.R` 이후 모델링/지도 출력 흐름은 연간 `df_feat_correct_merged`, `df_feat_grid_merged`를 사용하므로 daily merged table은 아직 최종 예측에 연결되지 않는다.

#### `list_process_site_daily`

- `sf_monitors_correct_daily`: hourly 측정자료를 `huimori::summarize_daily()`로 일별 집계하고, 관측소 이력의 운영기간을 일 단위로 확장한 뒤 EPSG:5179 `sf` point와 결합한다. `PM10`, `PM25`, `date`, `year`를 포함한다.
- `sf_monitors_correct_month`: `chr_months_spatial` 기준으로 `sf_monitors_correct_daily`를 월별 branch한다.

#### `list_process_feature_daily`

- `df_feat_correct_aod_daily`: 월별 branch 안에서 존재하는 AOD daily composite raster만 읽고, 해당 날짜 운영 중인 관측소에 대해 100/500/1000/2000/5000m buffer mean을 추출한다.
- `df_feat_correct_era5_land_daily`: 월별 ERA5-Land netCDF zip에서 `data_0.nc`를 임시 추출하고, `t2m`, `u10`, `v10`, `sp`는 일평균, `ssr`, `tp`는 일합산으로 집계한 뒤 buffer mean을 추출한다. `t2m`은 Kelvin에서 Celsius로 변환한다.
- `df_feat_correct_era5_blh_daily`: 월별 BLH netCDF를 일평균 raster로 집계하고 buffer mean을 추출한다.
- `df_feat_correct_merged_daily`: 월별 관측소 daily base에 daily AOD/ERA5/BLH와 연간 static feature 일부를 join한 비공간 data frame을 반환한다.

일별 branch는 `pattern = map(chr_months_spatial, sf_monitors_correct_month, ...)` 구조로 월 단위 병렬화되어 있고, 대체로 `controller_15`를 사용한다.

### `4_tune_models.R`

모델 predictor 정의, XGBoost tuning, grid prediction, tuning 평가를 담당한다.

#### `list_fit_models`

- `chr_terms_x`: 최종 predictor 목록을 구성하고 113개인지 검증한다. 구성은 static 지형/도로 5개, landuse 25 classes x 4 radii, AOD yearly 4개, BLH yearly 4개이다.
- `chr_outcome`: `PM10`, `PM25`.
- `form_fit`: pollutant별 formula를 만든다.

#### `list_tune_models`

활성 핵심 흐름:

1. `workflow_tune_xgb_correct_spatial`
   - `prepare_xgb_correct_training_data()`로 target year 이하의 누적 학습 데이터를 만든다. 즉 2018년 모델은 2015-2018 학습자료를 사용한다.
   - `make_xgb_spatial_resamples()`로 `TMSID` 단위 5-fold k-means spatial CV를 만든다.
   - CV diagnostics CSV/PNG를 쓴다.
   - `fit_tidy_xgb()`로 XGBoost hyperparameter tuning을 수행한다.
   - 결과에 `target_year`, `outcome`, diagnostics 경로 attribute를 붙인다.
   - branch pattern은 `cross(int_years_spatial, form_fit)`이므로 9년 x 2 pollutants = 18개 tuning branch이다.

2. `workflow_final_xgb_correct`
   - tuning 결과에서 `rmse` 기준 best workflow를 fit한다.
   - 결과 workflow에 `outcome`, `target_year` attribute를 유지한다.

3. `workflow_fit_xgb_correct`
   - `df_feat_grid_merged` branch의 year와 같은 `workflow_final_xgb_correct` 모델을 찾아 PM10/PM25를 모두 예측한다.
   - `predict_grid_with_matching_year_models()`가 metadata columns와 pollutant prediction columns를 반환한다.
   - pattern은 `map(df_feat_grid_merged)`이므로 grid-year branch 단위 예측이다.

비교/초안 흐름:

- `workflow_tune_mamba_correct_spatial`, `workflow_tune_mamba_incorrect_spatial`은 현재 학습 실행 없이 전처리된 `data_sub`만 반환한다.
- `workflow_tune_xgb_incorrect_spatial`, `workflow_fit_incorrect`는 incorrect 좌표 비교용 XGBoost 흐름이다.

#### `list_tune_eval`

- `df_tune_correct_metrics`: tuning metrics에 `target_year`, `outcome`, fallback 여부를 붙인다.
- `df_tune_correct_vip`: best model에서 SHAP/permutation variable importance를 계산한다.

### `5_fit_tmb.R`

현재 활성 DAG에 포함되지 않는다. 또한 `fit_all_tmb()` 호출부에 비어 있는 인자와 현재 `4_tune_models.R`와 맞지 않는 target 이름이 있어 실행 가능한 상태가 아니다. TMB/INLA 계열 함수는 `R/models.R`와 문서에 남아 있지만, 현재 운영 흐름은 XGBoost 중심이다.

### `6_export_prediction_maps.R`

grid prediction을 연간 지도 산출물로 변환한다.

- `chr_dir_prediction_maps`: 기본 출력 root는 `outputs/prediction_maps`.
- `df_prediction_map_index`: `int_years_spatial` x `PM10/PM25` 조합을 group branch로 만든다.
- `workflow_fit_xgb_correct_annual_raster`: `write_annual_prediction_outputs()`를 호출한다.
  - `workflow_fit_xgb_correct`의 chunk prediction 중 해당 year/pollutant를 모은다.
  - `expected_chunks = 593L`을 기대한다.
  - chunk raster들을 mosaic한다.
  - 한국 경계로 mask한다.
  - GeoTIFF를 쓰고 GDAL COG 변환을 시도한다.
  - PNG preview와 CSV metadata를 함께 쓴다.
- `workflow_fit_xgb_correct_map_validation`: raster/png/metadata 개수, chunk 수, 파일 존재, COG metadata, non-NA cell count와 예측 row count 일치 여부를 검증한다.

## 핵심 실행 흐름

활성 연간 예측 흐름은 다음과 같다.

```text
1_init_targets
  -> chr_date_range, int_years_spatial, chr_months_spatial

2_pin_files
  -> raw file paths, Korea boundary, raster/climate directories

3_process_feature / list_process_site
  -> dt_measurements
  -> sf_monitors_base
  -> sf_monitors_correct
  -> sf_monitors_correct_yr

3_process_feature / list_process_split
  -> sf_grid_size
  -> sf_grid_size_group
  -> list_pred_calc_grid

3_process_feature / list_process_feature
  -> monitor features by year
  -> df_feat_correct_merged
  -> grid features by chunk-year
  -> df_feat_grid_merged

4_tune_models
  -> chr_terms_x, form_fit
  -> workflow_tune_xgb_correct_spatial
  -> workflow_final_xgb_correct
  -> workflow_fit_xgb_correct

6_export_prediction_maps
  -> df_prediction_map_index
  -> workflow_fit_xgb_correct_annual_raster
  -> workflow_fit_xgb_correct_map_validation
```

연간 학습 데이터는 `target_year` 이하 연도를 누적해서 쓰고, 예측은 같은 연도의 fitted workflow와 grid feature branch를 매칭한다. 예를 들어 2020년 grid branch는 `target_year == 2020`인 PM10/PM25 모델을 선택한다.

## 주요 데이터 스키마

### 관측소 연간 학습 table

`df_feat_correct_merged`는 연도별 branch이며 최종적으로 data frame이다.

주요 key:

- `TMSID`
- `TMSID2`
- `year`

주요 outcome:

- `PM10`
- `PM25`

주요 predictors:

- `d_road`
- `dem`
- `dsm`
- `mtpi`
- `mtpi_1km`
- `landuse_frac_<class>_<radius>` for 25 fixed classes x 4 radii
- `aod_yearly_<radius>`
- `blh_yearly_<radius>`

### 예측 grid table

`df_feat_grid_merged`는 grid chunk x year branch이며 최종적으로 data frame이다.

주요 metadata:

- `gid`
- `x`
- `y`
- `layer`
- `year`

predictor schema는 `df_feat_correct_merged`의 `chr_terms_x`와 일치해야 한다. `predict_grid_with_matching_year_models()`는 이 schema를 강제하고, PM10/PM25 prediction columns를 붙인다.

## 병렬화와 branch 전략

- 연도 branch: `int_years_spatial` = 2015-2023.
- 월 branch: `chr_months_spatial` = 2015-01부터 2023-12까지.
- grid branch: `list_pred_calc_grid`가 `sf_grid_size_group` 기반 chunk로 나뉜다.
- grid-year cross branch: 도로, landuse, AOD, BLH, static topo yearly merge 등에서 grid chunk와 year를 cross한다.
- 모델 branch: year x pollutant = 18개 XGBoost tuning/final model.
- 지도 branch: year x pollutant = 18개 annual raster/PNG/metadata.

무거운 공간 추출은 대체로 `controller_20`, daily climate/AOD는 `controller_15`, XGBoost final/prediction은 `controller_01` 또는 `controller_04`에 배정되어 있다.

## 중요한 구현 판단과 제약

- CRS는 대부분 EPSG:5179를 기준으로 한다. buffer 생성 함수들은 longitude/latitude CRS에서 meter buffer를 만드는 것을 명시적으로 거부한다.
- landuse feature는 고정 class schema를 사용한다. 특정 class가 추출되지 않으면 0 컬럼을 채워 predictor 수를 안정화한다.
- feature merge는 key 중복과 row count 변화를 강하게 검사한다. 이 검증이 깨지면 target이 실패하는 것이 정상이다.
- correct final feature에서는 emission 계열 컬럼이 들어오면 실패하도록 되어 있다. 배출원 feature target은 남아 있지만 현재 최종 predictor에는 포함되지 않는다.
- XGBoost spatial CV는 현재 k-means 5-fold만 지원한다. fold label은 북쪽에서 남쪽, 같은 경우 서쪽에서 동쪽 순으로 안정화한다.
- `fit_tidy_xgb()`는 racing tuning 실패 시 일반 `tune_grid()`로 fallback하고, fallback error를 tuning 결과 attribute에 보관한다.
- 지도 출력은 `expected_chunks = 593L`에 의존한다. grid chunk 수가 바뀌면 export validation도 함께 바꿔야 한다.
- GDAL의 COG driver가 요청 compression을 지원하지 않으면 COG 생성 단계가 실패하거나 경고가 날 수 있다. validation은 metadata상 `cog_created = TRUE`를 기대한다.
- daily feature 흐름은 활성 target list에 포함되어 계산 대상이지만, 현재 모델 학습/예측/지도 출력에는 연결되어 있지 않다.

## 실행 관점에서 보는 현재 상태

현재 운영 가능한 주 흐름은 다음으로 보는 것이 맞다.

- 연간 correct 좌표 feature 생성
- 연간 누적 XGBoost 학습
- grid-year feature 생성
- grid-year PM10/PM25 예측
- 연간 PM10/PM25 raster, PNG, metadata 출력과 검증

실험적이거나 비활성으로 보는 부분:

- `5_fit_tmb.R` 전체
- `list_pred_process`
- Mamba target들
- daily merged feature의 downstream 모델링
- correct final predictor에서 제외된 emission/CHELSA target

## 코드 변경 시 우선 확인 지점

`inst/targets`를 수정할 때는 다음 순서로 영향 범위를 확인하는 것이 좋다.

1. `_targets.R`에 실제 포함되는 list인지 확인한다.
2. target 이름이 후속 target에서 참조되는지 `rg "<target_name>" -g '!daehoon/**'`로 확인한다.
3. branch 축이 `map()`인지 `cross()`인지 확인한다. 특히 grid x year feature는 row 정렬과 key uniqueness가 중요하다.
4. predictor를 추가/삭제하면 `merged_feature_predictor_terms()`, `chr_terms_x`, `df_feat_correct_merged`, `df_feat_grid_merged`를 함께 맞춘다.
5. grid chunk 수나 grid 생성 방식을 바꾸면 `expected_chunks = 593L`와 export validation을 함께 조정한다.
6. daily feature를 실제 모델에 연결하려면 `df_feat_correct_merged_daily`와 대응되는 daily/grid prediction feature schema, outcome formula, export 단위를 새로 정의해야 한다.
