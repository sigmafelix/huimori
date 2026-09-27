# Five-model prediction ensemble

## Plan and evaluation target

Predict PM10 and PM2.5 separately at unseen stations. Exclude Port rows. Keep
all years for a station together and stratify station splits by its latest type.
Reserve 20% of stations before fitting anything. Conflicting station-year types
are rejected; repeated observations of the same type are allowed.

On the remaining stations, use five outer folds to produce ensemble-training
predictions. For each outer fold, tune all five families on five inner folds,
refit the selected models on that outer analysis set, and predict its assessment
set. Fit nonnegative ridge stacking weights to these nested out-of-fold
predictions, selecting the ridge penalty with station-grouped CV. Finally retune
and fit the five families on all development stations and evaluate the stack
and every component once on the untouched holdout. Do not change settings based
on that holdout. No method can guarantee improvement over the best component.

All imputation, scaling, PCA, smoothing, bandwidth selection, and model selection
are trained inside the relevant analysis fold. The saved models retain that
preprocessing. Outcomes stay on the original concentration scale. Station IDs,
other pollutant outcomes, measurement counts, and join metadata are excluded
from predictors. Environmental predictors include terrain, roads, emissions,
land use, AOD, and BLH. Coordinates and year are also available to the models.

## Model search

- XGBoost and LightGBM: 80 joint configurations each, varying learning rate,
  tree count, depth, minimum node size, and row sampling; XGBoost also varies
  loss reduction. This replaces the earlier conditional learning-rate search.
- GAM: 24 configurations of PCA dimension, basis sizes, and smoothing penalty
  multiplier, using `mgcv::gam`, REML, shrinkage smooths, and a spatial smooth.
- Gaussian process: 48 configurations of PCA dimension, RBF/Matérn 3/2 kernels,
  environmental/spatial/temporal length scales, and noise. Exact Cholesky
  inference uses all analysis rows; no subsampling or inducing-point approximation.
- MGWR: 12 configurations of PCA dimension and ridge regularization. The local
  adapter backfits an intercept and predictor-specific coefficient surfaces.
  Each coefficient selects its own Gaussian distance bandwidth from 5–2000 km
  using leave-location-out partial-residual error within training data. Predictions
  at new locations use local regressions of stored training partial residuals at
  those selected bandwidths. This is a regularized MGWR implementation, not the
  `GWmodel` package implementation, and does not provide inferential standard errors.

Spatial learners compare 5/10/20 environmental PCs to control collinearity and
rank; trees retain the original environmental predictors. Coordinates use
EPSG:5179 in kilometers, obtained from `coords_google` (`latitude, longitude`).
The custom GAM, GP, and MGWR engines are registered with parsnip and used in
ordinary recipes/workflows and `tune::fit_resamples`. Candidate tables and engine
arguments are saved. Failed or nonconverged candidates cannot win; all five
families must have a successful candidate. Inspect tuning notes before a long run.

This is intentionally expensive: six searches per pollutant (five outer folds
plus the final development search), with 244 candidates × five inner folds per
search. Exact GP uses quadratic memory and cubic factorization time. MGWR also
uses quadratic distance/weight matrices. Edit `ensemble_candidates()` to expand
or adjust the search; do not interpret the smoke profile as an accuracy benchmark.

## Running and predicting

Install dependencies in your R library:

```r
install.packages(c("parsnip", "recipes", "workflows", "rsample", "tune",
  "yardstick", "xgboost", "bonsai", "lightgbm", "mgcv", "glmnet", "sf", "nanoparquet"))
```

```sh
Rscript tools/run_models/ensemble_models.R --smoke
Rscript tools/run_models/ensemble_models.R --full \
  /Users/isong/Documents/df_feat_correct_merged.parquet outputs/ensemble_models_full
```

The full command is explicit; sourcing the script never starts a run. Output
folders must be empty. The smoke test uses 120 synthetic observations, both
pollutants, two candidates per family, three outer folds and two inner folds.
It tests all five engines, missing-value preprocessing, nested station separation,
outcome-independent prediction, and save/reload prediction equality.

The smoke test was executed successfully on 2026-09-27, including a separate
fresh-R-session prediction check. One nonconverged MGWR candidate was rejected;
both pollutants retained a converged MGWR model. The real input passed a read-only
schema/split check with 96 environmental predictors. No full-data models were fit.
For this verification, missing `bonsai` and `lightgbm` packages were installed in
`/private/tmp/huimori-ensemble-r-library`, leaving the default R library unchanged:

```sh
R_LIBS_USER=/private/tmp/huimori-ensemble-r-library \
  Rscript tools/run_models/ensemble_models.R --smoke
```

For later runs, install the dependencies in your normal library using the R
command above, or keep using that temporary library while it exists.

For customization, source the script and call `run_ensemble_models()` with
`predictors`, `years`, `outer_v`, `inner_v`, `seed`, and `nthread` as needed.
Fits run sequentially by default to avoid fork/thread interactions and excessive
memory consumption. The script does not refit on the holdout after evaluation.

```r
source("tools/run_models/ensemble_models.R")
ensemble_dependencies()
ensemble_register()
model <- readRDS("outputs/ensemble_models_full/PM25/ensemble.rds")
prediction <- predict(model, new_data = new_station_covariates)
```

Each pollutant directory contains nested out-of-fold predictions, split audits,
tuning scores/notes, selected configurations, ensemble coefficients, component
and ensemble holdout predictions/metrics, and the reusable ensemble RDS.

Implementation references:
[parsnip custom engines](https://www.tidymodels.org/learn/develop/models/),
[MGWR backfitting and coefficient-specific bandwidths](https://gwmodel-lab.github.io/GWmodel3/reference/gwr_multiscale.html),
[LightGBM R interface](https://lightgbm.readthedocs.io/en/stable/R/index.html).
