# Temporally cascaded prediction ensemble

## Plan and evaluation target

Predict PM10 and PM2.5 separately, with strict forward-only year boundaries.
Exclude Port rows. Reserve 20% of stations, stratified by latest site type,
from every training stage. Final evaluation uses only these stations in the
latest `holdout_years` years (default: one). No observation from these future
years, even at other stations, enters development. Conflicting station-year
site types are rejected; repeated observations of the same type are allowed.

Inner and outer resampling use expanding windows: `max(training year) <
assessment year`, with no same-year observations in training. Inner folds need
at least two training years; outer folds need at least three so that inner
tuning is possible. `inner_v` and `outer_v` cap the number of most recent
eligible assessment years, rather than forcing unavailable folds. There must
be at least five development years plus the requested holdout years.

At each outer assessment year, tune each enabled family using only earlier
years, fit on that historical window, then predict the assessment year. Warm-up
rows receive no out-of-fold predictions and are excluded from stacking.
Nonnegative ridge stacking uses these historical predictions. Its penalty is
selected with another expanding-year validation loop, never random `cv.glmnet`
folds. Saved `cascade_prediction` values also choose their penalty using only
previously evaluated years; the first eligible meta-validation year uses a
fixed initial penalty of approximately one. The first OOF year has no earlier
OOF labels for fitting weights, so its cascade prediction is missing.

Finally, retune/refit on all development years and evaluate on the future
reserved-station holdout. The saved ensemble records `training_end_year` and
rejects prediction requests for that year or earlier. To model an earlier
period, rerun with an appropriate `years` range and a new output directory.
Do not use the final fitted blender to retroactively predict historical OOF rows.
Temporal CV permits a station's earlier years to train its later predictions;
the final reserved-station holdout additionally tests spatial transfer.
No method can guarantee improvement over the best component.

All imputation, scaling, PCA, smoothing, bandwidth selection, and model selection
are trained inside the relevant analysis fold. The saved models retain that
preprocessing. Prepared covariates must themselves be time-appropriate: the
script cannot remove future information already embedded in an upstream feature.
Outcomes stay on the original concentration scale. Station IDs,
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
- MGWR is disabled in the current candidate list, preserving the local setup.
  Its custom engine remains available if explicitly restored to that list.

Spatial learners compare 5/10/20 environmental PCs to control collinearity and
rank; trees retain the original environmental predictors. Coordinates use
EPSG:5179 in kilometers, obtained from `coords_google` (`latitude, longitude`).
The custom GAM, GP, and MGWR engines are registered with parsnip and used in
ordinary recipes/workflows and `tune::fit_resamples`. Candidate tables and engine
arguments are saved. Failed or nonconverged candidates cannot win; every enabled
family must have a successful candidate. Inspect tuning notes before a long run.

This is intentionally expensive: up to six searches per pollutant (five outer
assessment years plus the final development search), with 232 candidates and up
to five inner assessment years per search. Exact GP uses quadratic memory and cubic factorization time. MGWR also
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
  /Users/isong/Documents/df_feat_correct_merged.parquet outputs/ensemble_models_temporal
```

The full command is explicit; sourcing the script never starts a run. Output
folders must be empty. The smoke test uses 192 synthetic observations across
eight years, both pollutants, four candidates per enabled family, up to three
outer folds and two inner folds. It checks temporal boundaries at every level,
reserved-station exclusion, missing-value preprocessing, four distinct workers,
persistence, rejection of past-year prediction requests, and that perturbing
future outcomes does not change earlier cascade predictions. It exercises the
currently enabled XGBoost, LightGBM, GAM, and GP models.

For this verification, missing `bonsai` and `lightgbm` packages were installed in
`/private/tmp/huimori-ensemble-r-library`, leaving the default R library unchanged:

```sh
R_LIBS_USER=/private/tmp/huimori-ensemble-r-library \
  Rscript tools/run_models/ensemble_models.R --smoke
```

For later runs, install the dependencies in your normal library using the R
command above, or keep using that temporary library while it exists.

For customization, source the script and call `run_ensemble_models()` with
`predictors`, `years`, `holdout_years`, `outer_v`, `inner_v`, `seed`, `workers`,
and `nthread` as needed.
Hyperparameter candidates run in parallel on a persistent PSOCK cluster with
`workers = 4L` by default; values below four are rejected. For example, pass
`workers = 8L` to use eight worker processes. Each candidate evaluates its folds
sequentially, avoiding nested worker pools. Tree fits default to `nthread = 1L`,
and worker BLAS thread limits are set before launching R processes. GAM
explicitly uses `gam.control(nthreads = 1, ncv.threads = 1)`. Fresh R processes avoid
fork/OpenMP interactions. Custom parsnip engines and the calling R library paths
are initialized on every worker. The cluster is stopped on completion or error.
Tuning tables record worker process IDs, wall time, and CPU time; `workers.log`
captures worker messages. Selected component refits also run concurrently.
A CPU display of 100% for a single worker can mean one fully occupied core;
inspect all worker PIDs rather than only the parent R process. Four workers
parallelize separate GAM candidates, not four threads within each GAM. A last
slow candidate can leave fewer than four workers busy near the end of a search.
Each spatial-model worker holds its own matrices, so budget memory for the
chosen worker count. The script does not refit on the holdout after evaluation.

```r
source("tools/run_models/ensemble_models.R")
ensemble_dependencies()
ensemble_register()
model <- readRDS("outputs/ensemble_models_temporal/PM25/ensemble.rds")
# Every new_data$year must exceed model$training_end_year.
prediction <- predict(model, new_data = new_station_covariates)
```

Each pollutant directory contains nested out-of-fold predictions, split audits,
tuning scores/notes, selected configurations, ensemble coefficients, component
and ensemble holdout predictions/metrics, and the reusable ensemble RDS.
`outer_temporal_splits.csv`, each search's `temporal_splits.csv`,
`blender_temporal_splits.csv`, and `holdout_temporal_split.csv` document the cutoffs.
`nested_oof_predictions.csv` contains forward-only component and cascade predictions.

Implementation references:
[parsnip custom engines](https://www.tidymodels.org/learn/develop/models/),
[MGWR backfitting and coefficient-specific bandwidths](https://gwmodel-lab.github.io/GWmodel3/reference/gwr_multiscale.html),
[LightGBM R interface](https://lightgbm.readthedocs.io/en/stable/R/index.html),
[mgcv parallel computation](https://stat.ethz.ch/R-manual/R-devel/library/mgcv/html/mgcv-parallel.html).
