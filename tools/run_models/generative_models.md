# Bayesian-network, VAE, and GAN prediction

`generative_models.R` is a standalone R implementation without tidymodels,
parsnip, recipes, workflows, tune, or rsample. It models PM10 and PM2.5
separately, excludes Port rows, and retains the temporal evaluation protocol
from the ensemble experiment. It does not modify the earlier scripts.

## Models

- **Gaussian Bayesian network (`bnlearn`)**: hill-climbing DAG structure learning
  with AIC/BIC Gaussian scores, followed by Gaussian maximum-likelihood parameter
  fitting. The concentration node is constrained to be a sink. Given all
  covariates, its parents supply the conditional mean exactly, without noisy
  likelihood-weighting simulation. This is a graphical Bayesian network, not a
  Bayesian neural network; fitted coefficients are not posterior-averaged.
  The graph is predictive and should not be interpreted as discovered causality.
- **Conditional VAE (`torch`)**: the training encoder estimates `q(z | x, y)`;
  the decoder predicts concentration from `(x, z)`. Its objective combines
  squared reconstruction error and a tunable KL penalty relative to a standard
  normal latent prior. At prediction time, only `x` and samples from that prior
  are used. The encoder never sees a validation/test outcome.
- **Conditional GAN (`torch`)**: the discriminator distinguishes observed
  `(x, y)` pairs from generated `(x, G(x, z))` pairs. The generator minimizes
  a non-saturating adversarial loss plus a tunable supervised MSE term. This
  hybrid loss deliberately prioritizes concentration prediction. Reported
  predictions average latent draws; they are not claimed to be calibrated
  uncertainty intervals.

The neural generators each use two hidden ReLU layers. Full runs average three
independent training seeds and 64 latent draws per fitted component. Shared
latent draws across prediction rows give reproducible predictions independent
of batching/order, up to floating-point precision. Models run on CPU, with one
Torch thread per worker. Ordinary R arrays store neural state dictionaries so
saved models can be reloaded without serializing external tensor pointers.

## Tuning and temporal safeguards

1. Reserve 20% of stations, stratified by their latest site type, before fitting.
   The final test contains those stations in the latest `holdout_years` years
   (default one). All observations in those future years, including observations
   at other stations, are excluded from development.
2. Generate outer expanding-year folds. Every training year is strictly earlier
   than its assessment year. Outer folds need at least three historical years;
   inner tuning folds need at least two. Fold settings cap the number of eligible
   recent assessment years. At least five development years are required.
3. Tune each family independently on the same inner temporal folds, selecting
   pooled validation RMSE. Each candidate must complete every fold. Failed
   candidates receive infinite RMSE and their warnings/errors are recorded.
4. Refit selected candidates on the outer analysis window and predict the next
   assessment year. Learn nonnegative ridge ensemble weights from these nested
   historical predictions. Penalty selection also uses expanding-year windows.
   The earliest OOF year is warm-up for the blender; its ensemble prediction is
   missing. The first scored ensemble year uses a fixed initial ridge penalty
   near one. Later years choose the penalty only from earlier scored years.
5. Retune/refit on all development years, fit the final blender, and evaluate
   components and ensemble once on the future reserved-station holdout. The
   saved predictor refuses requests for years at or before its training cutoff.

Inner/outer temporal validation can use a station's earlier observations to
predict its later values. The final holdout also tests transfer to unseen
stations. Do not compare historical cascade rows using the final blender:
the saved cascade predictions use historical weights, whereas the final
blender knows all development outcomes.

Preprocessing is manual base R: training-window medians, removal of columns
that are constant or entirely missing in that window, standardization, and
optional PCA. A feature observed only in later years is omitted until historical
values are available, never imputed using the future. Target centering/scaling also uses
only training data. Selected transforms are saved with each model. Default
predictors are terrain, road distance, emissions, land use, AOD, BLH, year, and
projected coordinates (EPSG:5179, kilometers). Outcome-derived fields and station
IDs are excluded. Prepared covariates must themselves be time-appropriate; no
split strategy removes leakage already embedded in upstream features.

The full candidate grids contain 24 BN, 48 VAE, and 48 GAN configurations:

- BN: preprocessing dimension, Gaussian score, maximum parent count, and
  structure-search restarts.
- VAE/GAN: preprocessing dimension, hidden/latent sizes, learning rate, weight
  decay, batch size, and training epochs; VAE also tunes KL weight, and GAN
  tunes the supervised loss weight.

Learning rate is tuned jointly with the other settings. Epoch count is a tuned
hyperparameter; there is no early stopping against an outer fold or holdout.
Every neural tuning fit uses three seeds in full mode. This is computationally
expensive; no full run is started implicitly.

Four PSOCK workers tune candidates concurrently. `workers` may be increased but
cannot be below four. BLAS/OpenMP limits are set before process launch, Torch
intra/inter-op threads are limited to one, and selected component refits also
use the pool. The cluster closes on success or error. Tuning reports contain
worker PIDs and elapsed times. Seeds do not depend on worker scheduling.

## Run and predict

```r
install.packages(c("bnlearn", "torch", "glmnet", "nanoparquet", "sf"))
torch::install_torch() # only if the runtime is not already installed
```

```sh
Rscript tools/run_models/generative_models.R --smoke
Rscript tools/run_models/generative_models.R --full \
  /Users/isong/Documents/df_feat_correct_merged.parquet outputs/generative_models_full
```

An output folder must be empty. Sourcing the script starts no fits. To change
settings, call `run_generative_models()` with `predictors`, `years`, `workers`,
`outer_v`, `inner_v`, `holdout_years`, `test_prop`, and `seed`. Adjust the full
search ranges in `gen_candidates()` if needed.

```r
source("tools/run_models/generative_models.R")
gen_dependencies()
model <- readRDS("outputs/generative_models_full/PM25/models.rds")
# New years must exceed model$training_end_year; true concentrations are unnecessary.
predictions <- predict(model, new_data = covariates)
```

The smoke test uses 24 synthetic stations across eight years, both pollutants,
four candidates per family, three outer/two inner folds, three neural epochs,
one training seed, and eight latent draws. It tests all three backends, four
workers, temporal cutoffs, reserved-station exclusion, future-outcome
perturbation, response-independent prediction, batch invariance, and persistence.
It also asserts that tidymodels-related namespaces were not loaded. Smoke-test
accuracy is not a scientific comparison of these models.

For local verification, `bnlearn` is installed in a temporary library:

```sh
R_LIBS_USER=/private/tmp/huimori-generative-r-library \
  Rscript tools/run_models/generative_models.R --smoke
```

For later runs install dependencies in your normal library, or use that
temporary library while it exists.

## Reports

- `performance.csv`: RMSE, MAE, predictive R² (`1 - SSE/SST`), mean prediction
  bias, and row counts for BN, VAE, GAN, ensemble, and a historical training-mean
  baseline. Reports include pooled, year-specific, and site-type results, with
  cascade and holdout results labeled separately. R² is missing when truth is
  constant. Cascade ensemble warm-up rows are missing; compare row counts when
  comparing pooled component and ensemble cascade scores. Holdout rows match.
- `cascade_predictions.csv` and `holdout_predictions.csv`: row-level evidence.
- `*_tuning.csv`, `*_fold_scores.csv`, `selected_candidates.rds`: complete
  candidate scores/notes, temporal fold scores, and selected settings.
- `*_training_history.csv`: selected VAE/GAN fits' training objectives per epoch
  and seed. Training loss is not used as a substitute for predictive RMSE.
- `*_temporal_splits.csv`, `holdout_stations.csv`: split audits.
- `ensemble_weights.csv`, `blender_tuning.csv`, `models.rds`: ensemble settings
  and reusable fitted models. `configuration.rds`, `session_info.txt`, and
  `workers.log` provide run provenance.

References: [bnlearn prediction](https://www.bnlearn.com/examples/predict/),
[BN structure tuning](https://www.bnlearn.com/documentation/man/hc.html),
[R torch loss functions](https://torch.mlverse.org/docs/reference/nnf_binary_cross_entropy_with_logits.html).
