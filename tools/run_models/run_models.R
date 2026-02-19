#
library(pacman)
p_load(tidyverse, tidymodels, xgboost, nanoparquet, sf, data.table, finetune)

# path
basepath <- file.path(Sys.getenv("HOME"), "Documents")
filename <- "dt_daily.parquet"


# read
dt <- arrow::read_parquet(file.path(basepath, filename))

# two-stage model
# stage 1: static variables
# stage 2: dynamic variables
dtx <- dt[, !"geometry", with = FALSE]

dtx2 <- dtx[, .(PM10, PM25, weekday, is_weekend, d_road, dsm, dem, mtpi, mtpi_1km, lc_CRP, lc_FST, lc_WET, lc_IMP, lc_WTR, weekday)]

# binarize weekday
dtx22 <- dtx2[
  ,
  .(PM10, PM25, is_weekend, d_road, dsm, dem, mtpi, mtpi_1km, lc_CRP, lc_FST, lc_WET, lc_IMP, lc_WTR,
    weekday_mon = ifelse(weekday == 1, 1, 0),
    weekday_tue = ifelse(weekday == 2, 1, 0),
    weekday_wed = ifelse(weekday == 3, 1, 0),
    weekday_thu = ifelse(weekday == 4, 1, 0),
    weekday_fri = ifelse(weekday == 5, 1, 0),
    weekday_sat = ifelse(weekday == 6, 1, 0),
    weekday_sun = ifelse(weekday == 7, 1, 0),
    diff_dsm_dem = dsm - dem
  )
]

# 1. PM10 model
dt_pm10 <- dtx22[!is.na(PM10), !"PM25"]
covars <- setdiff(names(dt_pm10), "PM10")
dt_pm10[, (covars) := lapply(.SD, function(x) frollmean(x, 7, fill = mean(x, na.rm = TRUE))), .SDcols = covars]

pm10_split <- initial_time_split(dt_pm10, prop = 0.8)
train_data <- training(pm10_split)
test_data <- testing(pm10_split)

pm10_mod <- boost_tree(
  mtry = tune(),
  trees = 1000,
  learn_rate = tune(),
  min_n = tune(),
  tree_depth = tune(),
) %>%
  set_engine("xgboost") %>%
  set_mode("regression")

# add data and formula
pm10_workflow <-
  workflow() %>%
  add_model(pm10_mod) %>%
  add_formula(PM10 ~ .)

train_cv <- vfold_cv(train_data, v = 10)

# fit xgboost
pm10_fitted <- finetune::tune_sim_anneal(
  pm10_workflow,
  resamples = train_cv,
  metrics = metric_set(rmse, rsq),
  control = control_sim_anneal(save_pred = TRUE)
)
