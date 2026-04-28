#
library(pacman)
p_load(tidyverse, tidymodels, xgboost, nanoparquet, sf, data.table, finetune, lubridate, qs)

# path
basepath <- file.path(Sys.getenv("HOME"), "Documents")
# filename <- "dt_daily.parquet"
filename <- "df_feat_correct_merged_daily.qs"

# read
# dt <- arrow::read_parquet(file.path(basepath, filename))
dt <- qs::qread(file.path(basepath, filename))
dt <- rbindlist(dt, fill = TRUE)
# fill all nas with 0
dt[is.na(dt)] <- 0

# two-stage model
# stage 1: static variables
# stage 2: dynamic variables
if ("geometry" %in% names(dt)) {
  dtx <- dt[, !"geometry", with = FALSE]
} else {
  dtx <- dt
}

# data.table way to add weekday column
dtx2 <- dtx[
  ,
  `:=`(
    season = sin(2 * pi * as.integer(strftime(date, format = "%j")) / 365.25),
    weekday = lubridate::wday(date, week_start = 1),
    is_weekend = ifelse(weekday %in% c(6, 7), 1, 0)
  )
]

# subset year less than or equal to 2021
# dtx2 <- dtx[year >= 2021, .(season = sin(2 * pi * as.integer(strftime(date, format = "%j")) / 365.25), PM10, PM25, weekday, is_weekend, d_road, dsm, dem, mtpi, mtpi_1km, , weekday)]

# binarize weekday
dtx22 <- dtx2[
  year >= 2021,
  `:=`(
    PM10 = log(PM10 + 0.1), PM25 = log(PM25 + 0.1),
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

# deselect 1-3, 6-15th columns
dtx22 <- dtx22[year >= 2021]
dtx22 <- dtx22[, -c(1:3, 6:15)]


# 1. PM10 model
dt_pm10 <- dtx22[!is.na(PM10), !"PM25"]
covars <- setdiff(names(dt_pm10), "PM10")
dt_pm10[, (covars) := lapply(.SD, function(x) frollmean(x, 7, fill = mean(x, na.rm = TRUE))), .SDcols = covars]
# dt_pm10 <- dt_pm10[year == 2015, ]

pm10_split <- initial_time_split(dt_pm10, prop = 0.8)
train_data <- training(pm10_split)
test_data <- testing(pm10_split)

pm10_mod <- boost_tree(
  mtry = 7,
  trees = 2000,
  learn_rate = tune(),
  min_n = 5,
  tree_depth = tune(),
  loss_reduction = tune()
) %>%
  set_engine("xgboost") %>%
  set_mode("regression")

# add data and formula
pm10_workflow <-
  workflow() %>%
  add_model(pm10_mod) %>%
  add_formula(PM10 ~ .)

train_cv <- vfold_cv(train_data, v = 5)

# fit xgboost
pm10_fitted <- finetune::tune_race_anova(
  pm10_workflow,
  resamples = train_cv,
  metrics = metric_set(rmse, mae, rsq),
  control = control_race(save_pred = TRUE, verbose = TRUE, parallel_over = NULL)
)

# recover logarithm in predicted values
pm10_pred_recovered <- exp(pm10_fitted$.predictions[[1]]$.pred) - 0.1
pm10_orig_recovered <- exp(pm10_fitted$.predictions[[1]]$PM10) - 0.1

rmse_vec(estimate = pm10_pred_recovered, truth = pm10_orig_recovered)


# pm2.5
dt_pm25 <- dtx22[!is.na(PM25), !"PM10"]
covars <- setdiff(names(dt_pm25), "PM25")
dt_pm25[, (covars) := lapply(.SD, function(x) frollmean(x, 7, fill = mean(x, na.rm = TRUE))), .SDcols = covars]
# dt_pm25 <- dt_pm25[year == 2015, ]
pm25_split <- initial_time_split(dt_pm25, prop = 0.8)
train_data <- training(pm25_split)
test_data <- testing(pm25_split)

pm25_mod <- boost_tree(
  mtry = 7,
  trees = 2000,
  learn_rate = tune(),
  min_n = 5,
  tree_depth = tune(),
  loss_reduction = tune()
) %>%
  set_engine("xgboost") %>%
  set_mode("regression")

# add data and formula
pm25_workflow <-
  workflow() %>%
  add_model(pm25_mod) %>%
  add_formula(PM25 ~ .)

train_cv <- vfold_cv(train_data, v = 5)
# fit xgboost
pm25_fitted <- finetune::tune_race_anova(
  pm25_workflow,
  resamples = train_cv,
  metrics = metric_set(rmse, mae, rsq),
  control = control_race(save_pred = TRUE, verbose = TRUE, parallel_over = NULL)
)
# recover logarithm in predicted values
pm25_pred_recovered <- exp(pm25_fitted$.predictions[[1]]$.pred) - 0.1
pm25_orig_recovered <- exp(pm25_fitted$.predictions[[1]]$PM25) - 0.1
rmse_vec(estimate = pm25_pred_recovered, truth = pm25_orig_recovered)
