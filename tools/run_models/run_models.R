#
library(pacman)
p_load(tidyverse, tidymodels, xgboost, nanoparquet, sf)

# path
basepath <- file.path(Sys.getenv("HOME"), "Documents")
filename <- "dt_daily.parquet"

# read
dt <- arrow::read_parquet(file.path(basepath, filename))

# two-stage model
# stage 1: static variables
# stage 2: dynamic variables


# xgboost
