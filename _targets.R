# Load packages required to define the pipeliine:
library(targets)
library(tarchetypes)
# library(geotargets)
library(terra)
library(chopin)
library(crew)
library(sf)

sf::sf_use_s2(FALSE) # Disable S2 geometry library for sf package.

# set controllers
controller_01 <-
  crew::crew_controller_local(name = "controller_01", workers = 1)
controller_04 <-
  crew::crew_controller_local(name = "controller_04", workers = 4)
controller_08 <-
  crew::crew_controller_local(name = "controller_08", workers = 8)
controller_10 <-
  crew::crew_controller_local(name = "controller_10", workers = 10)
controller_15 <-
  crew::crew_controller_local(name = "controller_15", workers = 15)
controller_20 <-
  crew::crew_controller_local(name = "controller_20", workers = 20)




# Set target options:
targets::tar_option_set(
  packages =
    c("targets",# "geotargets", 
      "terra", "sf", "dplyr", "collapse",
      "data.table", "tibble", "tune", "yardstick", "workflows",
      "recipes", "dials",
      "chopin", "mirai", "parsnip", "finetune", "huimori", "nanoparquet",
      "readxl", "bonsai", "lightgbm", "xgboost", "crew"),
  format = "qs", # Optionally set the default storage format. qs is fast.
  controller = crew::crew_controller_group(
    controller_01,
    controller_04,
    controller_08,
    controller_10,
    controller_15,
    controller_20
  ),
  error = "continue",
  garbage_collection = 3,
  memory = "transient",
  deployment = "worker",
  storage = "worker",
  retrieval = "worker"
)

# Run the R scripts in the R/ folder with your custom functions:
targets::tar_source("inst/targets/1_init_targets.R")
targets::tar_source("inst/targets/2_pin_files.R")
targets::tar_source("inst/targets/3_process_feature.R")
targets::tar_source("inst/targets/4_tune_models.R")

# Replace the target list below with your own:
list(
  list_configs,
  list_basefiles,
  list_process_site,
  list_process_feature,
  list_process_split,
  list_fit_models,
  list_tune_models
)