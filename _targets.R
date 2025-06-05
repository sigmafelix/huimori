# Load packages required to define the pipeliine:
library(targets)
library(tarchetypes) # Load other packages as needed.
library(terra)
library(chopin)
library(crew)

# set controllers
controller_01 <-
  crew::crew_controller_local(workers = 1)
controller_10 <- 
  crew::crew_controller_local(workers = 10)
controller_15 <-
  crew::crew_controller_local(workers = 15)




# Set target options:
targets::tar_option_set(
  packages =
    c("targets", "terra", "sf", "dplyr", "collapse",
      "data.table", "tibble", "tune", "yardstick", "workflows",
      "chopin", "mirai", "parsnip", "finetune", "huimori", "nanoparquet"),
  format = "qs", # Optionally set the default storage format. qs is fast.
  controller = crew::crew_controller_group(
    controller_01,
    controller_10,
    controller_15
  ),
  error = "continue",
  garbage_collection = 3,
  memory = "transient",
  deployment = "worker"
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
  list_tune_models
)