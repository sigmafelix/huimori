
list_configs <-
  list(
    targets::tar_target(
      name = chr_dir_data,
      command = {
        if (Sys.getenv("USER") %in% c("isong", "songlab", "dhnyu")) {
          file.path("/mnt/hdd001", "Korea")
        } else {
          file.path(Sys.getenv("HOME"), "Documents")
        }
      }
    ),
    targets::tar_target(
      name = chr_dir_git,
      command = {
        if (Sys.getenv("USER") == "isong") {
          file.path(Sys.getenv("HOME"), "GitHub", "histmap-ko")
        } else if (Sys.getenv("USER") == "songlab") {
          file.path("/members", "songlab", "GitHub", "histmap-ko")
        } else if (Sys.getenv("USER") == "felix") {
          file.path(Sys.getenv("HOME"), "GitHub", "histmap-ko")
        } else {
          file.path(Sys.getenv("HOME"), "histmap-ko")
        }
      }
    ),
    targets::tar_target(
      name = chr_date_range,
      # this command should be modified to adjust the range of prediction
      command = c(as.Date("2015-01-01"), as.Date("2023-12-31"))
    ),
    
    ## Yearly Branching
    targets::tar_target(
      name = int_years_spatial,
      command = seq(2015, 2023, 1)
    ),
    
    ## Monthly Branching
    targets::tar_target(
      name = chr_months_spatial,
      command = {
        seq(from = chr_date_range[1], to = chr_date_range[2], by = "month") |> 
          format("%Y-%m")
      }
    )
  )