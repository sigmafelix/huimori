
list_configs <-
  list(
    targets::tar_target(
      name = chr_dir_data,
      command = {
        if (Sys.getenv("USER") == "isong") {
          file.path("/mnt/s", "Korea")
        } else if (Sys.getenv("USER") == "songlab") {
          file.path("/mnt", "hdd001", "Korea")
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
        } else {
          file.path(Sys.getenv("HOME"), "Documents", "GitHub", "histmap-ko")
        }
      }
    ),
    targets::tar_target(
      name = chr_date_range,
      # this command should be modified to adjust the range of prediction
      command = c(as.Date("2019-01-01"), as.Date("2023-12-31"))
    )
  )