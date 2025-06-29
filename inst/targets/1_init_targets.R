
list_configs <-
  list(
    targets::tar_target(
      name = chr_dir_data,
      command = {
        if (Sys.getenv("USER") == "isong") {
          file.path("/mnt/s", "Korea")
        } else {
          file.path(Sys.getenv("HOME"), "Documents")
        }
      }
    ),
    targets::tar_target(
      name = chr_dir_git,
      command = file.path(Sys.getenv("HOME"), "GitHub", "histmap-ko")
    )
  )