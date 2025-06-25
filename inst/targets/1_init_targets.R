
list_configs <-
  list(
    targets::tar_target(
      name = chr_dir_data,
      command = file.path(Sys.getenv("HOME"), "Documents")#file.path("/mnt/s", "Korea")
    ),
    targets::tar_target(
      name = chr_dir_git,
      command = file.path(Sys.getenv("HOME"), "GitHub", "histmap-ko")
    )
  )