daily_2023_controller_local <- function(name, workers, tasks_max = Inf, log_directory = NULL) {
  # crew 1.3.0 prunes launch records after connection. Retain live processx
  # handles independently so their cleanup finalizers cannot kill busy workers.
  launcher_class <- R6::R6Class("daily_2023_retained_local_launcher",
    inherit = crew::crew_class_launcher_local,
    private = list(handles = list()),
    public = list(
      launch_worker = function(call) {
        private$handles <- Filter(function(p) p$is_alive(), private$handles)
        process <- super$launch_worker(call)
        private$handles[[length(private$handles) + 1L]] <- process
        process
      },
      terminate = function() {
        super$terminate()
        for (process in private$handles) {
          if (process$is_alive()) process$kill_tree()
          process$wait(timeout = 5000)
        }
        private$handles <- list()
        invisible()
      }
    ))
  tls <- crew::crew_tls()
  launcher <- launcher_class$new(name = name, workers = workers,
    seconds_interval = 0.25, seconds_timeout = 60, seconds_launch = 30,
    seconds_idle = 300, seconds_wall = Inf, tasks_max = tasks_max, tasks_timers = 0L,
    tls = tls, r_arguments = c("--no-save", "--no-restore"),
    options_metrics = crew::crew_options_metrics(),
    options_local = crew::crew_options_local(log_directory = log_directory))
  crew::crew_controller(client = crew::crew_client(tls = tls), launcher = launcher,
    crashes_max = 5L)
}

daily_2023_controller20 <- function(workers = 20L, log_directory = "daehoon/data/daily_xgb_targets_audit/crew_prediction20_v4") {
  daily_2023_controller_local("daily_predict_20", workers, tasks_max = 12L, log_directory = log_directory)
}
