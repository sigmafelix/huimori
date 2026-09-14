# Definition-time configuration is repository-local. Runtime authority checks
# remain strict, but a missing target store must not break the annual DAG.
daily_2023_selection <- function(path = "inst/config/daily_xgb_2023_selection.json") {
  x <- jsonlite::read_json(path, simplifyVector = TRUE)
  stopifnot(x$version == 1L, x$year == 2023L,
    nrow(x$grid) == 7116L, nrow(x$registry) == 593L,
    !anyDuplicated(x$grid[c("chunk_id", "month")]),
    all(table(x$grid$month) == 593L), sum(x$registry$cells) == 111637615)
  x
}

daily_2023_definition_metadata <- function(path = "_targets/meta/meta") {
  if (!file.exists(path)) return(NULL)
  utils::read.delim(path, sep = "|", quote = "", colClasses = "character")
}

daily_2023_select_indices <- function(parent, names, design, metadata = NULL) {
  declared <- design$indices[[parent]]
  fallback <- declared$index[match(names, declared$branch)]
  if (anyNA(fallback) || anyDuplicated(fallback)) stop("Undeclared daily branch selection.")
  if (!is.null(metadata) && parent %in% metadata$name) {
    children <- strsplit(metadata$children[match(parent, metadata$name)], "*", fixed = TRUE)[[1L]]
    actual <- match(names, children)
    if (!anyNA(actual) && !anyDuplicated(actual)) return(sort(as.integer(actual)))
  }
  # A bootstrap declaration only. Daily execution must independently validate
  # the current catalog; this fallback never authorizes reading different data.
  sort(as.integer(fallback))
}

daily_2023_assert_selection_runtime <- function(design, metadata) {
  if (is.null(metadata)) stop("Accepted daily source target store is required.")
  for (parent in names(design$indices)) {
    if (!parent %in% metadata$name) stop("Accepted source parent absent: ", parent)
    children <- strsplit(metadata$children[match(parent, metadata$name)], "*", fixed = TRUE)[[1L]]
    expected <- design$indices[[parent]]$branch
    if (anyNA(match(expected, children)) || anyDuplicated(children)) stop("Accepted source branch catalog changed: ", parent)
  }
  invisible(TRUE)
}
