% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/helpers.R
\name{fill_date}
\alias{fill_date}
\title{Fill start or end date from data context}
\usage{
fill_date(date, year, start = TRUE)
}
\arguments{
\item{date}{Unquoted field name (e.g., field name specification in dplyr)}

\item{year}{numeric(1). Year to fill in.}

\item{start}{logical(1). Is this the start date?}
}
\value{
Vector filled with start or end date depending on the context.
}
\description{
Fill start or end date from data context
}
\note{
tz is fixed as "Asia/Seoul".
}
