
library(tidymodels)
library(targets)
vn<-        c(
          "dsm", "dem", "d_road",
          sprintf("class_%02d", c(1,3,11,13,14,15,16,21,22,31,32))
        )
fm <- reformulate(
          termlabels = vn,
          response = "PM10"
        )

tar_read(df_feat_correct_merged) %>%
  dplyr::mutate(d_road = as.numeric(d_road) / 1000) %>%
  dplyr::filter(year == 2019) %>%
  dplyr::filter(!is.na(PM10)) %>%
  fit_tidy_xgb(
    data = .,
    formula = fm, invars = vn)

  