
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


## Debugging rasterize_freq function
library(terra)
r <- rast(matrix(sample(1:20, 2.5e6, replace = TRUE), 2.5e3, 1e3))
mat <- matrix(1, 67, 67)

# remain a circle in the center with radius 33
mat <- matrix(0, 67, 67)
for (i in 1:67) {
  for (j in 1:67) {
    if (sqrt((i - 34) ^ 2 + (j - 34) ^ 2) <= 33) {
      mat[i, j] <- 1
    }
  }
}
terraOptions(progress = 16L, memmax = 128L)

system.time({
  freq_list <- rasterize_freq(r, mat)
})
freq_list