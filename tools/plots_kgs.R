library(targets)
library(tidyterra)
library(terra)
library(ggplot2)
library(geodata)
library(sf)
sf::sf_use_s2(FALSE)

kor <- geodata::gadm(country = "KOR", level = 0, path = "~/.geodatacache") %>%
  terra::project("EPSG:5179")  # Transform to Korean CRS (EPSG:5179)


res1 <- seq(1, 18, 2)
res2 <- seq(2, 18, 2)

list10 <- list()
list25 <- list()

for (i in seq_along(res1)) {
    list10[[i]] <- tar_read(df_diff_correct_incorrect_coord, res1[i]) %>%
    tidyterra::as_spatraster(xycols = 2:3, crs = 5179)
}
for (i in seq_along(res2)) {
    list25[[i]] <- tar_read(df_diff_correct_incorrect_coord, res2[i]) %>%
    tidyterra::as_spatraster(xycols = 2:3, crs = 5179)
}

getrange <- \(res) range(terra::values(res$diff), na.rm = TRUE)
# ranges in one data.frame
res_list <-
  Map(getrange, list10) %>%
  Reduce(rbind, .) %>%
  as.data.frame()
names(res_list) <- c("min", "max")
res_list$year <- 2015:2023


res_list25 <-
  Map(getrange, list25) %>%
  Reduce(rbind, .) %>%
  as.data.frame()
names(res_list25) <- c("min", "max")
res_list25$year <- 2015:2023

col_pal <- cols4all::c4a("brewer.rd_bu", n = 12)
quickplot <- function(
    res,
    pnt_corr = NULL,
    pnt_orig = NULL,
    year_target = 2015,
    mask_in = kor,
    title = "Differences (PM10)") {
#   pnt_corr_y <- pnt_corr[pnt_corr$year == year_target & pnt_corr$dist_m != 0, ] %>%
#     terra::vect()
#   pnt_orig_y <- pnt_orig[pnt_orig$year == year_target, ] %>%
#     terra::vect()
  masked <- terra::mask(res, mask_in)
  plot(masked,
    main = title,
    col = col_pal[-11:-12],
    axes = FALSE,
    breaks=c(-12, -10, -8, -6, -4, -2, 0, 2, 4, 6, 8),
    zlim=c(-11.8,7.4),
    las=2,yaxt="n",xaxt="n")

  # points(pnt_corr_y, pch = 19, col = "purple", cex = 3)
  # points(pnt_orig_y, pch = 4, col = "black", cex = 1)
  terra::sbar(50000, labels = c("0", "", "  50 km"))
}

png(
  "tools/pm10_diff_xgb_2015.png",
  width = 1200, height = 1200,
  pointsize = 10,
  res = 254, bg = "white"
)
quickplot(list10[[1]], year_target = 2015)
dev.off()
png(
  "tools/pm10_diff_xgb_2016.png",
  width = 1200, height = 1200,
  pointsize = 10,
  res = 254, bg = "white"
)
quickplot(list10[[2]], year_target = 2016)
dev.off()
png(
  "tools/pm10_diff_xgb_2022.png",
  width = 1200, height = 1200,
  pointsize = 10,
  res = 254, bg = "white"
)
quickplot(list10[[8]], year_target = 2022)
dev.off()
png(
  "tools/pm10_diff_xgb_2023.png",
  width = 1200, height = 1200,
  pointsize = 10,
  res = 254, bg = "white"
)
quickplot(list10[[9]], year_target = 2023)
dev.off()



# rangeplot
library(ggplot2)
library(ggpubr)

# Plot the range of differences
plot_ranges <-
  function(
    res_list,
    title = "Range of Differences by Year (PM10)") {
    res_df <- as.data.frame(res_list)
    res_df <- res_df %>%
      mutate(
        min = ifelse(max - min < 0.01, NA, min),
        max = ifelse(max - min < 0.01, NA, max)
      )

    ggplot(res_df, aes(x = year, ymin = min, ymax = max)) +
      geom_linerange(color = "blue", linewidth=2) +
      labs(title = title,
           x = "Year",
           y = "Difference Range") +
      scale_x_continuous(breaks = seq(2015, 2023, 1)) +
      theme_minimal() +
      theme(
        text= element_text(size = 14),
        axis.text = element_text(size = 12)
      )

}


# plots
gg_res_pm10 <- plot_ranges(res_list)
gg_res_pm25 <- plot_ranges(res_list25, title = "Range of Differences by Year (PM2.5)")

ggsave(
  gg_res_pm10,
  filename = "tools/pm10_range_xgb_plot.png",
  width = 10,
  height = 4,
  dpi = 300,
  bg = "white"
)
ggsave(
  gg_res_pm25,
  filename = "tools/pm25_range_xgb_plot.png",
  width = 10, height = 4,
  dpi = 300,
  bg = "white"
)



col_pal <- cols4all::c4a("brewer.rd_bu", n = 12)
quickplot25 <- function(
    res,
    pnt_corr = NULL,
    pnt_orig = NULL,
    year_target = 2015,
    title = "Differences (PM2.5)") {
#   pnt_corr_y <- pnt_corr[pnt_corr$year == year_target & pnt_corr$dist_m != 0, ] %>%
#     terra::vect()
#   pnt_orig_y <- pnt_orig[pnt_orig$year == year_target, ] %>%
#     terra::vect()
  plot(res,
    main = title,
    col = col_pal[-11:-12],
    breaks=c(-12, -10, -8, -6, -4, -2, 0, 2, 4, 6, 8),
    zlim=c(-11.8,7.4),
    axes = FALSE,
    las=2,yaxt="n",xaxt="n")
  
#   points(pnt_corr_y, pch = 19, col = "purple", cex = 3)
#   points(pnt_orig_y, pch = 4, col = "black", cex = 1)
  terra::sbar(50000, labels = c("0", "", "  50 km"))
}



png(
  "tools/pm25_diff_xgb_2015.png",
  width = 1200, height = 1200,
  pointsize = 10,
  res = 254, bg = "white"
)
quickplot(list25[[1]], year_target = 2015, title = "Differences (PM2.5)")
dev.off()
png(
  "tools/pm25_diff_xgb_2016.png",
  width = 1200, height = 1200,
  pointsize = 10,
  res = 254, bg = "white"
)
quickplot(list25[[2]], year_target = 2016, title = "Differences (PM2.5)")
dev.off()
png(
  "tools/pm25_diff_xgb_2022.png",
  width = 1200, height = 1200,
  pointsize = 10,
  res = 254, bg = "white"
)
quickplot(list25[[8]], year_target = 2022, title = "Differences (PM2.5)")
dev.off()
png(
  "tools/pm25_diff_xgb_2023.png",
  width = 1200, height = 1200,
  pointsize = 10,
  res = 254, bg = "white"
)
quickplot(list25[[9]], year_target = 2023, title = "Differences (PM2.5)")
dev.off()
