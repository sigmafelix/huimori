library(sf)
library(tmap)
library(geodata)

sf::sf_use_s2(FALSE)

kor <- geodata::gadm(country = "KOR", level = 2, path = "~/.geodatacache") %>%
  st_as_sf() %>%
  st_transform("EPSG:32652")  # Transform to Korean CRS (EPSG:5179)

# --- Step 2: Define bounding boxes ---
bbox_main <- st_bbox(c(xmin = 140000, ymin = 3650000, xmax = 550000, ymax = 4275000), crs = st_crs(kor))

bbox_baengnyeong <- st_bbox(c(xmin = 110600, ymin = 4184700, xmax = 133700, ymax = 4218440), crs = st_crs(kor))

# TODO: define exact bounding boxes
bbox_yeonpyeong  <- st_bbox(c(xmin = 115000, ymin = 1420000, xmax = 125000, ymax = 1430000), crs = st_crs(kor))
bbox_ulleung     <- st_bbox(c(xmin = 235000, ymin = 1350000, xmax = 245000, ymax = 1360000), crs = st_crs(kor))
bbox_dokdo       <- st_bbox(c(xmin = 250000, ymin = 1340000, xmax = 255000, ymax = 1345000), crs = st_crs(kor))

# --- Step 3: Create main and inset maps ---
main_map <- tm_shape(kor, bbox = bbox_main) +
  tm_polygons() +
  tm_layout(title = "Republic of Korea with Insets", frame = FALSE)

inset_baengnyeong <- tm_shape(kor, bbox = bbox_baengnyeong) +
  tm_polygons() +
  tm_layout(frame = FALSE, bg.color = "white")

inset_yeonpyeong <- tm_shape(kor, bbox = bbox_yeonpyeong) +
  tm_polygons() +
  tm_layout(frame = FALSE, bg.color = "white")

inset_ulleung <- tm_shape(kor, bbox = bbox_ulleung) +
  tm_polygons() +
  tm_layout(frame = FALSE, bg.color = "white")

inset_dokdo <- tm_shape(kor, bbox = bbox_dokdo) +
  tm_polygons() +
  tm_layout(frame = FALSE, bg.color = "white")

# --- Step 4: Compose final map with insets ---
final_map <- main_map +
  inset_baengnyeong +
  tm_inset(bbox_baengnyeong, position = c(-0.2, 0.88), width = 4, height = 6) +
  tm_inset(bbox_yeonpyeong,  position = c(-0.20, 0.8), width = 4, height = 4) +
  tm_inset(bbox_ulleung,     position = c(0.92, 0.95), width = 3, height = 3) +
  tm_inset(bbox_dokdo,       position = c(0.92, 0.9), width = 2, height = 2)

# --- Step 5: Display ---
tmap_mode("plot")  # use "view" for interactive
final_map
