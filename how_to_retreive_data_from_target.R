library(targets)
library(dplyr)

# ============================================================
# 1. 관측소 일별 자료: df_feat_correct_merged_daily
# ============================================================
# 2015-01 ~ 2023-12의 108개 월별 branch로 저장되어 있습니다.
# tar_load()하면 108개 월별 tibble이 list 형태로 불러와집니다.

tar_load(df_feat_correct_merged_daily)

correct_daily <- bind_rows(df_feat_correct_merged_daily)

# 예시: 첫 3행
correct_daily |>
  slice_head(n = 3) |>
  print(width = Inf)


# ============================================================
# 2. 격자 일별 자료: df_feat_grid_merged_daily
# ============================================================
# 전체 prediction grid는 593개의 spatial chunk로 나뉩니다.
# 각 chunk마다 2015-01 ~ 2023-12의 108개월 자료가 있으므로,
#
#   593 chunks × 108 months = 64,044 branches
#
# 입니다.
#
# 각 branch에는 수백만 행의 실제 자료를 저장하는 대신,
# 필요할 때 자료를 생성하기 위한 작은 contract가 저장되어 있습니다.
#
# children[[1]] : parent target에 속한 64,044개 branch 이름 전체를 꺼냅니다.
# branches[1]   : 그중 첫 번째 branch 하나를 선택합니다.

tar_source()

branches <- tar_meta(
  names = df_feat_grid_merged_daily,
  fields = children
)$children[[1]]

length(branches)
# 64044

# 64044개의 브랜치 중 첫번째만 읽어오기.
contract <- tar_read_raw(branches[1])

# 선택된 branch의 월과 spatial chunk 확인
contract$month
contract$chunk_id


# 실제 값을 확인하려면 다음 함수를 이용해야 한다.
# 여기서는 전체 월을 만들지 않고 2015-01-01 ~ 03만 생성합니다.

grid_daily <- materialize_grid_daily_contract(
  contract,
  dates = as.Date(c(
    "2015-01-01",
    "2015-01-02",
    "2015-01-03"
  ))
)

# 예시: gid = 1의 3일 자료
grid_daily |>
  filter(gid == 1) |>
  arrange(date) |>
  print(width = Inf)