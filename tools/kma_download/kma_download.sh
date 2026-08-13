#!/usr/bin/env bash
#
# Download KMA SFC grid NetCDF files over a range of times and observation
# variables from the apihub.kma.go.kr endpoint.
#
# For each requested obs var and each time step it fetches:
#   .../sfc_grid_nc_down.php?obs=<obs>&tm=<YYYYMMDDHHMM>&authKey=<key>
# and writes <outdir>/<obs>_<tm>.nc
#
set -euo pipefail

# ---- valid observation variables ------------------------------------------
VALID_OBS="ta hm ws_10m wv_10m pa ps rn_ox rn_day vs sd_tot sd_day sd_24h"
# ta=temperature  hm=humidity  ws_10m=wind speed  wv_10m=wind dir vector
# pa=pressure  ps=sea-level pressure  rn_ox=rain(binary)  rn_day=daily precip
# vs=visibility  sd_tot=snow total  sd_day=new snow  sd_24h=24h new snow

# ---- defaults --------------------------------------------------------------
OBS="$VALID_OBS"          # all by default
START=""
END=""
INTERVAL_H=1              # hours between time steps
OUTDIR="."
KEYFILE="$HOME/.kmakey"
DRYRUN=0
OVERWRITE=0

usage() {
  cat <<EOF
Usage: $(basename "$0") --start YYYYMMDDHHMM --end YYYYMMDDHHMM [options]

Required:
  --start YYYYMMDDHHMM   first time step
  --end   YYYYMMDDHHMM   last time step (inclusive)

Options:
  --interval HOURS       step size in hours (default: $INTERVAL_H)
  --obs LIST             comma/space separated subset, or "all" (default: all)
                         valid: $VALID_OBS
  --outdir DIR           output directory (default: current dir)
  --keyfile PATH         file containing authKey (default: ~/.kmakey)
  --overwrite            re-download even if the target file exists
  --dry-run              print the URLs/targets without downloading
  -h, --help             this help

Example:
  $(basename "$0") --start 202606301200 --end 202606301500 \\
                   --interval 1 --obs ta,hm --outdir ./kma_out
EOF
}

# ---- arg parsing -----------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    --start)     START="$2"; shift 2 ;;
    --end)       END="$2"; shift 2 ;;
    --interval)  INTERVAL_H="$2"; shift 2 ;;
    --obs)       OBS="${2//,/ }"; shift 2 ;;
    --outdir)    OUTDIR="$2"; shift 2 ;;
    --keyfile)   KEYFILE="$2"; shift 2 ;;
    --overwrite) OVERWRITE=1; shift ;;
    --dry-run)   DRYRUN=1; shift ;;
    -h|--help)   usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

# ---- validation ------------------------------------------------------------
[ -n "$START" ] && [ -n "$END" ] || { echo "ERROR: --start and --end are required" >&2; usage; exit 1; }
case "$START" in [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;; *) echo "ERROR: --start must be 12 digits YYYYMMDDHHMM" >&2; exit 1 ;; esac
case "$END"   in [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) ;; *) echo "ERROR: --end must be 12 digits YYYYMMDDHHMM" >&2; exit 1 ;; esac
case "$INTERVAL_H" in ''|*[!0-9]*) echo "ERROR: --interval must be a positive integer (hours)" >&2; exit 1 ;; esac
[ "$INTERVAL_H" -ge 1 ] || { echo "ERROR: --interval must be >= 1" >&2; exit 1; }

for o in $OBS; do
  echo " $VALID_OBS " | grep -q " $o " || { echo "ERROR: invalid obs '$o'. Valid: $VALID_OBS" >&2; exit 1; }
done

if [ "$DRYRUN" -eq 0 ]; then
  [ -r "$KEYFILE" ] || { echo "ERROR: key file not readable: $KEYFILE" >&2; exit 1; }
  KEY="$(cat "$KEYFILE")"
fi
mkdir -p "$OUTDIR"

# ---- portable date helpers (GNU or BSD) ------------------------------------
if date --version >/dev/null 2>&1; then GNU_DATE=1; else GNU_DATE=0; fi

# tm (YYYYMMDDHHMM) -> epoch seconds
tm_to_epoch() {
  local tm="$1"
  if [ "$GNU_DATE" -eq 1 ]; then
    date -u -d "${tm:0:4}-${tm:4:2}-${tm:6:2} ${tm:8:2}:${tm:10:2}:00" +%s
  else
    date -u -j -f "%Y%m%d%H%M" "$tm" +%s
  fi
}
# epoch seconds -> tm (YYYYMMDDHHMM)
epoch_to_tm() {
  if [ "$GNU_DATE" -eq 1 ]; then date -u -d "@$1" +%Y%m%d%H%M
  else date -u -j -f "%s" "$1" +%Y%m%d%H%M; fi
}

START_E="$(tm_to_epoch "$START")"
END_E="$(tm_to_epoch "$END")"
[ "$START_E" -le "$END_E" ] || { echo "ERROR: --start is after --end" >&2; exit 1; }
STEP=$((INTERVAL_H * 3600))

# ---- main loop -------------------------------------------------------------
BASE="https://apihub.kma.go.kr/api/typ01/url/sfc_grid_nc_down.php"
fail=0; ok=0
for o in $OBS; do
  e="$START_E"
  while [ "$e" -le "$END_E" ]; do
    tm="$(epoch_to_tm "$e")"
    out="$OUTDIR/${o}_${tm}.nc"
    url="${BASE}?obs=${o}&tm=${tm}&authKey=__KEY__"

    if [ "$DRYRUN" -eq 1 ]; then
      echo "[dry-run] $out  <=  $url"
    elif [ -s "$out" ] && [ "$OVERWRITE" -eq 0 ]; then
      echo "[skip]    $out (exists)"
    else
      echo "[get]     $out"
      if curl -fsS --retry 2 --output "$out" \
           "${BASE}?obs=${o}&tm=${tm}&authKey=${KEY}"; then
        # guard against an error page saved as .nc (KMA errors return small text).
        # Valid NetCDF starts with HDF5 magic 89 48 44 46 (nc4) or "CDF" (classic).
        magic="$(head -c 4 "$out" | od -An -tx1 | tr -d ' \n')"
        if [ "$magic" = "89484446" ] || [ "${magic:0:6}" = "434446" ]; then
          ok=$((ok+1))
        else
          echo "  WARNING: $out is not a NetCDF file (API error?). Keeping for inspection." >&2
          fail=$((fail+1))
        fi
      else
        echo "  ERROR: download failed for obs=$o tm=$tm" >&2
        rm -f "$out"
        fail=$((fail+1))
      fi
    fi
    e=$((e + STEP))
  done
done

[ "$DRYRUN" -eq 1 ] || echo "done: $ok ok, $fail failed"
