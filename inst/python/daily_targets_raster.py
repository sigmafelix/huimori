"""Bounded 2023 target exports; reuse immutable Gate E low-level COG primitives."""
import argparse
import csv
import gc
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "daehoon" / "scripts"))
import daily_gate_e_core as core
import numpy as np
import pyarrow.parquet as pq
from osgeo import gdal

FIELDS = ["chunk_id", "gid", "grid_cell_id", "date", "x", "y", "PM10", "PM25"]
JANUARY = Path("daehoon/data/daily_xgb_gate_e/year=2023/month=01/run=national_cog_002/rasters")


def load(path, date, windows):
    meta = core.read_json(str(path) + ".json")
    core.require(meta["status"] == "PASS" and meta["date"] == date, "Partition status/date mismatch")
    core.require(meta["sha256"] == core.sha(path), "Partition checksum mismatch")
    core.require(meta["schema"]["hash"] == core.SCHEMA, "Predictor schema drift")
    core.require(meta["scientific_release"] is False, "Nonproduction status required")
    w = windows[meta["chunk_id"]]
    t = pq.ParquetFile(path).read(use_threads=False)
    core.require(t.column_names == FIELDS and len(t) == w["cells"] == meta["rows"], "Partition schema/rows mismatch")
    core.require(set(t["chunk_id"].to_pylist()) == {w["chunk_id"]}, "Chunk changed")
    core.require(set(str(x) for x in t["date"].unique().to_pylist()) == {date}, "Date changed")
    core.require(core.spatial_sha(t) == w["spatial_sha256"], "Registered spatial identity changed")
    ids, rr, cc = core.coords(t)
    for p in core.POLS:
        v = t[p].to_numpy()
        core.exact_float32(v)
        thread = meta["timing"][p]
        core.require(thread["blas_before"] == thread["blas_after"] == 1 and thread["xgb_nthread"] == 5,
                     "Prediction thread evidence failed")
        core.require(thread["finite"] == len(t) and thread["negative"] == int((v < 0).sum()), "Prediction accounting failed")
    return t, ids, rr, cc, w, meta


def registry(path):
    rows = core.csv_rows(path)
    for w in rows:
        for k in ["position", "cells", "xoff", "yoff", "width", "height"]:
            w[k] = int(w[k])
    core.require(len(rows) == 593 and sum(w["cells"] for w in rows) == core.CELLS, "National registry mismatch")
    core.check_windows(rows)
    return {w["chunk_id"]: w for w in rows}


def check_values(path, entries, date, windows, pollutant):
    ds = gdal.Open(str(path))
    for entry in entries:
        t, ids, rr, cc, w, meta = load(entry, date, windows)
        block = ds.GetRasterBand(1).ReadAsArray(w["xoff"], w["yoff"], w["width"], w["height"])
        actual = block[rr-w["yoff"], cc-w["xoff"]].astype(np.float64)
        core.require(np.array_equal(actual, t[pollutant].to_numpy()), "Stored prediction/raster values differ")
    ds = None


def january_equal(path, date, pollutant):
    old = gdal.Open(str(JANUARY / date / f"{date}_{pollutant}.tif"))
    new = gdal.Open(str(path))
    core.check_geometry(old); core.check_geometry(new)
    for y in range(0, core.HEIGHT, 256):
        h = min(256, core.HEIGHT-y)
        a = old.GetRasterBand(1).ReadAsArray(0, y, core.WIDTH, h)
        b = new.GetRasterBand(1).ReadAsArray(0, y, core.WIDTH, h)
        core.require(np.array_equal(a, b, equal_nan=True), "January frozen raster mismatch")
    old = new = None


def run(index, windows_path, date, directory, review=False):
    core.require(len(date) == 10 and date.startswith("2023-") and
                 np.datetime64(date) >= np.datetime64("2023-01-01") and
                 np.datetime64(date) <= np.datetime64("2023-12-31"), "2023 date required")
    directory = Path(directory)
    core.require(str(directory).startswith("daehoon/data/daily_xgb_targets/year=2023/run="), "Isolated output required")
    entries = [r["path"] for r in core.csv_rows(index)]
    core.require(len(entries) == len(set(entries)) == 593, "Exactly 593 distinct partitions required")
    windows = registry(windows_path); occupied = np.zeros(core.WIDTH*core.HEIGHT, dtype=bool)
    start, cpu = time.monotonic(), time.process_time()
    final = directory / "rasters"
    if review:
        core.require(final.is_dir(), "Missing raster directory")
        stage = final
        datasets = {}
    else:
        stage = directory / "rasters.partial"
        core.require(not final.exists() and not stage.exists(), "Exclusive date output required")
        stage.mkdir()
        datasets = {p: core.create_raster(directory / f"{p}_staging.tif", p, date) for p in core.POLS}
        for ds in datasets.values():
            ds.SetMetadataItem("STATUS", "ORIGINAL_DAILY_2023_BASELINE")
            ds.SetMetadataItem("ESTIMAND", "RETROSPECTIVE")
            ds.SetMetadataItem("RELEASE", "NONPRODUCTION")
            ds.SetMetadataItem("SCIENTIFIC_RELEASE", "FALSE")
    chunks = set(); models = None
    try:
        for entry in entries:
            t, ids, rr, cc, w, meta = load(entry, date, windows)
            core.require(w["chunk_id"] not in chunks, "Duplicate chunk")
            chunks.add(w["chunk_id"]); core.mark(occupied, ids)
            if models is None:
                models = meta["models"]
                for source in models.values():
                    core.require(core.sha(source["path"]) == source["sha256"], "Fitted model changed")
            core.require(meta["models"] == models, "Mixed models")
            if not review:
                for p in core.POLS:
                    core.write_window(datasets[p], t[p].to_numpy(), rr, cc, w)
            del t, ids, rr, cc
            core.resources()
        core.require(chunks == set(windows) and int(occupied.sum()) == core.CELLS, "National completeness failed")
    finally:
        for p in datasets:
            datasets[p].FlushCache(); datasets[p] = None
    outputs = []
    for p in core.POLS:
        path = stage / f"{date}_{p}.tif"
        if not review:
            core.convert_cog(directory / f"{p}_staging.tif", path)
        cog = core.cog_info(path)
        stats = core.scan_raster(path, occupied, date, p)
        check_values(path, entries, date, windows, p)
        if date.startswith("2023-01"):
            january_equal(path, date, p)
        meta = dict(status="PASS", date=date, pollutant=p, chunks=593, cells=core.CELLS,
                    schema_sha256=core.SCHEMA, models=models, stats=stats, cog=cog,
                    exact_source_values=True, january_exact=date.startswith("2023-01"),
                    scientific_release=False, label="ORIGINAL_DAILY_2023_BASELINE",
                    estimand="RETROSPECTIVE", release="NONPRODUCTION",
                    sha256=core.sha(path), bytes=path.stat().st_size)
        if not review:
            core.atomic_json(stage / f"{date}_{p}.json", meta)
        else:
            previous = core.read_json(stage / f"{date}_{p}.json")
            core.require(previous["sha256"] == meta["sha256"] and previous["stats"] == stats, "Review metadata drift")
        outputs.append(meta)
    if not review:
        os.rename(stage, final)
    result = dict(status="PASS", date=date, chunks=593, cells=core.CELLS, outputs=outputs,
                  independent_review=review, january_exact=date.startswith("2023-01"),
                  wall_seconds=time.monotonic()-start, cpu_seconds=time.process_time()-cpu,
                  resources=core.resources())
    core.atomic_json(directory / ("review.json" if review else "result.json"), result)
    gc.collect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("index"); parser.add_argument("windows"); parser.add_argument("date"); parser.add_argument("directory")
    parser.add_argument("--review", action="store_true")
    args = parser.parse_args()
    run(args.index, args.windows, args.date, args.directory, args.review)
