"""Versioned loader for explicitly recorded 8x5 and 20x2 prediction partitions."""
import argparse
import daily_targets_raster as base

core = base.core


def load(path, date, windows):
    meta = core.read_json(str(path) + ".json")
    core.require(meta["status"] == "PASS" and meta["date"] == date, "Partition status/date mismatch")
    core.require(meta["sha256"] == core.sha(path), "Partition checksum mismatch")
    core.require(meta["schema"]["hash"] == core.SCHEMA, "Predictor schema drift")
    core.require(meta["scientific_release"] is False, "Nonproduction status required")
    mode = meta.get("resource_mode")
    if mode is None:
        threads = 5
    else:
        core.require(mode == dict(version=3, prediction_workers=20, xgboost_threads=2, blas_threads=1),
                     "Unrecognized prediction execution mode")
        threads = 2
    w = windows[meta["chunk_id"]]
    t = base.pq.ParquetFile(path).read(use_threads=False)
    core.require(t.column_names == base.FIELDS and len(t) == w["cells"] == meta["rows"], "Partition schema/rows mismatch")
    core.require(set(t["chunk_id"].to_pylist()) == {w["chunk_id"]}, "Chunk changed")
    core.require(set(str(x) for x in t["date"].unique().to_pylist()) == {date}, "Date changed")
    core.require(core.spatial_sha(t) == w["spatial_sha256"], "Registered spatial identity changed")
    ids, rr, cc = core.coords(t)
    for p in core.POLS:
        v = t[p].to_numpy()
        core.exact_float32(v)
        thread = meta["timing"][p]
        core.require(thread["blas_before"] == thread["blas_after"] == 1 and thread["xgb_nthread"] == threads,
                     "Prediction thread evidence failed")
        core.require(thread["finite"] == len(t) and thread["negative"] == int((v < 0).sum()), "Prediction accounting failed")
    return t, ids, rr, cc, w, meta


def run(*args, **kwargs):
    # One isolated process per date. Substitute only the versioned input loader;
    # retain every accepted occupancy, raster, COG and full-value review check.
    original = base.load
    try:
        base.load = load
        return base.run(*args, **kwargs)
    finally:
        base.load = original


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("index"); parser.add_argument("windows"); parser.add_argument("date"); parser.add_argument("directory")
    parser.add_argument("--review", action="store_true")
    args = parser.parse_args()
    run(args.index, args.windows, args.date, args.directory, args.review)
