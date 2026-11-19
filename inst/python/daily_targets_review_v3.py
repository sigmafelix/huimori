"""Independent full review with a pinned, versioned prediction input loader."""
import argparse
import os
from pathlib import Path

for name in ["OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS",
             "OMP_NUM_THREADS", "OMP_THREAD_LIMIT", "GDAL_NUM_THREADS"]:
    os.environ[name] = "1"
import daily_targets_raster_v3 as raster
from threadpoolctl import threadpool_info, threadpool_limits


def run(source, windows, destination):
    source, destination = Path(source), Path(destination)
    core = raster.core
    core.require(str(destination).startswith("daehoon/data/daily_xgb_targets/year=2023/run="), "Isolated review required")
    pins = core.read_json(destination / "implementation.json")
    core.require(all(core.sha(p) == sha for p, sha in pins.items()), "Reviewer implementation drift")
    result = core.read_json(source / "result.json")
    core.require(result["status"] == "PASS", "Raster writer not accepted")
    protected = [source / "inputs.csv", source / "implementation.json", source / "result.json", source / "resource_plan.json"]
    protected += sorted((source / "rasters").iterdir())
    before = {str(p): core.sha(p) for p in protected}
    link = destination / "rasters"
    core.require(not link.exists() and not link.is_symlink(), "Exclusive review required")
    link.symlink_to(os.path.relpath(source / "rasters", destination), target_is_directory=True)
    with threadpool_limits(limits=1):
        core.require(all(x["num_threads"] == 1 for x in threadpool_info()), "Nested thread violation")
        raster.run(source / "inputs.csv", windows, result["date"], destination, review=True)
        core.require(all(x["num_threads"] == 1 for x in threadpool_info()), "Thread drift")
    core.require(before == {str(p): core.sha(p) for p in protected}, "Protected raster changed during review")
    core.require(all(core.sha(p) == sha for p, sha in pins.items()), "Reviewer implementation changed")
    core.atomic_json(destination / "preservation.json", dict(status="PASS", protected_sha256=before,
        numerical_threads=1, threads=threadpool_info()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source"); parser.add_argument("windows"); parser.add_argument("destination")
    args = parser.parse_args()
    run(args.source, args.windows, args.destination)
