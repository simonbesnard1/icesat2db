# SPDX-License-Identifier: EUPL-1.2

"""Synthetic/real linkage benchmark; run from checkout with PYTHONPATH=.

Each method/size runs in a fresh process. Timings exclude imports, fixture
creation and checksums; peak RSS includes interpreter, imports and input data.
Default mode excludes HDF5/TileDB I/O and measures exact photon classification
and inclusive land-segment matching, checking identical output arrays.
--ingestion includes HDF5 reads through filtered beam DataFrame creation;
it excludes TileDB writes, downloads and final correctness assertions.
"""

import argparse
import gc
import hashlib
import json
import os
import platform
import resource
import subprocess
import sys
import time
from pathlib import Path

import h5py
import numpy as np
import pandas as pd

from icesat2db.beam.atl03_beam import ATL03Beam
from icesat2db.utils.atl03_atl08 import interval_indices, photon_indices, validate_pair
from icesat2db.utils.segment_id import pack_segment_id


def fixture(n):
    rng = np.random.default_rng(42)
    # Variable photon density, empty segments, and gaps in ATL08 land coverage.
    counts = rng.integers(0, 41, size=max(1, n // 20))
    counts[-1] += max(0, n - counts.sum())
    excess = counts.sum() - n
    if excess > 0:
        cumulative = np.cumsum(counts)
        counts = np.clip(n - (cumulative - counts), 0, counts)
    segment_ids = np.arange(len(counts), dtype=np.int64) + 100
    starts = np.cumsum(counts) - counts + 1
    starts[counts == 0] = 0
    selected = np.flatnonzero(rng.random(n) < 0.5)
    segment_rows = np.searchsorted(np.cumsum(counts), selected, side="right")
    signal_segments = segment_ids[segment_rows]
    offsets = selected - starts[segment_rows] + 2
    flags = rng.integers(0, 4, len(selected), dtype=np.int8)
    heights = rng.uniform(0, 30, len(selected)).astype(np.float32)
    begins = segment_ids[::5]
    ends = np.minimum(begins + 4, segment_ids[-1])
    keep = rng.random(len(begins)) > 0.1
    return (
        segment_ids,
        counts,
        starts,
        signal_segments,
        offsets,
        flags,
        heights,
        begins[keep],
        ends[keep],
    )


def real_fixture(atl03, atl08, beam):
    validate_pair(atl03, atl08)
    with h5py.File(atl03) as a, h5py.File(atl08) as b:
        geo = a[f"{beam}/geolocation"]
        signal = b[f"{beam}/signal_photons"]
        land = b[f"{beam}/land_segments"]
        return (
            geo["segment_id"][:],
            geo["segment_ph_cnt"][:].astype(np.int64),
            geo["ph_index_beg"][:].astype(np.int64),
            signal["ph_segment_id"][:],
            signal["classed_pc_indx"][:],
            signal["classed_pc_flag"][:],
            signal["ph_h"][:].astype(np.float32),
            land["segment_id_beg"][:],
            land["segment_id_end"][:],
        )


def numpy_link(data):
    seg, counts, starts, signal_seg, offsets, flags, heights, begins, ends = data
    n = counts.sum()
    rows = interval_indices(seg, begins, ends)
    links = np.full(len(seg), -1, dtype=np.int64)
    valid = rows >= 0
    links[valid] = pack_segment_id(123, 5, 0, begins[rows[valid]])
    links = np.repeat(links, counts)
    indices = photon_indices(seg, starts, counts, signal_seg, offsets, n)
    out_flags = np.full(n, -1, dtype=np.int8)
    out_heights = np.full(n, np.nan, dtype=np.float32)
    matched = indices >= 0
    out_flags[indices[matched]] = flags[matched]
    out_heights[indices[matched]] = heights[matched]
    return links, out_flags, out_heights


def pandas_link(data):
    seg, counts, starts, signal_seg, offsets, flags, heights, begins, ends = data
    n = counts.sum()
    photons = pd.DataFrame(
        {
            "segment": np.repeat(seg, counts),
            "offset": np.arange(n) - np.repeat(starts, counts) + 2,
        }
    )
    signal = pd.DataFrame(
        {"segment": signal_seg, "offset": offsets, "flag": flags, "height": heights}
    )
    merged = photons.merge(
        signal, on=["segment", "offset"], how="left", sort=False, validate="one_to_one"
    )
    parents = pd.DataFrame({"begin": begins, "end": ends})
    land = pd.merge_asof(
        photons[["segment"]], parents, left_on="segment", right_on="begin"
    )
    valid = land["begin"].notna() & land["segment"].le(land["end"])
    links = np.full(n, -1, dtype=np.int64)
    links[valid] = pack_segment_id(
        123, 5, 0, land.loc[valid, "begin"].to_numpy(dtype=np.int64)
    )
    return (
        links,
        merged.flag.fillna(-1).to_numpy(dtype=np.int8),
        merged.height.to_numpy(dtype=np.float32),
    )


def ingestion_worker(args):
    """Time HDF5 reads, beam parsing, matching, filtering and DataFrame creation."""
    validate_pair(args.atl03, args.atl08)
    linked = args.worker == "ingest_linked"
    times = []
    mapping = {"h_ph": {"SDS_Name": "heights/h_ph"}}
    # Warm OS/HDF5 caches; reopen files for every measured beam parse.
    for trial in range(args.repeats + 1):
        gc.collect()
        start = time.perf_counter()
        with h5py.File(args.atl03) as a, h5py.File(args.atl08) as b:
            data = ATL03Beam(
                a, args.beam, mapping, atl08_file=b if linked else None
            ).main_data
            n = len(a[f"{args.beam}/heights/delta_time"])
        duration = time.perf_counter() - start
        if trial:
            times.append(duration)
        kept = len(data)
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        if linked and trial == args.repeats:
            with h5py.File(args.atl03) as a, h5py.File(args.atl08) as b:
                signal = b[f"{args.beam}/signal_photons"]
                geo = a[f"{args.beam}/geolocation"]
                starts = dict(zip(geo["segment_id"][:], geo["ph_index_beg"][:]))
                # Independent dictionary/index arithmetic oracle.
                expected = np.array(
                    [
                        starts[s] + int(i) - 2
                        for s, i in zip(
                            signal["ph_segment_id"][:], signal["classed_pc_indx"][:]
                        )
                    ]
                )
                flags = signal["classed_pc_flag"][:]
                retained = data.set_index("photon_index")
                expected_signal = expected[flags > 0]
                np.testing.assert_array_equal(
                    retained.loc[expected_signal, "atl08_classed_pc_flag"],
                    flags[flags > 0],
                )
                np.testing.assert_allclose(
                    retained.loc[expected_signal, "atl08_ph_h"],
                    signal["ph_h"][:][flags > 0],
                )
                conf = a[f"{args.beam}/heights/signal_conf_ph"][:, 0]
                additional = int(np.count_nonzero(conf[expected_signal] < 3))
                assert kept == np.count_nonzero(conf >= 3) + additional
            matched = int((data.atl08_classed_pc_flag >= 0).sum())
            del retained, conf
        del data
    return {
        "method": args.worker,
        "photons": n,
        "retained_photons": kept,
        "additional_atl08_signal": additional if linked else 0,
        "classified_retained_photons": matched if linked else None,
        "median_seconds": float(np.median(times)),
        "times_seconds": times,
        "peak_rss_mib": peak,
    }


def worker(args):
    if args.worker.startswith("ingest_"):
        return ingestion_worker(args)
    data = (
        real_fixture(args.atl03, args.atl08, args.beam)
        if args.atl03
        else fixture(args.worker_size)
    )
    fn = numpy_link if args.worker == "numpy" else pandas_link
    result = fn(data)  # Warm up.
    del result
    times = []
    for _ in range(args.repeats):
        gc.collect()
        start = time.perf_counter()
        result = fn(data)
        times.append(time.perf_counter() - start)
        del result
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    result = fn(data)
    digest = hashlib.sha256()
    for array in result:
        digest.update(array.tobytes())
    return {
        "method": args.worker,
        "photons": int(data[1].sum()),
        "median_seconds": float(np.median(times)),
        "times_seconds": times,
        "peak_rss_mib": peak,
        "checksum": digest.hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sizes", nargs="+", type=int, default=[100_000, 1_000_000, 5_000_000]
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--ingestion", action="store_true")
    parser.add_argument("--atl03", type=Path)
    parser.add_argument("--atl08", type=Path)
    parser.add_argument("--beam", default="gt1l")
    parser.add_argument(
        "--worker", choices=["numpy", "pandas", "ingest_base", "ingest_linked"]
    )
    parser.add_argument("--worker-size", type=int)
    args = parser.parse_args()
    if bool(args.atl03) != bool(args.atl08) or (args.ingestion and not args.atl03):
        parser.error("Provide both --atl03 and --atl08 for real data/ingestion")
    if args.worker:
        print(json.dumps(worker(args)))
        return
    rows = []
    for size in [0] if args.atl03 else args.sizes:
        pair = []
        methods = (
            ("ingest_base", "ingest_linked") if args.ingestion else ("numpy", "pandas")
        )
        for method in methods:
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                method,
                "--worker-size",
                str(size),
                "--repeats",
                str(args.repeats),
            ]
            if args.atl03:
                command += [
                    "--atl03",
                    str(args.atl03),
                    "--atl08",
                    str(args.atl08),
                    "--beam",
                    args.beam,
                ]
            row = json.loads(subprocess.check_output(command, text=True))
            rows.append(row)
            pair.append(row)
            print(
                f"{row['photons']:,} {method}: {row['median_seconds']:.4f} s; {row['peak_rss_mib']:.1f} MiB RSS",
                flush=True,
            )
        if not args.ingestion:
            assert pair[0]["checksum"] == pair[1]["checksum"], "Methods disagree"
    report = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "cpu_count": os.cpu_count(),
        "repeats": args.repeats,
        "atl03": str(args.atl03) if args.atl03 else None,
        "atl08": str(args.atl08) if args.atl08 else None,
        "beam": args.beam if args.atl03 else None,
        "results": rows,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
