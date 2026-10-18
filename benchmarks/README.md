# ATL03/ATL08 benchmark report

Measured on 2026-09-16 in the local `icesat2db` conda environment: Python 3.12.13,
NumPy 2.4.6, pandas 3.0.3; Linux x86-64, 20 logical CPUs. Raw measurements and
checksums are stored alongside this report in JSON.

## Matching kernels

| Data | Photons | NumPy seconds | pandas seconds | Speedup | Peak RSS MiB, NumPy / pandas |
|---|---:|---:|---:|---:|---:|
| Synthetic | 100,000 | 0.0039 | 0.0239 | 6.1× | 172.5 / 181.3 |
| Synthetic | 1,000,000 | 0.0329 | 0.1762 | 5.4× | 209.3 / 271.3 |
| Synthetic | 5,000,000 | 0.1671 | 0.8553 | 5.1× | 376.5 / 653.8 |
| Real gt1l | 9,287,489 | 0.0244 | 0.7914 | 32.4× | 293.4 / 716.8 |

The pandas baseline performs an exact photon join on geolocation segment and
within-segment photon offset, followed by an inclusive interval join for land
segments. The NumPy implementation maps classification indices directly and
matches land intervals at geolocation-segment rate before expanding the links.
This compares two **correct** joins, not an incorrect equality join on the two
products' existing packed IDs. The old ingestion pipeline had no combined join,
so these speedups are relative to the benchmark's pandas baseline, not to the
previous complete ingestion pipeline.

Each method/size runs in its own process with one warm-up and five measured
repetitions. Reported time is the median; RSS is the process high-water mark,
including imports, fixture creation and input arrays, captured before checksums.
Input generation/loading, HDF5 reads, TileDB writes, and checksums are excluded
from matching timings. All three output arrays (parent IDs, classifications and
relative heights) have identical SHA-256 checksums for both methods at every size.

Synthetic data use seed 42, variable photon counts, empty geolocation segments,
gaps in land coverage, and approximately 50% classified records. The real beam is
`gt1l` from `ATL03/ATL08_20181014001049_02350102_007_01.h5`; its ATL08 file has
9,048 classified records and 62 land segments for 9,287,489 ATL03 photons.
Its sparse ATL08 coverage explains why the relative speedup is larger than in the
synthetic case. These are warm local benchmarks of one real acquisition, not
claims about all tracks, network ingestion or S3 query performance.

## Beam parsing, including local HDF5 I/O

A separate benchmark times file opening, reading, photon expansion, matching,
confidence filtering and creation of a beam DataFrame. It reads `h_ph` plus the
parser's standard coordinates/IDs and uses the default land confidence threshold
of 3. Three repetitions follow one warm-up. It excludes TileDB writes, downloads,
full-granule concatenation and provenance string columns added by the granule parser.

| Mode | Median seconds | Peak RSS MiB | Retained photons |
|---|---:|---:|---:|
| Original confidence-only selection | 5.036 | 816.6 | 1,226 |
| Linked, also retaining ATL08 signal | 5.117 | 1022.5 | 6,594 |

Linkage added about 1.6% elapsed time
and 205.9 MiB peak RSS in this run. It retained
**5,368 additional ATL08 ground/canopy photons** below
the original ATL03 confidence threshold. This changes the selected population
intentionally; the two parsed DataFrames should not be identical.

An independent dictionary-based index calculation verified that every ATL08
signal photon appears at its original ATL03 index with the expected class and
relative height, and that the increased row count equals the number of additional
low-confidence signal photons. Verification happens outside the timing and after
the measured memory high-water mark is captured.

## Reproduction

Run from the repository root in an environment with the project dependencies:

```bash
PYTHONPATH=. python benchmarks/benchmark_atl03_atl08.py --output benchmarks/atl03_atl08_synthetic.json

PYTHONPATH=. python benchmarks/benchmark_atl03_atl08.py \
  --atl03 icesat2db/tests/data/ATL03_20181014001049_02350102_007_01.h5 \
  --atl08 icesat2db/tests/data/ATL08_20181014001049_02350102_007_01.h5 \
  --beam gt1l --repeats 5 --output benchmarks/atl03_atl08_real.json

PYTHONPATH=. python benchmarks/benchmark_atl03_atl08.py \
  --atl03 icesat2db/tests/data/ATL03_20181014001049_02350102_007_01.h5 \
  --atl08 icesat2db/tests/data/ATL08_20181014001049_02350102_007_01.h5 \
  --beam gt1l --ingestion --repeats 3 --output benchmarks/atl03_atl08_ingestion.json
```

The real HDF5 fixtures are local ignored files, not included in the change.
Synthetic runs require no external data. The benchmark uses Linux `ru_maxrss`
units when converting peak RSS to MiB.

## Validation

Regression coverage includes one-based index conversion, empty geolocation
segments, land-segment gaps/boundaries, missing beams/subgroups, noise versus
unclassified photons, invalid offsets and duplicate classifications, acquisition
and release checks, pre-filter matching, large packed IDs, query order/index
preservation, TileDB round trips and old-schema rejection. It also checks the
corrected ATL08 terrain fill-value filters and prevents distinct photons sharing
coordinates/time from being deduplicated in linked mode.

The initial full test run passed 279 tests; 13 existing tests needed access outside
the sandbox. An approved rerun passed 11 of those, including the NASA CMR query.
Two existing processor tests still require a missing Earthdata `.netrc` file:
`TestCorrectErrorHandlingOfConfigFile.test_report_every` and
`TestDownloadCmrDataExceptions.test_compute_raises_and_logs_exception`.
No credentials were created. Four more linkage regression tests were added after
that run. The final local regression run passed **283 tests** (the two existing
authentication-dependent test classes were deselected and the already-passed
NASA CMR test was excluded). Together with the approved rerun, 294 tests passed
across the runs. Focused Ruff checks and `git diff --check` also passed.

See the [configuration and query guide](../doc/user/atl03_atl08_linkage.md).
