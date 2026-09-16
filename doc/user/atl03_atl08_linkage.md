# Combining ATL03 and ATL08

Enable paired ingestion in a configuration containing both `level_atl03` and
`level_atl08`:

```yaml
level_atl03:
  link_atl08: true
  retain_atl08_signal: true
  confidence_column: 0
  confidence_threshold: 3
  variables:
    # Keep your existing ATL03 variable definitions here.
```

The shipped configuration leaves `link_atl08: false` for compatibility. Use a
**new TileDB storage location and re-ingest both products** when enabling it.
Existing arrays without linkage attributes are rejected with an actionable error;
no schema migration or changes to existing archives happen automatically.

The processor passes the matching ATL08 file into the ATL03 parser before its
usual cleanup of downloaded files. A missing companion is an error in linked
mode. Filenames must identify the same acquisition time, track, cycle, region and
release; product-specific revisions can differ and are preserved. Orbit metadata
must also agree. Direct parsing uses:

```python
from icesat2db.granule.granule_parser import parse_h5_file

photons = parse_h5_file(
    atl03_path, "atl03", config, atl08_file=atl08_path
)
segments = parse_h5_file(atl08_path, "atl08", config)
```

## Stored fields and filtering

Linked ingestion automatically adds these attributes to the ATL03 schema:

| Field | Meaning |
|---|---|
| `source_granule` | Original ATL03 filename, including release and revision |
| `beam_id` | Beam name in the original granule |
| `photon_index` | Original zero-based photon index within the beam |
| `atl08_source_granule` | Companion ATL08 filename |
| `atl08_segment_id` | Packed parent ATL08 land-segment ID, or `-1` |
| `atl08_classed_pc_flag` | `-1` unmatched, `0` noise, `1` ground, `2` canopy, `3` canopy top |
| `atl08_ph_h` | ATL08 height above ground, or `NaN` |

ATL08 also stores `source_granule`. The existing ATL03 `segment_id` remains the
packed **geolocation** segment ID; it does not become an ATL08 land-segment ID.

Photon classification uses `ph_segment_id` and the original one-based indices:
`ph_index_beg + classed_pc_indx - 2`. Land-segment association uses inclusive
`segment_id_beg`/`segment_id_end` bounds at geolocation-segment rate, then expands
the links to photons. Gaps stay unmatched; overlapping/unsorted intervals,
duplicate classifications and out-of-bounds matched photon offsets raise errors.
Empty geolocation segments are supported. Missing ATL08 beam/subgroups leave the
corresponding fields unmatched. The contiguous ATL03 layout is validated before
expansion.

By default, linked ingestion keeps photons passing the ATL03 confidence filter
**or** classified as ground/canopy/canopy-top by ATL08. Set
`retain_atl08_signal: false` to retain the original confidence-only selection while
still adding links. Matching always uses the original indices before filtering.
ATL08 noise (`0`) does not automatically bypass the confidence filter.

ATL08 land-segment quality filtering remains independent of photon classification.
A photon can retain a link to a parent filtered out of the stored ATL08 array.
The terrain validity filters now correctly require both lower and upper bounds;
this can reduce the number of ingested ATL08 rows compared with the previous code.

## Querying combined measurements

Query each product with its own `IceSat2Provider` and `return_type="dataframe"`.
Request `atl08_segment_id` and `atl08_source_granule` with the photon measurements,
and `source_granule` with the ATL08 measurements. The normal provider also returns
`segment_id`. Then combine the selected subsets:

```python
from icesat2db import IceSat2Provider

combined = IceSat2Provider.combine_photons(
    photon_subset, segment_subset, variables=["h_canopy", "h_te_best_fit"]
)
# Additional columns: atl08_h_canopy, atl08_h_te_best_fit
```

This is a left join on parent ID **and exact ATL08 filename**. Photon order and
index are preserved, including repeated indices. Missing parents produce missing
measurements. Duplicate parent keys raise rather than multiplying photons.

Fetch enough ATL08 coverage to include parent segment centres outside the photon
query polygon. Applying an identical tight polygon to both products can exclude
those centres. The helper operates on supplied DataFrames; it does not perform
additional database queries automatically.

Benchmark results and reproducible commands are in
[the benchmark report](../../benchmarks/README.md).
