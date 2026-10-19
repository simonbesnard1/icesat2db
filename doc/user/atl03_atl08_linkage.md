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
An existing array built with different settings (including a different
`link_atl08`, `confidence_column` or `confidence_threshold`) is refused on
resume; no schema migration or changes to existing archives happen automatically.

If ATL08 of a granule is already stored but its ATL03 write did not complete,
the next run downloads the ATL08 file again only to link the photons; ATL08 is
not written twice. ATL03 granules without any ATL08 file cannot be linked and are
skipped with a warning.

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

Every ATL03 array, linked or not, also stores `photon_id`: RGT, cycle, beam,
geolocation segment and the photon's offset within that segment packed into an
int64 (see `icesat2db.utils.segment_id.pack_photon_id`). It identifies a photon
independently of the granule file and is the key used to deduplicate replays.
Photons of one pulse share time and nearly share position, so coordinates are
never used for deduplication.

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

ATL08 ingestion keeps every land segment with a valid terrain height, including
terrain-only, water and urban segments (see the quality filtering guide), so
photon parents are rarely missing from the stored ATL08 array. Note that ATL08
reports only the 100 m segments meeting its own criteria: classified photons in
gaps between reported segments keep `atl08_segment_id = -1`.

## Querying combined measurements

Use `get_linked_photons` on the ATL03 provider with the ATL08 provider of the
same database:

```python
from icesat2db import IceSat2Provider

photons = IceSat2Provider(storage_type="local", local_path=db, product="atl03")
segments = IceSat2Provider(storage_type="local", local_path=db, product="atl08")

combined = photons.get_linked_photons(
    segments,
    photon_variables=["h_ph", "atl08_classed_pc_flag", "atl08_ph_h"],
    segment_variables=["h_canopy", "h_te_best_fit"],
    geometry=area,
    start_time="2020-01-01",
    end_time="2020-12-31",
    segment_filters={"segment_watermask": "== 0", "h_canopy": ">= 0"},
    atl08_classed_pc_flag="in [1, 2, 3]",  # ATL03 query-time filter
)
# Additional columns: atl08_h_canopy, atl08_h_te_best_fit
```

A parent segment centre can lie up to ~70 m from its photons, outside the photon
area. The ATL08 query is therefore widened by `buffer_m` (default 150 m) and by one
day; the exact key join means the wider query never attaches a wrong parent.
Photons whose parent fails `segment_filters` keep NaN ATL08 values.

The join itself is `IceSat2Provider.combine_photons`, also usable on DataFrames you
queried yourself: a left join on parent ID **and exact ATL08 filename**. Photon
order and index are preserved, including repeated indices. Missing parents produce
missing measurements. Duplicate parent keys raise rather than multiplying photons.

Benchmark results and reproducible commands are in
[the benchmark report](../../benchmarks/README.md).
