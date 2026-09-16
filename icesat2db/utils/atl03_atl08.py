# SPDX-License-Identifier: EUPL-1.2

"""Exact ATL03/ATL08 linkage within a single acquisition and beam."""

from pathlib import Path

import numpy as np

from icesat2db.granule.granule_name import parse_granule_filename
from icesat2db.utils.segment_id import pack_segment_id


def linkage_variables(config, product):
    """Computed attributes enabled together with paired ingestion."""
    if not config.get("level_atl03", {}).get("link_atl08", False):
        return {}
    fields = {"source_granule": ("U80", "Source filename including release/revision")}
    if product == "atl03":
        fields.update(
            {
                "beam_id": ("U4", "Beam name in the source granule"),
                "photon_index": (
                    "int64",
                    "Zero-based original ATL03 beam photon index",
                ),
                "atl08_source_granule": (
                    "U80",
                    "Matched ATL08 filename including release/revision",
                ),
                "atl08_segment_id": (
                    "int64",
                    "Packed ATL08 land segment ID; -1 when unmatched",
                ),
                "atl08_classed_pc_flag": (
                    "int8",
                    "ATL08 class: -1 unmatched, 0 noise, 1 ground, 2 canopy, 3 canopy top",
                ),
                "atl08_ph_h": (
                    "float32",
                    "ATL08 height above ground; NaN when unavailable",
                ),
            }
        )
    return {
        name: {
            "SDS_Name": name,
            "dtype": dtype,
            "description": description,
            "long_name": name,
            "units": "meters" if name == "atl08_ph_h" else "adimensional",
        }
        for name, (dtype, description) in fields.items()
    }


def validate_pair(atl03_path, atl08_path):
    """Reject different acquisitions/releases; retain product-specific revisions."""
    a = parse_granule_filename(Path(atl03_path).name)
    b = parse_granule_filename(Path(atl08_path).name)
    keys = (
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "ref_ground_track",
        "cycle_number",
        "segment_number",
        "version",
    )
    if (
        a.product != "ATL03"
        or b.product != "ATL08"
        or any(getattr(a, key) != getattr(b, key) for key in keys)
    ):
        raise ValueError(
            "ATL03/ATL08 must share acquisition, track, cycle, region and release"
        )


def interval_indices(segment_ids, begins, ends):
    """Return land-segment row per geosegment, or -1 for gaps/outside coverage.

    Bounds must be sorted, inclusive and non-overlapping. Operate at geosegment
    rate, then expand the resulting links to photon rate only once.
    """
    segment_ids, begins, ends = map(np.asarray, (segment_ids, begins, ends))
    if (
        any(a.ndim != 1 for a in (segment_ids, begins, ends))
        or begins.shape != ends.shape
    ):
        raise ValueError(
            "Segment IDs and bounds must be one-dimensional with matching bounds"
        )
    if np.any(ends < begins) or np.any(begins[1:] <= ends[:-1]):
        raise ValueError(
            "ATL08 land segment intervals must be sorted and non-overlapping"
        )
    if not len(begins):
        return np.full(segment_ids.shape, -1, dtype=np.int64)
    rows = np.searchsorted(begins, segment_ids, side="right") - 1
    valid = (rows >= 0) & (segment_ids <= ends[np.maximum(rows, 0)])
    return np.where(valid, rows, -1)


def photon_indices(
    segment_ids, starts, counts, ph_segment_ids, classed_indices, n_photons
):
    """Map ATL08 signal records to original zero-based ATL03 photon indices.

    Missing geosegments (e.g. subset files) remain unmatched. Malformed matched
    offsets fail explicitly rather than labelling a neighbouring segment.
    """
    segment_ids, starts, counts, ph_segment_ids, classed_indices = (
        np.asarray(a, dtype=np.int64)
        for a in (segment_ids, starts, counts, ph_segment_ids, classed_indices)
    )
    if any(
        a.ndim != 1
        for a in (segment_ids, starts, counts, ph_segment_ids, classed_indices)
    ):
        raise ValueError("Photon linkage arrays must be one-dimensional")
    if (
        not (segment_ids.shape == starts.shape == counts.shape)
        or ph_segment_ids.shape != classed_indices.shape
    ):
        raise ValueError("Photon linkage array lengths differ")
    if np.any(np.diff(segment_ids) <= 0):
        raise ValueError("ATL03 segment IDs must be strictly increasing")
    result = np.full(len(ph_segment_ids), -1, dtype=np.int64)
    if not len(segment_ids):
        return result
    rows = np.searchsorted(segment_ids, ph_segment_ids)
    rows = np.minimum(rows, len(segment_ids) - 1)
    matched = segment_ids[rows] == ph_segment_ids
    rows = rows[matched]
    offsets = classed_indices[matched]
    indices = starts[rows] + offsets - 2
    if np.any(
        (starts[rows] <= 0)
        | (offsets <= 0)
        | (offsets > counts[rows])
        | (indices < 0)
        | (indices >= n_photons)
    ):
        raise ValueError("ATL08 photon index outside its ATL03 geolocation segment")
    # Product records normally follow photon order. Avoid sorting/hash tables
    # for that common case, while accepting unsorted records safely.
    if np.any(np.diff(indices) <= 0) and np.any(np.diff(np.sort(indices)) == 0):
        raise ValueError("Duplicate ATL08 classification for an ATL03 photon")
    result[matched] = indices
    return result


def link_beam(atl03, atl08, segment_ids, counts, n_photons, rgt, cycle, beam_id):
    """Read companion beam and return dense linkage fields before filtering."""
    result = {
        "photon_index": np.arange(n_photons, dtype=np.int64),
        "atl08_segment_id": np.full(n_photons, -1, dtype=np.int64),
        "atl08_classed_pc_flag": np.full(n_photons, -1, dtype=np.int8),
        "atl08_ph_h": np.full(n_photons, np.nan, dtype=np.float32),
    }
    starts = np.asarray(atl03["geolocation/ph_index_beg"][()], dtype=np.int64)
    # np.repeat is valid only for a complete contiguous photon layout.
    expected = np.cumsum(counts) - counts + 1
    if starts.shape != counts.shape or np.any(
        starts[counts > 0] != expected[counts > 0]
    ):
        raise ValueError("ATL03 photon layout is not contiguous")
    if atl08 is None:
        return result
    if "land_segments" in atl08:
        land = atl08["land_segments"]
        begins = land["segment_id_beg"][()]
        rows = interval_indices(segment_ids, begins, land["segment_id_end"][()])
        links = np.full(len(segment_ids), -1, dtype=np.int64)
        valid = rows >= 0
        links[valid] = pack_segment_id(rgt, cycle, beam_id, begins[rows[valid]])
        result["atl08_segment_id"] = np.repeat(links, counts)
    if "signal_photons" in atl08:
        signal = atl08["signal_photons"]
        indices = photon_indices(
            segment_ids,
            starts,
            counts,
            signal["ph_segment_id"][()],
            signal["classed_pc_indx"][()],
            n_photons,
        )
        flags = signal["classed_pc_flag"][()]
        heights = signal["ph_h"][()]
        if (
            flags.shape != indices.shape
            or heights.shape != indices.shape
            or np.any((flags < 0) | (flags > 3))
        ):
            raise ValueError("Invalid ATL08 signal photon fields")
        valid = indices >= 0
        result["atl08_classed_pc_flag"][indices[valid]] = flags[valid]
        heights = heights.astype(np.float32)
        heights[
            (~np.isfinite(heights)) | (np.abs(heights) >= np.finfo(np.float32).max)
        ] = np.nan
        result["atl08_ph_h"][indices[valid]] = heights[valid]
    return result
