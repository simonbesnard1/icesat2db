# SPDX-License-Identifier: EUPL-1.2

"""Regression tests for exact paired ingestion and subset joins."""

import h5py
import numpy as np
import pandas as pd
import pytest

from icesat2db.core.icesat2database import IceSat2Database
from icesat2db.core.icesat2granule import IceSat2Granule
from icesat2db.core.icesat2provider import IceSat2Provider
from icesat2db.granule.granule_parser import parse_h5_file
from icesat2db.utils.atl03_atl08 import interval_indices, photon_indices, validate_pair
from icesat2db.utils.constants import configured_products
from icesat2db.utils.segment_id import pack_segment_id


@pytest.fixture
def paired_files(tmp_path):
    a = tmp_path / "ATL03_20200101000000_00010101_007_01.h5"
    b = tmp_path / "ATL08_20200101000000_00010101_007_02.h5"
    for path in (a, b):
        with h5py.File(path, "w") as f:
            f["orbit_info/rgt"] = [1]
            f["orbit_info/cycle_number"] = [1]
    with h5py.File(a, "a") as f:
        for beam in ("gt1l", "gt2r"):
            base = f.create_group(beam)
            base["geolocation/segment_id"] = [100, 101, 102, 103, 104, 105, 106]
            base["geolocation/segment_ph_cnt"] = [2, 0, 2, 2, 2, 2, 2]
            base["geolocation/ph_index_beg"] = [1, 0, 3, 5, 7, 9, 11]
            base["heights/delta_time"] = np.arange(12, dtype=float)
            base["heights/lat_ph"] = np.linspace(50, 50.01, 12)
            base["heights/lon_ph"] = np.linspace(10, 10.01, 12)
            base["heights/h_ph"] = np.arange(12, dtype=float) + 20
            conf = np.zeros((12, 5), dtype=np.int8)
            conf[[0, 4, 10], 0] = 4
            base["heights/signal_conf_ph"] = conf
    with h5py.File(b, "a") as f:
        base = f.create_group("gt1l")
        base["land_segments/segment_id_beg"] = [100, 105]
        base["land_segments/segment_id_end"] = [103, 106]
        base["land_segments/delta_time"] = [2.0, 10.0]
        base["land_segments/latitude"] = [50.0, 50.01]
        base["land_segments/longitude"] = [10.0, 10.01]
        for key in ("h_te_uncertainty", "h_te_best_fit", "h_te_median"):
            base[f"land_segments/terrain/{key}"] = [1.0, 1.0]
        base["land_segments/canopy/h_canopy"] = [10.0, 20.0]
        base["land_segments/canopy/h_canopy_uncertainty"] = [1.0, 1.0]
        base["land_segments/urban_flag"] = [0, 0]
        base["land_segments/segment_watermask"] = [0, 0]
        # Unsorted ATL08 records; one segment absent from ATL03.
        base["signal_photons/ph_segment_id"] = [106, 100, 102, 104, 999]
        base["signal_photons/classed_pc_indx"] = [2, 2, 1, 1, 1]
        base["signal_photons/classed_pc_flag"] = [3, 1, 0, 2, 1]
        base["signal_photons/ph_h"] = [9.0, 0.0, 7.0, 4.0, 0.0]
    return a, b


@pytest.fixture
def linked_config():
    return {
        "level_atl03": {
            "link_atl08": True,
            "variables": {
                "h_ph": {"SDS_Name": "heights/h_ph", "dtype": "float32"},
                "segment_id": {"SDS_Name": "segment_id", "dtype": "int64"},
            },
        },
        "level_atl08": {
            "variables": {
                "h_canopy": {
                    "SDS_Name": "land_segments/canopy/h_canopy",
                    "dtype": "float32",
                },
                "segment_id": {"SDS_Name": "segment_id", "dtype": "int64"},
            }
        },
    }


def test_intervals_boundaries_gaps_and_empty():
    np.testing.assert_array_equal(
        interval_indices([99, 100, 103, 104, 105, 106, 107], [100, 105], [103, 106]),
        [-1, 0, 0, -1, 1, 1, -1],
    )
    np.testing.assert_array_equal(interval_indices([100], [], []), [-1])
    for begins, ends in (([2, 1], [2, 1]), ([1, 2], [2, 3]), ([2], [1])):
        with pytest.raises(ValueError):
            interval_indices([1], begins, ends)


def test_photon_offsets_missing_zero_count_and_duplicates():
    args = ([100, 101, 102], [1, 0, 3], [2, 0, 2])
    np.testing.assert_array_equal(
        photon_indices(*args, [102, 100, 999], [2, 1, 1], 4), [3, 0, -1]
    )
    for segs, offsets in (
        ([100], [0]),
        ([100], [3]),
        ([101], [1]),
        ([100, 100], [1, 1]),
    ):
        with pytest.raises(ValueError):
            photon_indices(*args, segs, offsets, 4)


def test_pair_rejects_wrong_release_acquisition_product(paired_files):
    a, b = paired_files
    validate_pair(a, b)  # revisions may differ
    for bad in (
        str(b).replace("007_02", "006_02"),
        str(b).replace("00010101", "00020101"),
        a,
    ):
        with pytest.raises(ValueError):
            validate_pair(a, bad)


def test_paired_ingestion_before_filter_and_cross_beam(paired_files, linked_config):
    a, b = paired_files
    data = parse_h5_file(a, "atl03", linked_config, atl08_file=b)
    beam = data.loc[data.beam_id == "gt1l"]
    np.testing.assert_array_equal(beam.photon_index, [0, 1, 4, 6, 10, 11])
    np.testing.assert_array_equal(beam.atl08_classed_pc_flag, [-1, 1, -1, 2, -1, 3])
    np.testing.assert_allclose(beam.atl08_ph_h, [np.nan, 0, np.nan, 4, np.nan, 9])
    ids = pack_segment_id(1, 1, 0, np.array([100, 100, 100, 100, 105, 105]))
    ids[3] = -1  # Classified signal photon in a land-segment gap.
    np.testing.assert_array_equal(beam.atl08_segment_id, ids)
    other = data.loc[data.beam_id == "gt2r"]
    np.testing.assert_array_equal(other.photon_index, [0, 4, 10])
    assert (other.atl08_classed_pc_flag == -1).all()
    assert (other.atl08_segment_id == -1).all()
    assert (data.source_granule == a.name).all()
    assert (data.atl08_source_granule == b.name).all()
    linked_config["level_atl03"]["retain_atl08_signal"] = False
    strict = parse_h5_file(a, "atl03", linked_config, atl08_file=b)
    np.testing.assert_array_equal(strict.photon_index, [0, 4, 10, 0, 4, 10])


def test_pair_required_and_invalid_layout(paired_files, linked_config):
    a, b = paired_files
    with pytest.raises(ValueError, match="matching ATL08"):
        parse_h5_file(a, "atl03", linked_config)
    with h5py.File(a, "a") as f:
        f["gt1l/geolocation/ph_index_beg"][2] = 4
    with pytest.raises(ValueError, match="contiguous"):
        parse_h5_file(a, "atl03", linked_config, atl08_file=b)
    with pytest.raises(ValueError, match="both"):
        configured_products({"level_atl03": {"link_atl08": True}})


def test_pipeline_and_provider_join(paired_files, linked_config, tmp_path):
    a, b = paired_files
    pipeline = IceSat2Granule(str(tmp_path / "download"), linked_config)
    parsed = pipeline.parse_granules([("atl03", str(a)), ("atl08", str(b))], "pair")
    photons = parsed["atl03"]
    combined = IceSat2Provider.combine_photons(photons, parsed["atl08"], ["h_canopy"])
    assert len(combined) == len(photons)
    np.testing.assert_allclose(
        combined.atl08_h_canopy, [10, 10, 10, np.nan, 20, 20, np.nan, np.nan, np.nan]
    )
    wrong_release = parsed["atl08"].copy()
    wrong_release["source_granule"] = "other_release.h5"
    assert (
        IceSat2Provider.combine_photons(photons, wrong_release, ["h_canopy"])
        .atl08_h_canopy.isna()
        .all()
    )
    with pytest.raises(pd.errors.MergeError):
        IceSat2Provider.combine_photons(
            photons, pd.concat([parsed["atl08"]] * 2), ["h_canopy"]
        )


@pytest.mark.parametrize("field", ["h_te_uncertainty", "h_te_best_fit", "h_te_median"])
@pytest.mark.parametrize("invalid", [-999.0, np.finfo(np.float32).max, np.nan])
def test_terrain_validity_filter(paired_files, linked_config, field, invalid):
    _, b = paired_files
    with h5py.File(b, "a") as f:
        f[f"gt1l/land_segments/terrain/{field}"][0] = invalid
    data = parse_h5_file(b, "atl08", linked_config)
    assert len(data) == 1
    assert data.h_canopy.iloc[0] == 20


def test_tiledb_roundtrip_and_old_schema_guard(paired_files, linked_config, tmp_path):
    import tiledb

    # Keep the test's contexts small on machines exposing many CPU cores.
    tiledb.default_ctx(
        {"sm.compute_concurrency_level": "2", "sm.io_concurrency_level": "2"}
    )
    linked_config["tiledb"] = {
        "storage_type": "local",
        "local_path": str(tmp_path / "db"),
        "dimensions": ["latitude", "longitude", "time"],
        "spatial_range": {
            "lat_min": -90,
            "lat_max": 90,
            "lon_min": -180,
            "lon_max": 180,
        },
        "time_range": {"start_time": "2018-01-01", "end_time": "2030-01-01"},
    }
    a, b = paired_files
    for product, path in (("atl03", a), ("atl08", b)):
        db = IceSat2Database(linked_config, product=product)
        db._create_arrays()
        data = parse_h5_file(path, product, linked_config, atl08_file=b)
        db.write_granule(data)
        with tiledb.open(db.array_uri, ctx=db.ctx) as array:
            stored = array[:]
        assert len(stored["source_granule"]) == len(data)
        assert set(stored["source_granule"]) == {path.name}
        if product == "atl03":
            order = np.argsort(stored["photon_index"], kind="stable")
            assert stored["atl08_classed_pc_flag"].dtype == np.int8
            assert sorted(stored["atl08_segment_id"].tolist()) == sorted(
                data.atl08_segment_id.tolist()
            )
            assert len(order) == len(data)
            with pytest.raises(ValueError, match="Missing linkage"):
                db._extract_variable_data(data.drop(columns="photon_index"))
    linked_config["tiledb"]["local_path"] = str(tmp_path / "old_db")
    linked_config["level_atl03"]["link_atl08"] = False
    old = IceSat2Database(linked_config, product="atl03")
    old._create_arrays()
    linked_config["level_atl03"]["link_atl08"] = True
    with pytest.raises(ValueError, match="Re-ingest"):
        IceSat2Database(linked_config, product="atl03")._create_arrays()


def test_noise_class_is_not_unclassified(paired_files, linked_config):
    a, b = paired_files
    with h5py.File(a, "a") as f:
        f["gt1l/heights/signal_conf_ph"][2, 0] = 4
    data = parse_h5_file(a, "atl03", linked_config, atl08_file=b)
    noise = data.loc[(data.beam_id == "gt1l") & (data.photon_index == 2)]
    assert noise.atl08_classed_pc_flag.iloc[0] == 0
    assert noise.atl08_ph_h.iloc[0] == 7


def test_no_signal_subgroup_keeps_links(paired_files, linked_config):
    a, b = paired_files
    with h5py.File(b, "a") as f:
        del f["gt1l/signal_photons"]
    data = parse_h5_file(a, "atl03", linked_config, atl08_file=b)
    assert (data.atl08_classed_pc_flag == -1).all()
    assert data.atl08_ph_h.isna().all()
    assert (data.loc[data.beam_id == "gt1l", "atl08_segment_id"] >= 0).all()


def test_join_preserves_large_ids_order_and_index():
    ids = pack_segment_id(1387, 5, 3, np.array([100, 101]))
    photons = pd.DataFrame(
        {
            "atl08_segment_id": [ids[1], -1, ids[0]],
            "atl08_source_granule": ["a", "a", "a"],
        },
        index=[9, 9, 1],
    )
    parents = pd.DataFrame(
        {"segment_id": ids, "source_granule": ["a", "a"], "h": [10.0, 20.0]}
    )
    result = IceSat2Provider.combine_photons(photons, parents, ["h"])
    np.testing.assert_allclose(result.atl08_h, [20.0, np.nan, 10.0])
    pd.testing.assert_index_equal(result.index, photons.index)
    np.testing.assert_array_equal(result.atl08_segment_id, photons.atl08_segment_id)


def test_randomized_mapping_against_dictionary_oracle():
    rng = np.random.default_rng(713)
    counts = rng.integers(0, 8, 100)
    ids = np.arange(100, 200)
    starts = np.cumsum(counts) - counts + 1
    starts[counts == 0] = 0
    selected = rng.permutation(counts.sum())[::3]
    photon_segments = np.repeat(ids, counts)
    seg_lookup = dict(zip(ids, starts))
    segments = photon_segments[selected]
    offsets = np.array([i - seg_lookup[s] + 2 for i, s in zip(selected, segments)])
    np.testing.assert_array_equal(
        photon_indices(ids, starts, counts, segments, offsets, counts.sum()), selected
    )
    begins = ids[::7]
    ends = begins + 4
    expected = [
        next((j for j, (a, b) in enumerate(zip(begins, ends)) if a <= s <= b), -1)
        for s in ids
    ]
    np.testing.assert_array_equal(interval_indices(ids, begins, ends), expected)
