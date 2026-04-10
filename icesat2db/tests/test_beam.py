# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import os
import pathlib
import unittest
import warnings

import h5py
import numpy as np
import pandas as pd

from icesat2db.beam.Beam import beam_handler
from icesat2db.beam.atl08_beam import ATL08Beam

# Resolve path to the test H5 file (same pattern as test_icesat2_granules.py)
THIS_DIR = pathlib.Path(__file__).parent
ATL08_H5 = THIS_DIR / "data" / "ATL08_20181014001049_02350102_007_01.h5"

# Minimal field mapping sufficient for ATL08Beam construction
MINIMAL_FIELD_MAPPING = {
    "h_canopy": {"SDS_Name": "land_segments/canopy/h_canopy"},
    "h_canopy_uncertainty": {"SDS_Name": "land_segments/canopy/h_canopy_uncertainty"},
    "h_te_best_fit": {"SDS_Name": "land_segments/terrain/h_te_best_fit"},
    "h_te_uncertainty": {"SDS_Name": "land_segments/terrain/h_te_uncertainty"},
    "h_te_median": {"SDS_Name": "land_segments/terrain/h_te_median"},
    "urban_flag": {"SDS_Name": "land_segments/urban_flag"},
    "segment_watermask": {"SDS_Name": "land_segments/segment_watermask"},
    "segment_id_beg": {"SDS_Name": "land_segments/segment_id_beg"},
    "segment_id_end": {"SDS_Name": "land_segments/segment_id_end"},
}


def _first_valid_beam(h5file, field_mapping):
    """Return the first beam that has land_segments."""
    for beam_name in ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]:
        if beam_name in h5file and "land_segments" in h5file[beam_name]:
            return ATL08Beam(h5file, beam_name, field_mapping)
    return None


# ---------------------------------------------------------------------------
# beam_handler.apply_filter tests (pure Python, no H5 needed)
# ---------------------------------------------------------------------------


class TestBeamHandlerApplyFilter(unittest.TestCase):

    def _make_data(self, n=10):
        return {"segment_id": np.arange(n, dtype=np.int64)}

    def test_no_filters_returns_all_true(self):
        data = self._make_data(8)
        mask = beam_handler.apply_filter(data, filters=None)
        self.assertEqual(len(mask), 8)
        self.assertTrue(np.all(mask))

    def test_empty_filters_dict_returns_all_true(self):
        data = self._make_data(5)
        mask = beam_handler.apply_filter(data, filters={})
        self.assertTrue(np.all(mask))

    def test_filter_returning_all_true(self):
        data = self._make_data(6)
        filters = {"always_true": lambda: np.ones(6, dtype=bool)}
        mask = beam_handler.apply_filter(data, filters=filters)
        self.assertTrue(np.all(mask))

    def test_filter_returning_partial_mask(self):
        n = 10
        data = self._make_data(n)
        keep = np.array([True, False] * 5)
        filters = {"alternating": lambda: keep}
        mask = beam_handler.apply_filter(data, filters=filters)
        np.testing.assert_array_equal(mask, keep)

    def test_filter_with_keyerror_skips_gracefully(self):
        data = self._make_data(4)

        def raises_key():
            raise KeyError("missing_field")

        filters = {"bad_filter": raises_key}
        # Should not raise; should return all-True because the error is swallowed
        mask = beam_handler.apply_filter(data, filters=filters)
        self.assertTrue(np.all(mask))

    def test_filter_with_generic_exception_skips_gracefully(self):
        data = self._make_data(4)

        def raises_generic():
            raise RuntimeError("unexpected")

        filters = {"bad_filter": raises_generic}
        mask = beam_handler.apply_filter(data, filters=filters)
        self.assertTrue(np.all(mask))

    def test_multiple_filters_combined_with_and(self):
        n = 6
        data = self._make_data(n)
        # First filter keeps indices 0,1,2,3; second keeps 2,3,4,5
        f1 = np.array([True, True, True, True, False, False])
        f2 = np.array([False, False, True, True, True, True])
        filters = {
            "f1": lambda: f1,
            "f2": lambda: f2,
        }
        mask = beam_handler.apply_filter(data, filters=filters)
        expected = f1 & f2
        np.testing.assert_array_equal(mask, expected)


# ---------------------------------------------------------------------------
# ATL08Beam tests using the real test H5 file
# ---------------------------------------------------------------------------


@unittest.skipUnless(ATL08_H5.exists(), f"Test H5 not found: {ATL08_H5}")
class TestATL08Beam(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.simplefilter("ignore", DeprecationWarning)
        cls.h5file = h5py.File(str(ATL08_H5), "r")
        cls.beam = _first_valid_beam(cls.h5file, MINIMAL_FIELD_MAPPING)
        if cls.beam is None:
            raise unittest.SkipTest("No beam with land_segments found in test H5")

    @classmethod
    def tearDownClass(cls):
        cls.h5file.close()

    # --- construct_segment_id ---

    def test_construct_segment_id_returns_int64_array(self):
        seg_ids = self.beam.construct_segment_id()
        self.assertEqual(seg_ids.dtype, np.int64)

    def test_construct_segment_id_length_matches_land_segments(self):
        seg_ids = self.beam.construct_segment_id()
        n_segs = len(self.beam["land_segments/segment_id_beg"])
        self.assertEqual(len(seg_ids), n_segs)

    def test_construct_segment_id_unique(self):
        seg_ids = self.beam.construct_segment_id()
        self.assertEqual(len(seg_ids), len(np.unique(seg_ids)))

    def test_construct_segment_id_encodes_rgt(self):
        rgt = int(self.beam.parent_granule["orbit_info/rgt"][0])
        seg_ids = self.beam.construct_segment_id()
        # RGT is in bits [63..43], so right-shift 43 should recover it
        rgt_decoded = (seg_ids[0] >> 43) & 0x7FF  # 11-bit mask
        self.assertEqual(int(rgt_decoded), rgt)

    # --- _get_main_data / main_data ---

    def test_main_data_returns_dataframe_or_none(self):
        result = self.beam.main_data
        self.assertTrue(result is None or isinstance(result, pd.DataFrame))

    def test_main_data_not_none_for_valid_beam(self):
        # The test H5 has land data, so main_data should not be None
        self.assertIsNotNone(self.beam.main_data)

    def test_main_data_has_required_columns(self):
        df = self.beam.main_data
        for col in ["latitude", "longitude", "time", "segment_id"]:
            self.assertIn(col, df.columns)

    def test_main_data_latitude_in_valid_range(self):
        df = self.beam.main_data
        self.assertTrue((df["latitude"] >= -90).all())
        self.assertTrue((df["latitude"] <= 90).all())

    def test_main_data_longitude_in_valid_range(self):
        df = self.beam.main_data
        self.assertTrue((df["longitude"] >= -180).all())
        self.assertTrue((df["longitude"] <= 180).all())

    def test_main_data_time_is_datetime(self):
        df = self.beam.main_data
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df["time"]))

    def test_main_data_caches_result(self):
        # Calling main_data twice should return the same object
        first = self.beam.main_data
        second = self.beam.main_data
        self.assertIs(first, second)

    def test_main_data_no_fill_values_in_h_canopy(self):
        df = self.beam.main_data
        if "h_canopy" in df.columns:
            fill = np.float32(3.4028235e38)
            self.assertTrue((df["h_canopy"] < fill).all())

    def test_main_data_segment_id_is_int64(self):
        df = self.beam.main_data
        self.assertEqual(df["segment_id"].dtype, np.int64)

    # --- beam with no land_segments ---

    def test_beam_without_land_segments_returns_none(self):
        """A beam group that has no land_segments sub-group should return None."""
        # Create a mock h5py-like object using a real beam group and patching
        # 'land_segments' out of its keys — simplest approach: use a temp HDF5
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            with h5py.File(tmp_path, "w") as f:
                # Create a minimal beam group WITHOUT land_segments
                beam_grp = f.create_group("gt1l")
                beam_grp.attrs["description"] = "Strong beam"
                f.create_group("orbit_info")
                f["orbit_info"].create_dataset("rgt", data=np.array([235]))
                f["orbit_info"].create_dataset("cycle_number", data=np.array([1]))

            with h5py.File(tmp_path, "r") as f:
                beam = ATL08Beam(f, "gt1l", MINIMAL_FIELD_MAPPING)
                result = beam._get_main_data()
            self.assertIsNone(result)
        finally:
            os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# ATL08Beam segment_id encoding correctness
# ---------------------------------------------------------------------------


class TestATL08BeamSegmentIdEncoding(unittest.TestCase):
    """Verify the bit-packing formula matches the documented layout."""

    @unittest.skipUnless(ATL08_H5.exists(), f"Test H5 not found: {ATL08_H5}")
    def test_segment_id_decodes_cycle(self):
        with h5py.File(str(ATL08_H5), "r") as f:
            beam = _first_valid_beam(f, MINIMAL_FIELD_MAPPING)
            if beam is None:
                self.skipTest("No valid beam in test H5")
            cycle = int(f["orbit_info/cycle_number"][0])
            seg_ids = beam.construct_segment_id()
            cycle_decoded = (seg_ids[0] >> 35) & 0xFF  # 8-bit mask
            self.assertEqual(int(cycle_decoded), cycle)

    @unittest.skipUnless(ATL08_H5.exists(), f"Test H5 not found: {ATL08_H5}")
    def test_segment_id_decodes_segment_id_beg(self):
        with h5py.File(str(ATL08_H5), "r") as f:
            beam = _first_valid_beam(f, MINIMAL_FIELD_MAPPING)
            if beam is None:
                self.skipTest("No valid beam in test H5")
            seg_id_beg = int(f[beam.beam_name]["land_segments/segment_id_beg"][0])
            seg_ids = beam.construct_segment_id()
            low_bits = int(seg_ids[0]) & 0xFFFFFFFF  # lower 32 bits
            self.assertEqual(low_bits, seg_id_beg)


if __name__ == "__main__":
    unittest.main()
