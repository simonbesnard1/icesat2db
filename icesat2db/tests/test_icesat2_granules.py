# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import os
import pathlib
import tempfile
import unittest
import warnings

import h5py
import pandas as pd
import yaml

from icesat2db.core.icesat2granule import IceSat2Granule
from icesat2db.granule.granule_parser import parse_h5_file
from icesat2db.utils.constants import IceSat2Product

# THIS_DIR = pathlib.Path(__name__).parent
THIS_DIR = pathlib.Path.cwd().parent
ATL08_NAME = "./data/ATL08_20181014001049_02350102_007_01.h5"


class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        warnings.simplefilter("ignore", DeprecationWarning)
        os.chdir(os.path.dirname(__file__))

    _data_info = {
        "level_atl08": {
            "variables": {
                "segment_id": {
                    "SDS_Name": "segment_id",
                },
                "latitude_20m": {
                    "SDS_Name": "land_segments/latitude_20m",
                },
                "longitude_20m": {
                    "SDS_Name": "land_segments/longitude_20m",
                },
                "canopy_openness": {
                    "SDS_Name": "land_segments/canopy/canopy_openness",
                },
            },
        }
    }

    def _generic_test_parse_granule(self, file, data):
        import h5py

        beams = ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]

        with h5py.File(file, "r") as f:
            present_beams = [b for b in beams if b in f]

            # all beams present
            self.assertEqual(len(present_beams), 6)

            for beam in present_beams:
                hdf_len = len(f[beam]["land_segments"]["latitude_20m"])

                # parsed data must not exceed original size
                self.assertLessEqual(
                    len(data),
                    hdf_len * len(present_beams),  # rough upper bound
                    "Parsed data larger than previous",
                )

                # basic sanity: original beam not empty
                self.assertGreater(hdf_len, 0)

    def test_parse_granule_atl08(self):
        data = parse_h5_file(
            ATL08_NAME,
            IceSat2Product.ATL08.value,
            data_info=self._data_info,
        )
        self._generic_test_parse_granule(ATL08_NAME, data)
        # Some of the data is correct
        data_orig = h5py.File(ATL08_NAME, "r")
        # original ATL08 values (first segment, first 20m bin)
        lat = data_orig["gt1l"]["land_segments"]["latitude_20m"][0][0]
        lon = data_orig["gt1l"]["land_segments"]["longitude_20m"][0][0]

        # match against flattened column
        row = data.loc[data["latitude_20m_1"] == lat]

        self.assertFalse(row.empty, "No matching latitude found in parsed data")

        self.assertEqual(
            row["longitude_20m_1"].values[0],
            lon,
        )


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)


class TestIceSat2Granule(unittest.TestCase):
    """Tests for IceSat2Granule: _join_dfs, parse_granules, process_granule."""

    @classmethod
    def setUpClass(cls):
        os.chdir(os.path.dirname(__file__))
        cls.atl08_path = str(
            pathlib.Path(__file__).parent
            / "data"
            / "ATL08_20181014001049_02350102_007_01.h5"
        )
        cls.temp_dir = tempfile.TemporaryDirectory()
        with open("data/data_config.yml", "r") as f:
            cls.data_info = yaml.safe_load(f)
        cls.granule = IceSat2Granule(cls.temp_dir.name, cls.data_info)

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    # ── _join_dfs ─────────────────────────────────────────────────────────────

    def test_join_dfs_returns_none_when_atl08_key_missing(self):
        """Empty dict → no ATL08 entry → None."""
        self.assertIsNone(IceSat2Granule._join_dfs({}, "key"))

    def test_join_dfs_returns_none_for_empty_dataframe(self):
        self.assertIsNone(
            IceSat2Granule._join_dfs(
                {IceSat2Product.ATL08.value: pd.DataFrame()}, "key"
            )
        )

    def test_join_dfs_returns_none_when_segment_id_missing(self):
        df = pd.DataFrame({"latitude": [1.0, 2.0], "longitude": [-50.0, -51.0]})
        self.assertIsNone(
            IceSat2Granule._join_dfs({IceSat2Product.ATL08.value: df}, "key")
        )

    def test_join_dfs_returns_dataframe_when_valid(self):
        df = pd.DataFrame({"segment_id": [1, 2], "latitude": [1.0, 2.0]})
        result = IceSat2Granule._join_dfs({IceSat2Product.ATL08.value: df}, "key")
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        self.assertIn("segment_id", result.columns)

    # ── parse_granules ────────────────────────────────────────────────────────

    def test_parse_granules_nonexistent_file_returns_empty(self):
        granules = [(IceSat2Product.ATL08.value, "/nonexistent/file.h5")]
        result = self.granule.parse_granules(granules, "key_001")
        self.assertEqual(result, {})

    def test_parse_granules_valid_atl08_returns_dataframe(self):
        granules = [(IceSat2Product.ATL08.value, self.atl08_path)]
        result = self.granule.parse_granules(granules, "key_001")
        self.assertIn(IceSat2Product.ATL08.value, result)
        df = result[IceSat2Product.ATL08.value]
        self.assertFalse(df.empty)
        self.assertIn("segment_id", df.columns)

    # ── process_granule ───────────────────────────────────────────────────────

    def test_process_granule_missing_product_returns_none_tuple(self):
        """A None filepath signals a missing download → (None, None)."""
        row = [("key_002", ("ATL08", None))]
        granule_id, result = self.granule.process_granule(row)
        self.assertIsNone(granule_id)
        self.assertIsNone(result)

    def test_process_granule_valid_atl08_returns_dataframe(self):
        granule_key = "ATL08_20181014001049_02350102_007_01"
        row = [(granule_key, (IceSat2Product.ATL08.value, self.atl08_path))]
        granule_id, result = self.granule.process_granule(row)
        self.assertIsNotNone(granule_id)
        self.assertIsNotNone(result)
        self.assertFalse(result.empty)
        self.assertIn("segment_id", result.columns)

    def test_process_granule_bad_file_returns_none_dataframe(self):
        """An unreadable file path causes parse failure → (granule_key, None).
        parse_granules catches the exception and returns {}, so process_granule
        returns (granule_key, None) rather than (None, None)."""
        row = [("key_003", (IceSat2Product.ATL08.value, "/bad/path.h5"))]
        granule_id, result = self.granule.process_granule(row)
        self.assertEqual(granule_id, "key_003")
        self.assertIsNone(result)
