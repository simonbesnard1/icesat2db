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
import numpy as np
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

            total = 0
            for beam in present_beams:
                hdf_len = len(f[beam]["land_segments"]["latitude_20m"])
                # basic sanity: original beam not empty
                self.assertGreater(hdf_len, 0)
                total += hdf_len

            # parsed data must not exceed the segments in the file
            self.assertLessEqual(len(data), total, "Parsed data larger than file")

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
        # first valid 20 m bin: fill values are stored as NaN
        lats = data_orig["gt1l"]["land_segments"]["latitude_20m"][:, 0]
        first = int(np.argmax(lats < np.float32(3.4028235e38)))
        lat = lats[first]
        lon = data_orig["gt1l"]["land_segments"]["longitude_20m"][first][0]

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

    # ── _validate_dfs ─────────────────────────────────────────────────────────
    # Each product is validated independently and written to its own TileDB
    # array — there's no cross-product join, just a per-product sanity check.

    def test_validate_dfs_returns_empty_dict_for_empty_input(self):
        self.assertEqual(IceSat2Granule._validate_dfs({}, "key"), {})

    def test_validate_dfs_drops_empty_dataframe(self):
        result = IceSat2Granule._validate_dfs(
            {IceSat2Product.ATL08.value: pd.DataFrame()}, "key"
        )
        self.assertEqual(result, {})

    def test_validate_dfs_rejects_dataframe_missing_segment_id(self):
        """Records without their id are an error, not silently dropped data."""
        df = pd.DataFrame({"latitude": [1.0, 2.0], "longitude": [-50.0, -51.0]})
        with self.assertRaisesRegex(ValueError, "segment_id"):
            IceSat2Granule._validate_dfs({IceSat2Product.ATL08.value: df}, "key")

    def test_validate_dfs_rejects_atl03_without_photon_id(self):
        df = pd.DataFrame({"segment_id": [1], "latitude": [1.0]})
        with self.assertRaisesRegex(ValueError, "photon_id"):
            IceSat2Granule._validate_dfs({IceSat2Product.ATL03.value: df}, "key")

    def test_validate_dfs_keeps_valid_dataframe(self):
        df = pd.DataFrame({"segment_id": [1, 2], "latitude": [1.0, 2.0]})
        result = IceSat2Granule._validate_dfs({IceSat2Product.ATL08.value: df}, "key")
        self.assertIn(IceSat2Product.ATL08.value, result)
        self.assertEqual(len(result[IceSat2Product.ATL08.value]), 2)
        self.assertIn("segment_id", result[IceSat2Product.ATL08.value].columns)

    def test_validate_dfs_keeps_valid_products_and_drops_empty(self):
        """Valid products are kept; a product without records is omitted."""
        valid_df = pd.DataFrame({"segment_id": [1, 2], "latitude": [1.0, 2.0]})
        result = IceSat2Granule._validate_dfs(
            {
                IceSat2Product.ATL08.value: valid_df,
                IceSat2Product.ATL03.value: pd.DataFrame(),
            },
            "key",
        )
        self.assertIn(IceSat2Product.ATL08.value, result)
        self.assertNotIn(IceSat2Product.ATL03.value, result)

    # ── parse_granules ────────────────────────────────────────────────────────

    def test_parse_granules_nonexistent_file_raises(self):
        granules = [(IceSat2Product.ATL08.value, "/nonexistent/file.h5")]
        with self.assertRaises(FileNotFoundError):
            self.granule.parse_granules(granules, "key_001")

    def test_parse_granules_valid_atl08_returns_dataframe(self):
        granules = [(IceSat2Product.ATL08.value, self.atl08_path)]
        result = self.granule.parse_granules(granules, "key_001")
        self.assertIn(IceSat2Product.ATL08.value, result)
        df = result[IceSat2Product.ATL08.value]
        self.assertFalse(df.empty)
        self.assertIn("segment_id", df.columns)

    # ── process_granule ───────────────────────────────────────────────────────

    def test_process_granule_missing_product_raises(self):
        """A None filepath signals a missing download, which must fail the granule."""
        row = [("key_002", ("ATL08", None))]
        with self.assertRaisesRegex(ValueError, "Missing HDF5"):
            self.granule.process_granule(row)

    def test_process_granule_rejects_mixed_granules(self):
        row = [
            ("key_a", (IceSat2Product.ATL08.value, self.atl08_path)),
            ("key_b", (IceSat2Product.ATL03.value, self.atl08_path)),
        ]
        with self.assertRaisesRegex(ValueError, "different granules"):
            self.granule.process_granule(row)

    def test_process_granule_valid_atl08_returns_dataframe(self):
        granule_key = "ATL08_20181014001049_02350102_007_01"
        row = [(granule_key, (IceSat2Product.ATL08.value, self.atl08_path))]
        granule_id, result = self.granule.process_granule(row)
        self.assertIsNotNone(granule_id)
        self.assertIsNotNone(result)
        self.assertIn(IceSat2Product.ATL08.value, result)
        df = result[IceSat2Product.ATL08.value]
        self.assertFalse(df.empty)
        self.assertIn("segment_id", df.columns)

    def test_process_granule_bad_file_raises_and_keeps_download(self):
        """A parse failure propagates instead of returning an empty success, which
        the processor would checkpoint as processed (silently losing the granule).
        The download directory is kept so a retry need not download again."""
        granule_dir = os.path.join(self.granule.download_path, "key_003")
        os.makedirs(granule_dir, exist_ok=True)
        row = [("key_003", (IceSat2Product.ATL08.value, "/bad/path.h5"))]
        with self.assertRaises(FileNotFoundError):
            self.granule.process_granule(row)
        self.assertTrue(os.path.isdir(granule_dir))
