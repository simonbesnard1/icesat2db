# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import tiledb
import yaml

from icesat2db.core.icesat2database import IceSat2Database


class TestIceSat2Database(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Dynamically resolve the path to the `data` folder
        cls.data_dir = Path(__file__).parent / "data"
        cls.yaml_file_path = cls.data_dir / "data_config.yml"

        if not cls.yaml_file_path.exists():
            raise FileNotFoundError(f"Config file not found: {cls.yaml_file_path}")

        with open(cls.yaml_file_path, "r") as file:
            cls.config = yaml.safe_load(file)

        # Override local TileDB path with a temporary directory
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.config["tiledb"]["local_path"] = cls.temp_dir.name

        # Initialize IceSat2Database instance
        cls.icesat2_db = IceSat2Database(cls.config)
        cls.icesat2_db._create_arrays()  # Create the TileDB array for testing

    @classmethod
    def tearDownClass(cls):
        """Cleanup temporary directory."""
        cls.temp_dir.cleanup()

    def test_tiledb_dimensions(self):
        """Test that TileDB dimensions are configured correctly."""
        with tiledb.open(
            self.icesat2_db.array_uri, mode="r", ctx=self.icesat2_db.ctx
        ) as array:
            schema = array.schema
            dims = schema.domain

            # Check dimensions
            lat_dim = dims.dim("latitude")
            lon_dim = dims.dim("longitude")
            time_dim = dims.dim("time")

            self.assertIn(
                "latitude",
                lat_dim.name,
                "The 'latitude' dimension is missing from the TileDB schema.",
            )
            self.assertIn(
                "longitude",
                lon_dim.name,
                "The 'longitude' dimension is missing from the TileDB schema.",
            )
            self.assertIn(
                "time",
                time_dim.name,
                "The 'time' dimension is missing from the TileDB schema.",
            )

            self.assertEqual(lat_dim.domain, (-90.0, 90.0), "Latitude range mismatch")
            self.assertEqual(
                lon_dim.domain, (-180.0, 180.0), "Longitude range mismatch"
            )
            # Check chunk size
            self.assertEqual(lat_dim.tile, 1.0, "Latitude chunk size mismatch")
            self.assertEqual(lon_dim.tile, 1.0, "Longitude chunk size mismatch")

    def test_tiledb_attributes(self):
        """Test that TileDB attributes are correctly set."""
        with tiledb.open(
            self.icesat2_db.array_uri, mode="r", ctx=self.icesat2_db.ctx
        ) as array:
            schema = array.schema

            # Check for expected attributes
            expected_attributes = [
                "delta_time",
                "segment_id",
                "canopy_h_metrics_16",
            ]  # Example attributes in the array

            for attr in expected_attributes:
                self.assertIn(
                    attr, schema.attr(attr).name, f"Missing attribute: {attr}"
                )

    def test_overwrite_behavior(self):
        """Ensure overwrite behavior works correctly."""
        self.assertTrue(
            self.config["tiledb"]["overwrite"],
            "Overwrite setting should be True",
        )

        # Check if array exists after creation
        self.assertTrue(
            tiledb.array_exists(self.icesat2_db.array_uri),
            "TileDB array should exist after creation",
        )

        # Re-create the array and confirm it overwrites
        self.icesat2_db._create_arrays()  # Overwrite
        self.assertTrue(
            tiledb.array_exists(self.icesat2_db.array_uri),
            "TileDB array should still exist after overwrite",
        )

    def test_write_granule(self):
        """Test the `write_granule` function to write data to TileDB."""
        granule_file = self.data_dir / "example_data.csv"

        if not granule_file.exists():
            raise FileNotFoundError(f"Granule file not found: {granule_file}")

        granule_data = pd.read_csv(granule_file)
        self.icesat2_db.write_granule(granule_data)

        with tiledb.open(
            self.icesat2_db.array_uri, mode="r", ctx=self.icesat2_db.ctx
        ) as array:
            segment_id = array.query(attrs=("segment_id",)).multi_index[:, :, :]

            self.assertTrue(
                np.array_equal(
                    segment_id["segment_id"],
                    [
                        5383848881708766,
                        5383840291774169,
                        5383840291774164,
                        5383840291774364,
                        5383840291774354,
                        5383840291774324,
                    ],
                ),
                "Segment_id mismatch",
            )

    def test_check_granules_status_returns_false_when_unprocessed(self):
        statuses = self.icesat2_db.check_granules_status(["fake_id_aaa", "fake_id_bbb"])
        self.assertFalse(statuses["fake_id_aaa"])
        self.assertFalse(statuses["fake_id_bbb"])

    def test_mark_granules_as_processed_batch_and_check(self):
        keys = ["batch_granule_001", "batch_granule_002"]
        # Initially unprocessed
        self.assertTrue(
            all(not v for v in self.icesat2_db.check_granules_status(keys).values())
        )
        self.icesat2_db.mark_granules_as_processed_batch(keys)
        statuses = self.icesat2_db.check_granules_status(keys)
        self.assertTrue(all(statuses.values()))

    def test_mark_granule_as_processed_single(self):
        key = "single_granule_001"
        self.icesat2_db.mark_granule_as_processed(key)
        self.assertTrue(self.icesat2_db.check_granules_status([key])[key])

    def test_mark_granules_as_processed_batch_empty_list(self):
        """Empty batch should not raise."""
        self.icesat2_db.mark_granules_as_processed_batch([])

    def test_spatial_chunking_splits_into_tiles(self):
        df = pd.DataFrame(
            {
                "latitude": [0.5, 1.5, 30.5, -30.5],
                "longitude": [0.5, 50.5, -100.5, 100.5],
            }
        )
        chunks = list(self.icesat2_db.spatial_chunking(df))
        self.assertGreater(len(chunks), 0)
        total = sum(len(tile_df) for _, tile_df in chunks)
        self.assertEqual(total, len(df))

    def test_spatial_chunking_empty_dataframe_yields_nothing(self):
        df = pd.DataFrame(
            {
                "latitude": pd.Series([], dtype=float),
                "longitude": pd.Series([], dtype=float),
            }
        )
        self.assertEqual(list(self.icesat2_db.spatial_chunking(df)), [])

    def test_spatial_chunking_missing_columns_raises(self):
        with self.assertRaises(ValueError):
            list(self.icesat2_db.spatial_chunking(pd.DataFrame({"latitude": [1.0]})))

    def test_validate_granule_data_raises_on_missing_dimension(self):
        df_bad = pd.DataFrame({"latitude": [1.0]})  # longitude and time missing
        with self.assertRaises(ValueError):
            self.icesat2_db._validate_granule_data(df_bad)

    def test_validate_granule_data_passes_when_all_dims_present(self):
        df = pd.read_csv(self.data_dir / "example_data.csv")
        # Should not raise
        self.icesat2_db._validate_granule_data(df)

    def test_write_granule_out_of_domain_rows_are_silently_dropped(self):
        """Rows outside ±90/±180 are filtered; write should not raise."""
        df = pd.read_csv(self.data_dir / "example_data.csv").copy()
        df["latitude"] = 999.0  # clearly outside domain
        self.icesat2_db.write_granule(df)  # should not raise

    def test_coerce_series_to_float32(self):
        s = pd.Series([1.0, 2.0, 3.0], dtype=np.float64)
        result = IceSat2Database._coerce_series("v", s, np.dtype(np.float32))
        self.assertEqual(result.dtype, np.float32)

    def test_coerce_series_to_string(self):
        s = pd.Series([1, None, 3])
        result = IceSat2Database._coerce_series("v", s, np.dtype("U10"))
        self.assertTrue(np.issubdtype(result.dtype, np.str_))

    def test_schema_cache_is_populated_after_first_write(self):
        """_get_schema_cache should return non-empty dicts."""
        cache = self.icesat2_db._get_schema_cache()
        self.assertIn("dim_names", cache)
        self.assertIn("latitude", cache["dim_names"])


suite = unittest.TestLoader().loadTestsFromTestCase(TestIceSat2Database)


class TestIceSat2DatabaseMultiProduct(unittest.TestCase):
    """
    IceSat2Database's `product` parameter lets ATL08 and ATL03 (or any other
    product) write to independent TileDB arrays under the same local_path,
    without the default 'atl08' array name changing (backward compatible with
    existing on-disk archives).
    """

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        base_tiledb = {
            "storage_type": "local",
            "local_path": cls.temp_dir.name,
            "overwrite": True,
            "dimensions": ["latitude", "longitude", "time"],
            "spatial_range": {
                "lat_min": -90.0,
                "lat_max": 90.0,
                "lon_min": -180.0,
                "lon_max": 180.0,
            },
            "time_range": {"start_time": "2018-01-01", "end_time": "2030-12-31"},
        }
        cls.config = {
            "tiledb": base_tiledb,
            "level_atl08": {
                "variables": {
                    "segment_id": {"SDS_Name": "segment_id", "dtype": "int64"},
                }
            },
            "level_atl03": {
                "variables": {
                    "segment_id": {"SDS_Name": "segment_id", "dtype": "int64"},
                    "h_ph": {"SDS_Name": "heights/h_ph", "dtype": "float32"},
                }
            },
        }

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def test_default_product_uses_unchanged_array_uri(self):
        db = IceSat2Database(self.config)
        self.assertEqual(Path(db.array_uri).name, "array_uri")

    def test_atl08_product_matches_default_array_uri(self):
        db = IceSat2Database(self.config, product="atl08")
        self.assertEqual(Path(db.array_uri).name, "array_uri")

    def test_atl03_product_uses_separate_array_uri(self):
        db = IceSat2Database(self.config, product="atl03")
        self.assertEqual(Path(db.array_uri).name, "array_uri_atl03")

    def test_atl08_and_atl03_variables_config_are_independent(self):
        db08 = IceSat2Database(self.config, product="atl08")
        db03 = IceSat2Database(self.config, product="atl03")
        self.assertIn("segment_id", db08.variables_config)
        self.assertNotIn("h_ph", db08.variables_config)
        self.assertIn("h_ph", db03.variables_config)

    def test_two_product_arrays_coexist_without_collision(self):
        db08 = IceSat2Database(self.config, product="atl08")
        db03 = IceSat2Database(self.config, product="atl03")
        db08._create_arrays()
        db03._create_arrays()

        self.assertNotEqual(db08.array_uri, db03.array_uri)
        self.assertTrue(tiledb.array_exists(db08.array_uri, ctx=db08.ctx))
        self.assertTrue(tiledb.array_exists(db03.array_uri, ctx=db03.ctx))

        with tiledb.open(db03.array_uri, mode="r", ctx=db03.ctx) as array:
            attr_names = {array.schema.attr(i).name for i in range(array.schema.nattr)}
        self.assertIn("h_ph", attr_names)
