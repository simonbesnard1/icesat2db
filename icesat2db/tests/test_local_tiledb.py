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


suite = unittest.TestLoader().loadTestsFromTestCase(TestIceSat2Database)
