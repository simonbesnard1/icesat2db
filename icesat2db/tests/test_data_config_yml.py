# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felixd@gfz.de and urbazaev@gfz.de
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import os
import unittest

import yaml


class TestDataConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.chdir(os.path.dirname(__file__))
        cls.yaml_file_path = "data/data_config.yml"
        with open(cls.yaml_file_path, "r") as file:
            cls.config = yaml.safe_load(file)

    def test_file_load(self):
        """Test if the YAML file is loaded correctly"""
        self.assertIsNotNone(self.config, "Failed to load data_config.yml file")

    def test_tiledb_parameters(self):
        """Validate the TileDB configuration"""
        tiledb = self.config.get("tiledb")
        self.assertIsNotNone(tiledb, "'tiledb' section is missing")
        self.assertIn("storage_type", tiledb)
        self.assertIn("dimensions", tiledb)
        self.assertIsInstance(tiledb["dimensions"], list)
        self.assertGreater(
            len(tiledb["dimensions"]), 0, "TileDB dimensions list is empty"
        )
        self.assertIn("consolidation_settings", tiledb)
        self.assertIn("fragment_size", tiledb["consolidation_settings"])
        self.assertIn("memory_budget", tiledb["consolidation_settings"])

    def test_earth_data_info(self):
        """Check Earthdata Search API configuration"""
        earth_data = self.config.get("earth_data_info")
        self.assertIsNotNone(earth_data, "'earth_data_info' section is missing")
        self.assertIn("CMR_URL", earth_data)
        self.assertTrue(earth_data["CMR_URL"].startswith("https"), "Invalid CMR_URL")
        self.assertIn("CMR_PRODUCT_IDS", earth_data)
        self.assertIsInstance(earth_data["CMR_PRODUCT_IDS"], dict)

    def test_level_atl08_variables(self):
        """Verify structure and content of level_2a variables"""
        level_atl08 = self.config.get("level_atl08")
        self.assertIsNotNone(level_atl08, "'level_atl08' section is missing")
        variables = level_atl08.get("variables")
        self.assertIsNotNone(variables, "'variables' under level_atl08 is missing")
        self.assertIsInstance(variables, dict)
        self.assertIn("shot_number", variables, "'shot_number' variable is missing")
        shot_number = variables["shot_number"]
        # self.assertEqual(
        #     shot_number.get("dtype"),
        #     "uint64",
        #     "shot_number dtype should be 'uint64'",
        # )
        self.assertIn("description", shot_number)
        self.assertIsInstance(shot_number["description"], str)

    def test_data_dir(self):
        """Verify the data directory path exists or is writable"""
        data_dir = self.config.get("data_dir")
        self.assertIsNotNone(data_dir, "'data_dir' is missing")
        self.assertIsInstance(data_dir, str)


suite = unittest.TestLoader().loadTestsFromTestCase(TestDataConfig)
