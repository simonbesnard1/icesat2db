# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import os
import unittest
from pathlib import Path

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
        """Verify structure and content of level_atl08 variables"""
        level_atl08 = self.config.get("level_atl08")
        self.assertIsNotNone(level_atl08, "'level_atl08' section is missing")
        variables = level_atl08.get("variables")
        self.assertIsNotNone(variables, "'variables' under level_atl08 is missing")
        self.assertIsInstance(variables, dict)
        self.assertIn("segment_id", variables, "'segment_id' variable is missing")
        shot_number = variables["segment_id"]
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


class TestATL03DataConfig(unittest.TestCase):
    """
    Validate the level_atl03 block in the production config
    (data/config_files/data_config.yml). Deliberately not added to
    icesat2db/tests/data/data_config.yml — that fixture is shared by many
    existing ATL08-only tests, and adding level_atl03 there would change what
    IceSat2Processor considers "configured products" for every one of them.
    """

    @classmethod
    def setUpClass(cls):
        cls.config_path = (
            Path(__file__).resolve().parents[2]
            / "data"
            / "config_files"
            / "data_config.yml"
        )
        if not cls.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {cls.config_path}")
        with open(cls.config_path, "r") as f:
            cls.config = yaml.safe_load(f)

    def test_level_atl03_section_present(self):
        self.assertIn("level_atl03", self.config)

    def test_confidence_filter_settings_present(self):
        level_atl03 = self.config["level_atl03"]
        self.assertIn("confidence_column", level_atl03)
        self.assertIn("confidence_threshold", level_atl03)
        self.assertIsInstance(level_atl03["confidence_column"], int)
        self.assertIsInstance(level_atl03["confidence_threshold"], int)

    def test_required_variables_present(self):
        variables = self.config["level_atl03"]["variables"]
        for name in ("h_ph", "dist_ph_along", "quality_ph", "segment_id", "beam_id"):
            self.assertIn(name, variables, f"'{name}' variable is missing")
            self.assertIn("SDS_Name", variables[name])
            self.assertIn("dtype", variables[name])

    def test_signal_conf_ph_is_profile_expanded(self):
        signal_conf = self.config["level_atl03"]["variables"]["signal_conf_ph"]
        self.assertTrue(signal_conf.get("is_profile"))
        self.assertEqual(signal_conf.get("profile_length"), 5)
        self.assertEqual(len(signal_conf.get("profile_labels", [])), 5)

    def test_cmr_product_id_configured(self):
        cmr_ids = self.config["earth_data_info"]["CMR_PRODUCT_IDS"]
        self.assertIn("ATL03", cmr_ids)
        self.assertTrue(cmr_ids["ATL03"], "ATL03 CMR concept id must not be empty")


suite_atl03 = unittest.TestLoader().loadTestsFromTestCase(TestATL03DataConfig)
