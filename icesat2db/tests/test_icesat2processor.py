# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from icesat2db.core.icesat2processor import IceSat2Processor


class TestValidateAndParseDate(unittest.TestCase):
    """Unit tests for IceSat2Processor._validate_and_parse_date (static method)."""

    def test_valid_date_returns_datetime(self):
        result = IceSat2Processor._validate_and_parse_date("2024-06-15", "start_date")
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.month, 6)
        self.assertEqual(result.day, 15)

    def test_valid_date_boundary_jan_first(self):
        result = IceSat2Processor._validate_and_parse_date("2018-01-01", "start_date")
        self.assertEqual(result, datetime(2018, 1, 1))

    def test_invalid_format_slash_raises(self):
        with self.assertRaises(ValueError) as ctx:
            IceSat2Processor._validate_and_parse_date("15/06/2024", "start_date")
        self.assertIn("start_date", str(ctx.exception))

    def test_invalid_format_missing_day_raises(self):
        with self.assertRaises(ValueError):
            IceSat2Processor._validate_and_parse_date("2024-06", "end_date")

    def test_invalid_format_text_raises(self):
        with self.assertRaises(ValueError):
            IceSat2Processor._validate_and_parse_date("not-a-date", "start_date")

    def test_error_message_contains_date_type(self):
        with self.assertRaises(ValueError) as ctx:
            IceSat2Processor._validate_and_parse_date("bad", "end_date")
        self.assertIn("end_date", str(ctx.exception))


class TestEnsureDirectory(unittest.TestCase):
    """Unit tests for IceSat2Processor._ensure_directory (static method)."""

    def test_creates_new_directory(self):
        with tempfile.TemporaryDirectory() as base:
            new_dir = os.path.join(base, "subdir", "nested")
            result = IceSat2Processor._ensure_directory(new_dir)
            self.assertTrue(os.path.isdir(new_dir))
            self.assertEqual(result, new_dir)

    def test_existing_directory_does_not_raise(self):
        with tempfile.TemporaryDirectory() as base:
            # Call twice — second call must not raise
            IceSat2Processor._ensure_directory(base)
            result = IceSat2Processor._ensure_directory(base)
            self.assertEqual(result, base)

    def test_returns_path_string(self):
        with tempfile.TemporaryDirectory() as base:
            path = os.path.join(base, "mydir")
            result = IceSat2Processor._ensure_directory(path)
            self.assertIsInstance(result, str)


class TestLoadYamlFile(unittest.TestCase):
    """Unit tests for IceSat2Processor._load_yaml_file (static method)."""

    # Path to the shared test config used across the test suite
    CONFIG_PATH = str(Path(__file__).parent / "data" / "data_config.yml")

    def test_valid_yaml_returns_dict(self):
        result = IceSat2Processor._load_yaml_file(self.CONFIG_PATH)
        self.assertIsInstance(result, dict)

    def test_valid_yaml_contains_tiledb_key(self):
        result = IceSat2Processor._load_yaml_file(self.CONFIG_PATH)
        self.assertIn("tiledb", result)

    def test_nonexistent_file_raises(self):
        with self.assertRaises((FileNotFoundError, OSError)):
            IceSat2Processor._load_yaml_file("/nonexistent/path/config.yml")

    def test_inline_yaml_content(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as tmp:
            tmp.write("key: value\nnested:\n  a: 1\n")
            tmp_path = tmp.name
        try:
            result = IceSat2Processor._load_yaml_file(tmp_path)
            self.assertEqual(result["key"], "value")
            self.assertEqual(result["nested"]["a"], 1)
        finally:
            os.unlink(tmp_path)


class TestFilterUnprocessedGranules(unittest.TestCase):
    """
    Unit tests for IceSat2Processor._filter_unprocessed_granules.

    The method is an instance method that calls self.database_writer.check_granules_status.
    We bypass __init__ entirely and inject a mock database_writer.
    """

    def _make_processor(self, processed_map: dict) -> IceSat2Processor:
        """
        Return a bare IceSat2Processor with a mock database_writer whose
        check_granules_status returns `processed_map`.
        """
        obj = object.__new__(IceSat2Processor)
        mock_writer = MagicMock()
        mock_writer.check_granules_status.return_value = processed_map
        obj.database_writer = mock_writer
        return obj

    def test_all_unprocessed_returns_full_dict(self):
        cmr = {"g1": ("ATL08", "url1"), "g2": ("ATL08", "url2")}
        proc = self._make_processor({"g1": False, "g2": False})
        result = proc._filter_unprocessed_granules(cmr)
        self.assertEqual(result, cmr)

    def test_all_processed_returns_empty_dict(self):
        cmr = {"g1": ("ATL08", "url1"), "g2": ("ATL08", "url2")}
        proc = self._make_processor({"g1": True, "g2": True})
        result = proc._filter_unprocessed_granules(cmr)
        self.assertEqual(result, {})

    def test_mixed_returns_only_unprocessed(self):
        cmr = {
            "g1": ("ATL08", "url1"),
            "g2": ("ATL08", "url2"),
            "g3": ("ATL08", "url3"),
        }
        proc = self._make_processor({"g1": True, "g2": False, "g3": True})
        result = proc._filter_unprocessed_granules(cmr)
        self.assertNotIn("g1", result)
        self.assertIn("g2", result)
        self.assertNotIn("g3", result)

    def test_empty_cmr_returns_empty(self):
        proc = self._make_processor({})
        result = proc._filter_unprocessed_granules({})
        self.assertEqual(result, {})

    def test_check_granules_status_called_with_all_ids(self):
        cmr = {"g1": "info1", "g2": "info2"}
        proc = self._make_processor({"g1": False, "g2": False})
        proc._filter_unprocessed_granules(cmr)
        called_ids = proc.database_writer.check_granules_status.call_args[0][0]
        self.assertCountEqual(called_ids, ["g1", "g2"])

    def test_unknown_granule_id_treated_as_unprocessed(self):
        """If check_granules_status doesn't return a key, default is False (unprocessed)."""
        cmr = {"g_new": "info"}
        proc = self._make_processor({})  # empty map — granule not present
        result = proc._filter_unprocessed_granules(cmr)
        self.assertIn("g_new", result)
