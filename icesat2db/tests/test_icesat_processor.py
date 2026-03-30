# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
import logging
import os
import unittest

import geopandas as gpd
import yaml


import os
import unittest
import yaml
import tempfile
from pathlib import Path
from icesat2db.core.icesat2processor import IceSat2Processor


class TestIceSat2Processor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.chdir(os.path.dirname(__file__))

        # create a valid temporary config file
        cls.tmp_dir = tempfile.TemporaryDirectory()
        cls.log_dir = os.path.join(cls.tmp_dir.name, "logs")
        cls.logger = logging.getLogger("test_logger")
        cls.logger.handlers = []

        cls.valid_config_path = os.path.join(cls.tmp_dir.name, "config.yml")
        with open(cls.valid_config_path, "w") as f:
            f.write("dummy: value")
        cls.logger_config_path = os.path.join(cls.tmp_dir.name, "logger_config.yml")
        with open(cls.logger_config_path, "w") as f:
            f.write("data_dir: {}\nprogress_dir: {}\n".format(
                cls.tmp_dir.name, cls.tmp_dir.name
            ))

        # minimal valid geometry placeholder
        cls.valid_geometry = gpd.read_file("data/bounding_box.geojson")

    @classmethod
    def tearDownClass(cls):
        cls.tmp_dir.cleanup()

    # ------------------------
    # config_file validation
    # ------------------------
    def test_config_file_none(self):
        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=None
            )

    def test_config_file_not_string(self):
        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=123
            )

    def test_config_file_wrong_extension(self):
        wrong_path = os.path.join(self.tmp_dir.name, "config.txt")
        with open(wrong_path, "w") as f:
            f.write("dummy")

        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=wrong_path
            )

    def test_config_file_not_existing(self):
        missing_path = os.path.join(self.tmp_dir.name, "missing.yml")

        with self.assertRaises(FileNotFoundError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=missing_path
            )

    # ------------------------
    # credentials validation
    # ------------------------
    def test_credentials_not_dict(self):
        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=self.valid_config_path,
                credentials="not_a_dict"
            )

    # ------------------------
    # parallel_engine validation
    # ------------------------
    def test_parallel_engine_invalid(self):
        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=self.valid_config_path,
                parallel_engine="invalid"
            )

    # ------------------------
    # log_dir validation
    # ------------------------

    def test_log_dir_not_string(self):
        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=self.valid_config_path,
                log_dir=123
            )

    # ------------------------
    # geometry validation
    # ------------------------
    def test_geometry_none(self):
        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=None,
                config_file=self.valid_config_path
            )

    # ------------------------
    # date validation
    # ------------------------
    def test_only_start_date(self):
        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=self.valid_config_path,
                start_date="2020-01-01",
                end_date=None
            )

    def test_start_after_end(self):
        with self.assertRaises(ValueError):
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=self.valid_config_path,
                start_date="2021-01-01",
                end_date="2020-01-01"
            )

    # ------------------------
    # logger validation
    # ------------------------
    def test_log_dir_created(self):
        try:
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=self.logger_config_path,
                log_dir=self.log_dir,
                earth_data_dir=self.tmp_dir.name,
                start_date = "2021-01-01",
                end_date = "2022-01-01"
            )
        except Exception:
            pass

        self.assertTrue(os.path.isdir(self.log_dir))

    def test_log_file_created(self):
        try:
            IceSat2Processor(
                geometry=self.valid_geometry,
                config_file=self.logger_config_path,
                log_dir=self.log_dir,
                earth_data_dir=self.tmp_dir.name,
                start_date="2021-01-01",
                end_date="2022-01-01"
            )
        except Exception:
            pass

        files = os.listdir(self.log_dir)
        self.assertTrue(any(f.endswith(".log") for f in files))

    # def test_file_handler_added(self):
    #     try:
    #         IceSat2Processor(
    #             geometry=self.valid_geometry,
    #             config_file=self.logger_config_path,
    #             log_dir=self.log_dir,
    #             earth_data_dir=self.tmp_dir.name,
    #             start_date="2021-01-01",
    #             end_date="2022-01-01"
    #         )
    #     except Exception:
    #         pass
    #
    #     handlers = [h for h in self.logger.handlers if isinstance(h, logging.FileHandler)]
    #     self.assertTrue(len(handlers) >= 1)
    #
    # def test_no_duplicate_file_handler(self):
    #     for _ in range(2):
    #         try:
    #             IceSat2Processor(
    #             geometry=self.valid_geometry,
    #             config_file=self.logger_config_path,
    #             log_dir=self.log_dir,
    #             earth_data_dir=self.tmp_dir.name,
    #             start_date = "2021-01-01",
    #             end_date = "2022-01-01"
    #             )
    #         except Exception:
    #             pass
    #
    #     handlers = [h for h in self.logger.handlers if isinstance(h, logging.FileHandler)]
    #     self.assertEqual(len(handlers), 1)
    #
    # def test_file_handler_added2(self):
    #     try:
    #         IceSat2Processor(
    #             geometry=self.valid_geometry,
    #             config_file=self.logger_config_path,
    #             log_dir=self.log_dir,
    #             earth_data_dir=self.tmp_dir.name,
    #             start_date="2021-01-01",
    #             end_date="2022-01-01"
    #         )
    #     except Exception as e:
    #         error = e
    #     else:
    #         error = None
    #
    #     handlers = [
    #         h for h in self.logger.handlers
    #         if isinstance(h, logging.FileHandler)
    #     ]
    #
    #     if len(handlers) == 0:
    #         self.fail(f"No FileHandler added. Constructor likely failed early: {error}")
    #
    # def test_no_duplicate_file_handler2(self):
    #     last_error = None
    #
    #     for _ in range(2):
    #         try:
    #             IceSat2Processor(
    #                 geometry=self.valid_geometry,
    #                 config_file=self.logger_config_path,
    #                 log_dir=self.log_dir,
    #                 earth_data_dir=self.tmp_dir.name,
    #                 start_date="2021-01-01",
    #                 end_date="2022-01-01"
    #             )
    #         except Exception as e:
    #             last_error = e
    #
    #     handlers = [
    #         h for h in self.logger.handlers
    #         if isinstance(h, logging.FileHandler)
    #     ]
    #
    #     if len(handlers) == 0:
    #         self.fail(f"No FileHandler added at all. Constructor failed early: {last_error}")
    #
    #     self.assertEqual(
    #         len(handlers),
    #         1,
    #         f"Expected exactly one FileHandler, got {len(handlers)} (duplicate handler bug)"
    #     )

suite = unittest.TestLoader().loadTestsFromTestCase(TestIceSat2Processor)
