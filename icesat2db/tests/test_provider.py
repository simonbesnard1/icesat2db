# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import tempfile
import unittest

import geopandas as gpd
import pandas as pd
import yaml
from pandas import DataFrame
from xarray import Dataset

from icesat2db import IceSat2Database, IceSat2Provider


class TestIceSat2Provider(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.chdir(os.path.dirname(__file__))
        cls.yaml_file_path = "data/data_config.yml"
        cls.geometry = gpd.read_file("data/bounding_box.geojson")

        with open(cls.yaml_file_path, "r") as file:
            cls.config = yaml.safe_load(file)

        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.config["tiledb"]["local_path"] = cls.temp_dir.name

        cls.gedi_db = IceSat2Database(cls.config)
        cls.gedi_db._create_arrays()  # Create the TileDB array for testing
        # write test data
        granule_data = pd.read_csv("data/example_data.csv")
        cls.gedi_db.write_granule(granule_data)

        cls.gedi_provider = IceSat2Provider(
            storage_type="local", local_path=cls.temp_dir.name
        )

    @classmethod
    def tearDownClass(cls):
        """Cleanup temporary directory."""
        cls.temp_dir.cleanup()

    def test_get_data_with_geometry(self):
        """Test get_data with a geometry argument."""
        variables = ["canopy_h_metrics_16", "h_canopy_quad"]
        result = self.gedi_provider.get_data(variables, geometry=self.geometry)
        self.assertIsNotNone(result, "Result should not be None")
        self.assertTrue(
            isinstance(result, Dataset), "Result should be an xarray Dataset"
        )

    def test_get_data_with_time_range(self):
        """Test get_data with start and end time arguments."""
        variables = ["canopy_h_metrics_16", "h_canopy_quad"]
        start_time = "2018-01-01"
        end_time = "2030-12-31"
        result = self.gedi_provider.get_data(
            variables,
            geometry=self.geometry,
            start_time=start_time,
            end_time=end_time,
        )
        self.assertIsNotNone(result, "Result should not be None")
        self.assertTrue(
            isinstance(result, Dataset), "Result should be an xarray Dataset"
        )

    def test_get_data_with_point_query(self):
        """Test get_data with point and radius for query."""
        variables = ["canopy_h_metrics_16", "h_canopy_quad"]
        point = (-3.035047, -54.949860)
        radius = 1.0
        result = self.gedi_provider.get_data(
            variables,
            query_type="nearest",
            point=point,
            radius=radius,
            num_shots=2,
        )
        self.assertIsNotNone(result, "Result should not be None")
        self.assertTrue(
            isinstance(result, Dataset), "Result should be an xarray Dataset"
        )

    def test_get_data_with_quality_filters(self):
        """Test get_data with quality filters."""
        variables = ["canopy_h_metrics_16", "h_canopy_quad"]

        unfiltered_result = self.gedi_provider.get_data(
            variables, geometry=self.geometry
        )
        self.assertIsNotNone(unfiltered_result, "Unfiltered result should not be None")
        self.assertTrue(
            isinstance(unfiltered_result, Dataset),
            "Unfiltered result should be an xarray Dataset",
        )

        quality_filters = {
            "h_canopy_quad": "> 12.0",
        }

        filtered_result = self.gedi_provider.get_data(
            variables, geometry=self.geometry, **quality_filters
        )
        self.assertIsNotNone(filtered_result, "Filtered result should not be None")
        self.assertTrue(
            isinstance(filtered_result, Dataset),
            "Filtered result should be an xarray Dataset",
        )

        self.assertTrue(
            len(filtered_result[variables[0]]) < len(unfiltered_result[variables[0]]),
            "Filtered data should have fewer entries than unfiltered data",
        )
        self.assertTrue(
            len(filtered_result[variables[1]]) < len(unfiltered_result[variables[1]]),
            "Filtered data should have fewer entries than unfiltered data",
        )

    def test_get_data_with_different_return_types(self):
        """Test get_data with different return types."""
        variables = ["canopy_h_metrics_16", "h_canopy_quad"]

        # Test xarray return type
        result_xarray = self.gedi_provider.get_data(
            variables, geometry=self.geometry, return_type="xarray"
        )
        self.assertTrue(
            isinstance(result_xarray, Dataset),
            "Result should be an xarray Dataset",
        )

        # Test pandas DataFrame return type
        result_df = self.gedi_provider.get_data(
            variables, geometry=self.geometry, return_type="dataframe"
        )
        self.assertTrue(
            isinstance(result_df, DataFrame),
            "Result should be a pandas DataFrame",
        )

    def test_get_data_invalid_query_type(self):
        """Test get_data with an invalid query type."""
        variables = ["canopy_h_metrics_16", "h_canopy_quad"]
        query_type = "invalid_query"
        with self.assertRaises(ValueError):
            self.gedi_provider.get_data(
                variables, geometry=self.geometry, query_type=query_type
            )

    def test_get_data_invalid_return_type(self):
        """Test get_data with an invalid query type."""
        variables = ["canopy_h_metrics_16", "h_canopy_quad"]
        return_type = "invalid_return"
        with self.assertRaises(ValueError):
            self.gedi_provider.get_data(
                variables, geometry=self.geometry, return_type=return_type
            )
