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

import geopandas as gpd
import numpy as np
import pandas as pd
import yaml
from pandas import DataFrame
from shapely.geometry import box
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
        point = (-54.949859619140625, -3.0350465774536133)
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

    def test_get_data_no_geometry_raises(self):
        with self.assertRaises(ValueError):
            self.gedi_provider.get_data(["h_canopy_quad"], geometry=None)


class TestTileDBProviderLowLevel(unittest.TestCase):
    """Unit tests for TileDBProvider helper methods."""

    @classmethod
    def setUpClass(cls):
        os.chdir(os.path.dirname(__file__))
        with open("data/data_config.yml", "r") as f:
            config = yaml.safe_load(f)
        cls.temp_dir = tempfile.TemporaryDirectory()
        config["tiledb"]["local_path"] = cls.temp_dir.name

        db = IceSat2Database(config)
        db._create_arrays()
        db.write_granule(pd.read_csv("data/example_data.csv"))

        cls.provider = IceSat2Provider(
            storage_type="local", local_path=cls.temp_dir.name
        )

    @classmethod
    def tearDownClass(cls):
        cls.provider.close()
        cls.temp_dir.cleanup()

    # ── _build_condition_string ───────────────────────────────────────────────

    def test_build_condition_string_empty_returns_none(self):
        self.assertIsNone(self.provider._build_condition_string({}))

    def test_build_condition_string_simple(self):
        result = self.provider._build_condition_string({"h_canopy_quad": "> 5.0"})
        self.assertIsNotNone(result)
        self.assertIn("h_canopy_quad", result)
        self.assertIn(">", result)

    def test_build_condition_string_multiple_filters_joined_with_and(self):
        result = self.provider._build_condition_string(
            {"h_canopy_quad": "> 5.0", "brightness_flag": "== 0"}
        )
        self.assertIn("and", result)

    def test_build_condition_string_unknown_operator_logs_warning(self):
        # No valid operator — should return None (empty cond_list)
        result = self.provider._build_condition_string(
            {"h_canopy_quad": "between 5 10"}
        )
        self.assertIsNone(result)

    # ── _build_profile_attrs ──────────────────────────────────────────────────

    def test_build_profile_attrs_scalar_variable(self):
        attr_list, profile_vars, subsegment_vars, label_info, scalar_renames = (
            self.provider._build_profile_attrs(["h_canopy_quad"], {})
        )
        self.assertIn("h_canopy_quad", attr_list)
        self.assertNotIn("h_canopy_quad", profile_vars)
        self.assertNotIn("h_canopy_quad", subsegment_vars)
        self.assertNotIn("h_canopy_quad", label_info)
        self.assertEqual(scalar_renames, {})

    def test_build_profile_attrs_profile_variable_expands(self):
        meta = {"canopy_h_metrics.profile_length": 18}
        attr_list, profile_vars, subsegment_vars, label_info, scalar_renames = (
            self.provider._build_profile_attrs(["canopy_h_metrics"], meta)
        )
        self.assertEqual(len(attr_list), 18)
        self.assertIn("canopy_h_metrics", profile_vars)
        self.assertEqual(len(profile_vars["canopy_h_metrics"]), 18)
        self.assertEqual(attr_list[0], "canopy_h_metrics_1")
        self.assertEqual(attr_list[-1], "canopy_h_metrics_18")
        self.assertNotIn("canopy_h_metrics", label_info)

    def test_build_profile_attrs_profile_variable_with_labels(self):
        meta = {
            "canopy_h_metrics.profile_length": 18,
            "canopy_h_metrics.profile_labels": "10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95",
            "canopy_h_metrics.profile_label_name": "percentile",
        }
        attr_list, profile_vars, subsegment_vars, label_info, scalar_renames = (
            self.provider._build_profile_attrs(["canopy_h_metrics"], meta)
        )
        self.assertIn("canopy_h_metrics", label_info)
        self.assertEqual(label_info["canopy_h_metrics"]["dim_name"], "percentile")
        self.assertEqual(
            label_info["canopy_h_metrics"]["labels"],
            [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95],
        )

    def test_build_profile_attrs_subsegment_variable_expands(self):
        meta = {"h_canopy_20m.subsegment_length": 5}
        attr_list, profile_vars, subsegment_vars, label_info, scalar_renames = (
            self.provider._build_profile_attrs(["h_canopy_20m"], meta)
        )
        self.assertEqual(len(attr_list), 5)
        self.assertIn("h_canopy_20m", subsegment_vars)

    def test_build_profile_attrs_single_label_selection(self):
        meta = {
            "canopy_h_metrics.profile_length": 18,
            "canopy_h_metrics.profile_labels": "10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95",
            "canopy_h_metrics.profile_label_name": "percentile",
        }
        attr_list, profile_vars, subsegment_vars, label_info, scalar_renames = (
            self.provider._build_profile_attrs(["canopy_h_metrics:50"], meta)
        )
        # Only one attribute should be fetched (index 9, 1-based)
        self.assertEqual(len(attr_list), 1)
        self.assertEqual(attr_list[0], "canopy_h_metrics_9")
        # It should not appear in profile_vars (not a multi-column profile)
        self.assertNotIn("canopy_h_metrics", profile_vars)
        # The rename dict maps TileDB attr name to friendly name
        self.assertEqual(scalar_renames.get("canopy_h_metrics_9"), "canopy_h_metrics_p50")

    def test_build_profile_attrs_single_label_invalid_raises(self):
        meta = {
            "canopy_h_metrics.profile_length": 18,
            "canopy_h_metrics.profile_labels": "10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95",
        }
        with self.assertRaises(ValueError):
            self.provider._build_profile_attrs(["canopy_h_metrics:99"], meta)

    def test_build_profile_attrs_subsegment_single_label_selection(self):
        meta = {
            "h_canopy_20m.subsegment_length": 5,
            "h_canopy_20m.subsegment_labels": "10,30,50,70,90",
            "h_canopy_20m.subsegment_label_name": "along_track_offset_m",
        }
        attr_list, profile_vars, subsegment_vars, label_info, scalar_renames = (
            self.provider._build_profile_attrs(["h_canopy_20m:50"], meta)
        )
        # Centre bin is index 3 (1-based)
        self.assertEqual(len(attr_list), 1)
        self.assertEqual(attr_list[0], "h_canopy_20m_3")
        self.assertNotIn("h_canopy_20m", subsegment_vars)
        # Sub-segment labels use _<offset>m suffix, not _p<label>
        self.assertEqual(scalar_renames.get("h_canopy_20m_3"), "h_canopy_20m_50m")

    # ── _filter_by_polygon ────────────────────────────────────────────────────

    def test_filter_by_polygon_retains_interior_points(self):
        data = {
            "segment_id": np.array([1, 2, 3]),
            "longitude": np.array([-54.9, -54.9, 0.0]),
            "latitude": np.array([-3.0, -3.1, 50.0]),
        }
        poly = gpd.GeoDataFrame(geometry=[box(-56, -4, -53, -2)], crs="EPSG:4326")
        result = self.provider._filter_by_polygon(data, poly)
        self.assertEqual(len(result["segment_id"]), 2)

    def test_filter_by_polygon_empty_data_returns_unchanged(self):
        data = {"segment_id": np.array([])}
        poly = gpd.GeoDataFrame(geometry=[box(-56, -4, -53, -2)], crs="EPSG:4326")
        result = self.provider._filter_by_polygon(data, poly)
        self.assertEqual(len(result["segment_id"]), 0)

    def test_filter_by_polygon_accepts_precomputed_shapely_geometry(self):
        """Caller may pass a shapely geometry directly (no union_all needed)."""
        data = {
            "segment_id": np.array([1, 2]),
            "longitude": np.array([-54.9, 0.0]),
            "latitude": np.array([-3.0, 50.0]),
        }
        geom = box(-56, -4, -53, -2)
        result = self.provider._filter_by_polygon(data, geom)
        self.assertEqual(len(result["segment_id"]), 1)

    # ── _get_tiledb_spatial_domain ────────────────────────────────────────────

    def test_get_tiledb_spatial_domain_returns_correct_bounds(self):
        min_lon, max_lon, min_lat, max_lat = self.provider._get_tiledb_spatial_domain()
        self.assertEqual(min_lon, -180.0)
        self.assertEqual(max_lon, 180.0)
        self.assertEqual(min_lat, -90.0)
        self.assertEqual(max_lat, 90.0)

    def test_get_tiledb_spatial_domain_is_cached(self):
        r1 = self.provider._get_tiledb_spatial_domain()
        r2 = self.provider._get_tiledb_spatial_domain()
        self.assertIs(r1, r2)

    # ── close / reopen ────────────────────────────────────────────────────────

    def test_close_sets_array_handle_to_none(self):
        self.provider._get_array()  # ensure open
        self.provider.close()
        self.assertIsNone(self.provider._array_handle)
        self.assertIsNone(self.provider._schema_cache)
        self.assertIsNone(self.provider._metadata_cache)

    def test_get_array_reopens_after_close(self):
        self.provider.close()
        array = self.provider._get_array()
        self.assertTrue(array.isopen)

    # ── get_available_variables ───────────────────────────────────────────────

    def test_get_available_variables_returns_non_empty_dataframe(self):
        result = self.provider.get_available_variables()
        self.assertIsInstance(result, pd.DataFrame)
        self.assertFalse(result.empty)

    def test_get_available_variables_is_cached(self):
        r1 = self.provider.get_available_variables()
        r2 = self.provider.get_available_variables()
        self.assertIs(r1, r2)
