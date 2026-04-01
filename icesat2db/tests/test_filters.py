# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import unittest

import numpy as np
import tiledb

from icesat2db.utils.filters import TileDBFilterPolicy


def _filter_names(fl: tiledb.FilterList):
    """Return list of filter class names from a FilterList."""
    return [type(fl[i]).__name__ for i in range(len(fl))]


class TestTileDBFilterPolicyFastMode(unittest.TestCase):
    """Tests with use_filters=False (default write-optimised mode)."""

    def setUp(self):
        self.policy = TileDBFilterPolicy(cfg={})

    def test_use_filters_default_false(self):
        self.assertFalse(self.policy.use_filters)

    def test_spatial_dim_filters_fast(self):
        fl = self.policy.spatial_dim_filters()
        self.assertIsInstance(fl, tiledb.FilterList)
        names = _filter_names(fl)
        self.assertEqual(names, ["ZstdFilter"])

    def test_time_dim_filters_fast_has_double_delta(self):
        fl = self.policy.time_dim_filters()
        names = _filter_names(fl)
        self.assertIn("DoubleDeltaFilter", names)
        self.assertIn("ZstdFilter", names)

    def test_time_dim_filters_fast_level_1(self):
        fl = self.policy.time_dim_filters()
        # Last filter is Zstd; level should be 1 in fast mode
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 1)

    def test_timestamp_filters_fast_has_double_delta(self):
        fl = self.policy.timestamp_filters()
        names = _filter_names(fl)
        self.assertIn("DoubleDeltaFilter", names)
        self.assertIn("ZstdFilter", names)

    def test_timestamp_filters_fast_level_1(self):
        fl = self.policy.timestamp_filters()
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 1)

    def test_filters_for_dtype_float32_fast_zstd_only(self):
        fl = self.policy.filters_for_dtype(np.float32)
        names = _filter_names(fl)
        self.assertEqual(names, ["ZstdFilter"])

    def test_filters_for_dtype_float64_fast_zstd_only(self):
        fl = self.policy.filters_for_dtype(np.float64)
        names = _filter_names(fl)
        self.assertEqual(names, ["ZstdFilter"])

    def test_filters_for_dtype_int8_fast_zstd_only(self):
        fl = self.policy.filters_for_dtype(np.int8)
        names = _filter_names(fl)
        self.assertEqual(names, ["ZstdFilter"])

    def test_filters_for_dtype_int64_fast_zstd_only(self):
        fl = self.policy.filters_for_dtype(np.int64)
        names = _filter_names(fl)
        self.assertEqual(names, ["ZstdFilter"])

    def test_filters_for_dtype_fast_zstd_level_1(self):
        fl = self.policy.filters_for_dtype(np.float32)
        self.assertEqual(fl[0].level, 1)

    def test_custom_default_zstd_level(self):
        policy = TileDBFilterPolicy(cfg={"default_zstd_level": 3})
        fl = policy.filters_for_dtype(np.float32)
        self.assertEqual(fl[0].level, 3)


class TestTileDBFilterPolicyFullMode(unittest.TestCase):
    """Tests with use_filters=True (full compression pipeline)."""

    def setUp(self):
        self.policy = TileDBFilterPolicy(cfg={"use_filters": True})

    def test_use_filters_true(self):
        self.assertTrue(self.policy.use_filters)

    def test_spatial_dim_filters_full(self):
        fl = self.policy.spatial_dim_filters()
        names = _filter_names(fl)
        self.assertEqual(names, ["ZstdFilter"])

    def test_spatial_dim_custom_level(self):
        policy = TileDBFilterPolicy(cfg={"use_filters": True, "spatial_zstd_level": 1})
        fl = policy.spatial_dim_filters()
        self.assertEqual(fl[0].level, 1)

    def test_time_dim_filters_full_level_3(self):
        fl = self.policy.time_dim_filters()
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 3)

    def test_timestamp_filters_full_level_2(self):
        fl = self.policy.timestamp_filters()
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 2)

    def test_filters_for_float32_full_has_byteshuffle(self):
        fl = self.policy.filters_for_dtype(np.float32)
        names = _filter_names(fl)
        self.assertIn("ByteShuffleFilter", names)
        self.assertIn("ZstdFilter", names)

    def test_filters_for_float64_full_has_byteshuffle(self):
        fl = self.policy.filters_for_dtype(np.float64)
        names = _filter_names(fl)
        self.assertIn("ByteShuffleFilter", names)

    def test_filters_for_float_full_zstd_level_4(self):
        fl = self.policy.filters_for_dtype(np.float32)
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 4)

    def test_filters_for_int8_full_has_bitwidth(self):
        fl = self.policy.filters_for_dtype(np.int8)
        names = _filter_names(fl)
        self.assertIn("BitWidthReductionFilter", names)
        self.assertIn("ZstdFilter", names)

    def test_filters_for_int16_full_has_bitwidth(self):
        fl = self.policy.filters_for_dtype(np.int16)
        names = _filter_names(fl)
        self.assertIn("BitWidthReductionFilter", names)

    def test_filters_for_int32_full_has_bitwidth(self):
        fl = self.policy.filters_for_dtype(np.int32)
        names = _filter_names(fl)
        self.assertIn("BitWidthReductionFilter", names)

    def test_filters_for_uint8_full_has_bitwidth(self):
        fl = self.policy.filters_for_dtype(np.uint8)
        names = _filter_names(fl)
        self.assertIn("BitWidthReductionFilter", names)

    def test_filters_for_int64_full_zstd_only(self):
        # int64 is wide — no BitWidthReduction
        fl = self.policy.filters_for_dtype(np.int64)
        names = _filter_names(fl)
        self.assertNotIn("BitWidthReductionFilter", names)
        self.assertIn("ZstdFilter", names)

    def test_filters_for_uint64_full_zstd_only(self):
        fl = self.policy.filters_for_dtype(np.uint64)
        names = _filter_names(fl)
        self.assertNotIn("BitWidthReductionFilter", names)

    def test_custom_float_zstd_level(self):
        policy = TileDBFilterPolicy(cfg={"use_filters": True, "float_zstd_level": 9})
        fl = policy.filters_for_dtype(np.float32)
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 9)

    def test_custom_int_zstd_level(self):
        policy = TileDBFilterPolicy(cfg={"use_filters": True, "int_zstd_level": 5})
        fl = policy.filters_for_dtype(np.int16)
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 5)

    def test_custom_time_zstd_level(self):
        policy = TileDBFilterPolicy(cfg={"use_filters": True, "time_zstd_level": 5})
        fl = policy.time_dim_filters()
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 5)

    def test_custom_timestamp_zstd_level(self):
        policy = TileDBFilterPolicy(
            cfg={"use_filters": True, "timestamp_zstd_level": 4}
        )
        fl = policy.timestamp_filters()
        zstd = fl[len(fl) - 1]
        self.assertEqual(zstd.level, 4)


class TestTileDBFilterPolicyNoCfg(unittest.TestCase):
    """TileDBFilterPolicy constructed with None cfg should behave like empty dict."""

    def test_none_cfg_uses_defaults(self):
        policy = TileDBFilterPolicy(cfg=None)
        self.assertFalse(policy.use_filters)
        fl = policy.filters_for_dtype(np.float32)
        self.assertIsInstance(fl, tiledb.FilterList)


if __name__ == "__main__":
    unittest.main()
