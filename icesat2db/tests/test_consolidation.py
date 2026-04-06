# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import tempfile
import unittest
from unittest.mock import MagicMock, patch

import tiledb

from icesat2db.utils.tiledb_consolidation import (
    SpatialConsolidationPlan,
    SpatialConsolidationPlanner,
)

# ---------------------------------------------------------------------------
# SpatialConsolidationPlan tests
# ---------------------------------------------------------------------------


class TestSpatialConsolidationPlan(unittest.TestCase):

    def _make_plan(self):
        return SpatialConsolidationPlan(
            {
                0: {"num_fragments": 2, "fragment_uris": ["a", "b"]},
                1: {"num_fragments": 1, "fragment_uris": ["c"]},
            }
        )

    def test_len(self):
        plan = self._make_plan()
        self.assertEqual(len(plan), 2)

    def test_getitem(self):
        plan = self._make_plan()
        node = plan[0]
        self.assertEqual(node["num_fragments"], 2)
        self.assertEqual(node["fragment_uris"], ["a", "b"])

    def test_iter_yields_dicts(self):
        plan = self._make_plan()
        nodes = list(plan)
        self.assertEqual(len(nodes), 2)
        self.assertIsInstance(nodes[0], dict)

    def test_items_yields_id_dict_pairs(self):
        plan = self._make_plan()
        pairs = list(plan.items())
        self.assertEqual(len(pairs), 2)
        ids = [k for k, _ in pairs]
        self.assertIn(0, ids)
        self.assertIn(1, ids)

    def test_dump_returns_full_dict(self):
        plan = self._make_plan()
        d = plan.dump()
        self.assertIsInstance(d, dict)
        self.assertIn(0, d)
        self.assertIn(1, d)

    def test_empty_plan(self):
        plan = SpatialConsolidationPlan({})
        self.assertEqual(len(plan), 0)
        self.assertEqual(list(plan), [])


# ---------------------------------------------------------------------------
# _generate_plan tests (via the static method directly)
# ---------------------------------------------------------------------------


class TestGeneratePlan(unittest.TestCase):

    def _frag(self, uri, lat_min, lat_max, lon_min, lon_max, cell_num=10_000_000):
        return {
            "uri": uri,
            "latitude_range": (lat_min, lat_max),
            "longitude_range": (lon_min, lon_max),
            "cell_num": cell_num,
        }

    def test_empty_fragments_returns_empty(self):
        result = SpatialConsolidationPlanner._generate_plan([])
        self.assertEqual(result, {})

    def test_single_fragment_one_node(self):
        frags = [self._frag("f1", 0, 10, 0, 10)]
        result = SpatialConsolidationPlanner._generate_plan(frags)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["num_fragments"], 1)

    def test_two_non_overlapping_fragments_two_nodes(self):
        frags = [
            self._frag("f1", 0, 10, 0, 10),
            self._frag("f2", 20, 30, 20, 30),
        ]
        result = SpatialConsolidationPlanner._generate_plan(frags)
        self.assertEqual(len(result), 2)

    def test_two_overlapping_fragments_one_node(self):
        frags = [
            self._frag("f1", 0, 15, 0, 15),
            self._frag("f2", 10, 20, 10, 20),
        ]
        result = SpatialConsolidationPlanner._generate_plan(frags)
        self.assertEqual(len(result), 1)
        node = result[0]
        self.assertEqual(node["num_fragments"], 2)
        self.assertIn("f1", node["fragment_uris"])
        self.assertIn("f2", node["fragment_uris"])

    def test_touching_fragments_overlap(self):
        # lat ranges [0,10] and [10,20] touch at lat=10 → should overlap
        frags = [
            self._frag("f1", 0, 10, 0, 10),
            self._frag("f2", 10, 20, 0, 10),
        ]
        result = SpatialConsolidationPlanner._generate_plan(frags)
        self.assertEqual(len(result), 1)

    def test_three_fragments_chain_overlap(self):
        # f1 overlaps f2, f2 overlaps f3 → all three in one group
        frags = [
            self._frag("f1", 0, 10, 0, 10),
            self._frag("f2", 5, 15, 5, 15),
            self._frag("f3", 12, 20, 12, 20),
        ]
        result = SpatialConsolidationPlanner._generate_plan(frags)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["num_fragments"], 3)

    def test_mixed_overlap_gives_two_groups(self):
        # f1 + f2 overlap; f3 separate
        frags = [
            self._frag("f1", 0, 10, 0, 10),
            self._frag("f2", 5, 15, 5, 15),
            self._frag("f3", 50, 60, 50, 60),
        ]
        result = SpatialConsolidationPlanner._generate_plan(frags)
        self.assertEqual(len(result), 2)
        sizes = sorted(v["num_fragments"] for v in result.values())
        self.assertEqual(sizes, [1, 2])

    def test_all_fragments_assigned(self):
        frags = [self._frag(f"f{i}", i * 5, i * 5 + 4, 0, 10) for i in range(6)]
        result = SpatialConsolidationPlanner._generate_plan(frags)
        total = sum(v["num_fragments"] for v in result.values())
        self.assertEqual(total, 6)


# ---------------------------------------------------------------------------
# _extract_fragments tests (mocked FragmentInfoList)
# ---------------------------------------------------------------------------


class TestExtractFragments(unittest.TestCase):

    def _make_mock_fragment(self, uri, nonempty_domain):
        frag = MagicMock()
        frag.uri = uri
        frag.nonempty_domain = nonempty_domain
        return frag

    def test_extract_fragments_basic(self):
        mock_info = [
            self._make_mock_fragment(
                "/arr/__fragments/frag_a", ((0.0, 10.0), (5.0, 15.0))
            ),
            self._make_mock_fragment(
                "/arr/__fragments/frag_b", ((20.0, 30.0), (0.0, 10.0))
            ),
        ]
        result = SpatialConsolidationPlanner._extract_fragments(mock_info)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["uri"], "frag_a")
        self.assertEqual(result[0]["latitude_range"], (0.0, 10.0))
        self.assertEqual(result[1]["longitude_range"], (0.0, 10.0))

    def test_extract_fragments_empty(self):
        result = SpatialConsolidationPlanner._extract_fragments([])
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# SpatialConsolidationPlanner.compute tests (with local TileDB array)
# ---------------------------------------------------------------------------


class TestSpatialConsolidationPlannerCompute(unittest.TestCase):

    def test_compute_returns_plan(self):
        """Create a tiny local TileDB array, write two fragments, then compute."""
        with tempfile.TemporaryDirectory() as tmpdir:
            import numpy as np

            uri = f"{tmpdir}/test_array"

            # Create a simple sparse array
            lat_dim = tiledb.Dim(
                name="latitude", domain=(-90.0, 90.0), tile=10.0, dtype=np.float64
            )
            lon_dim = tiledb.Dim(
                name="longitude", domain=(-180.0, 180.0), tile=10.0, dtype=np.float64
            )
            dom = tiledb.Domain(lat_dim, lon_dim)
            attr = tiledb.Attr(name="value", dtype=np.float32)
            schema = tiledb.ArraySchema(domain=dom, attrs=[attr], sparse=True)
            tiledb.Array.create(uri, schema)

            ctx = tiledb.Ctx()

            # Write two spatially separate fragments
            with tiledb.open(uri, "w", ctx=ctx) as arr:
                arr[np.array([1.0, 2.0]), np.array([1.0, 2.0])] = {
                    "value": np.array([1.0, 2.0], dtype=np.float32)
                }
            with tiledb.open(uri, "w", ctx=ctx) as arr:
                arr[np.array([50.0, 51.0]), np.array([100.0, 101.0])] = {
                    "value": np.array([3.0, 4.0], dtype=np.float32)
                }

            plan = SpatialConsolidationPlanner.compute(uri, ctx)
            self.assertIsInstance(plan, SpatialConsolidationPlan)
            self.assertGreater(len(plan), 0)

    def test_compute_raises_on_bad_uri(self):
        ctx = tiledb.Ctx()
        with self.assertRaises(Exception):
            SpatialConsolidationPlanner.compute("/nonexistent/path/array", ctx)

    def test_compute_empty_array_returns_empty_plan(self):
        """Array with no fragments gives an empty plan."""
        with tempfile.TemporaryDirectory() as tmpdir:
            import numpy as np

            uri = f"{tmpdir}/empty_array"
            lat_dim = tiledb.Dim(
                name="latitude", domain=(-90.0, 90.0), tile=10.0, dtype=np.float64
            )
            lon_dim = tiledb.Dim(
                name="longitude", domain=(-180.0, 180.0), tile=10.0, dtype=np.float64
            )
            dom = tiledb.Domain(lat_dim, lon_dim)
            attr = tiledb.Attr(name="value", dtype=np.float32)
            schema = tiledb.ArraySchema(domain=dom, attrs=[attr], sparse=True)
            tiledb.Array.create(uri, schema)

            ctx = tiledb.Ctx()
            plan = SpatialConsolidationPlanner.compute(uri, ctx)
            self.assertEqual(len(plan), 0)


if __name__ == "__main__":
    unittest.main()
