# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import pathlib
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import h5py
import numpy as np

from icesat2db.downloader.data_downloader import (
    CMRDataDownloader,
    H5FileDownloader,
    IceSat2Downloader,
    _get_session,
    _normalize_entry,
)
from icesat2db.utils.constants import IceSat2Product

# ---------------------------------------------------------------------------
# _normalize_entry
# ---------------------------------------------------------------------------


class TestNormalizeEntry(unittest.TestCase):

    def test_four_tuple_passthrough(self):
        t = ("http://example.com/f.h5", "ATL08", "2020-01-01", 42.5)
        url, product, start_time, size = _normalize_entry(t)
        self.assertEqual(url, "http://example.com/f.h5")
        self.assertEqual(product, "ATL08")
        self.assertEqual(size, 42.5)

    def test_three_tuple_size_defaults_to_zero(self):
        t = ("http://example.com/f.h5", "ATL08", "2020-01-01")
        url, product, start_time, size = _normalize_entry(t)
        self.assertEqual(size, 0.0)

    def test_four_tuple_size_none_becomes_zero(self):
        t = ("http://example.com/f.h5", "ATL08", "2020-01-01", None)
        _, _, _, size = _normalize_entry(t)
        self.assertEqual(size, 0.0)

    def test_four_tuple_size_as_string_converted(self):
        t = ("http://example.com/f.h5", "ATL08", "2020-01-01", "123.4")
        _, _, _, size = _normalize_entry(t)
        self.assertAlmostEqual(size, 123.4)

    def test_bad_tuple_raises_value_error(self):
        with self.assertRaises(ValueError):
            _normalize_entry(("only", "two"))

    def test_five_element_raises_value_error(self):
        with self.assertRaises(ValueError):
            _normalize_entry(("a", "b", "c", 1.0, "extra"))


# ---------------------------------------------------------------------------
# _get_session
# ---------------------------------------------------------------------------


class TestGetSession(unittest.TestCase):

    def test_returns_requests_session(self):
        import requests

        session = _get_session()
        self.assertIsInstance(session, requests.Session)

    def test_same_thread_returns_same_session(self):
        s1 = _get_session()
        s2 = _get_session()
        self.assertIs(s1, s2)

    def test_different_threads_return_different_sessions(self):
        sessions = []

        def grab():
            sessions.append(_get_session())

        t1 = threading.Thread(target=grab)
        t2 = threading.Thread(target=grab)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(len(sessions), 2)
        self.assertIsNot(sessions[0], sessions[1])


# ---------------------------------------------------------------------------
# IceSat2Downloader
# ---------------------------------------------------------------------------


class TestIceSat2Downloader(unittest.TestCase):

    def test_download_raises_not_implemented(self):
        downloader = IceSat2Downloader()
        with self.assertRaises(NotImplementedError):
            downloader._download()


# ---------------------------------------------------------------------------
# CMRDataDownloader._filter_granules_with_all_products
# ---------------------------------------------------------------------------


class TestCMRDataDownloaderFilter(unittest.TestCase):

    def _product_values(self):
        return {p.value for p in IceSat2Product}

    def _make_granules_with_all_products(self, granule_id="G001"):
        """Build a granule dict that has every required product."""
        entries = [
            (
                f"http://example.com/{granule_id}/{p.value}.h5",
                p.value,
                "2020-01-01",
                1.0,
            )
            for p in IceSat2Product
        ]
        return {granule_id: entries}

    def _make_downloader(self):
        mock_geom = MagicMock()
        mock_geom.total_bounds = np.array([-10, -10, 10, 10])
        return CMRDataDownloader(geom=mock_geom)

    def test_keeps_granule_with_all_products(self):
        downloader = self._make_downloader()
        granules = self._make_granules_with_all_products("G001")
        result = downloader._filter_granules_with_all_products(granules)
        self.assertIn("G001", result)

    def test_drops_granule_missing_a_product(self):
        downloader = self._make_downloader()
        # Patch IceSat2Product to temporarily require two products so we can
        # test the "missing product" branch without changing production code.
        from enum import Enum

        FakeProduct = Enum("FakeProduct", {"ATL08": "atl08", "ATL03": "atl03"})
        with patch("icesat2db.downloader.data_downloader.IceSat2Product", FakeProduct):
            granules = {
                "G002": [("http://example.com/file.h5", "atl08", "2020-01-01", 1.0)]
                # "atl03" is absent → should be dropped
            }
            result = downloader._filter_granules_with_all_products(granules)
        self.assertNotIn("G002", result)

    def test_deduplicates_duplicate_product_entries(self):
        downloader = self._make_downloader()
        # ATL08 appears twice; only one entry should be kept per product
        entries = [
            ("http://example.com/dup1.h5", "atl08", "2020-01-01", 1.0),
            ("http://example.com/dup2.h5", "atl08", "2020-01-02", 2.0),
        ]
        granules = {"G003": entries}
        result = downloader._filter_granules_with_all_products(granules)
        # G003 should be kept (ATL08 is the only required product)
        self.assertIn("G003", result)
        # Only one entry per product
        product_counts = {}
        for entry in result["G003"]:
            _, prod, _, _ = _normalize_entry(entry)
            product_counts[prod] = product_counts.get(prod, 0) + 1
        for count in product_counts.values():
            self.assertEqual(count, 1)

    def test_empty_granules_returns_empty(self):
        downloader = self._make_downloader()
        result = downloader._filter_granules_with_all_products({})
        self.assertEqual(result, {})

    def test_multiple_granules_filtered_correctly(self):
        downloader = self._make_downloader()
        from enum import Enum

        FakeProduct = Enum("FakeProduct", {"ATL08": "atl08", "ATL03": "atl03"})
        with patch("icesat2db.downloader.data_downloader.IceSat2Product", FakeProduct):
            good = {
                "GOOD": [
                    ("http://x.com/atl08.h5", "atl08", "2020-01-01", 1.0),
                    ("http://x.com/atl03.h5", "atl03", "2020-01-01", 1.0),
                ]
            }
            bad = {"BAD": [("http://x.com/f.h5", "atl08", "2020-01-01", 1.0)]}
            all_granules = {**good, **bad}
            result = downloader._filter_granules_with_all_products(all_granules)
        self.assertIn("GOOD", result)
        self.assertNotIn("BAD", result)


# ---------------------------------------------------------------------------
# H5FileDownloader
# ---------------------------------------------------------------------------


class TestH5FileDownloader(unittest.TestCase):

    def test_init_sets_download_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dl = H5FileDownloader(download_path=tmpdir)
            self.assertEqual(dl.download_path, pathlib.Path(tmpdir))

    def test_init_default_download_path(self):
        dl = H5FileDownloader()
        self.assertEqual(dl.download_path, pathlib.Path("."))

    def test_is_hdf5_valid_returns_true_for_real_h5(self):
        h5_path = (
            pathlib.Path(__file__).parent
            / "data"
            / "ATL08_20181014001049_02350102_007_01.h5"
        )
        if not h5_path.exists():
            self.skipTest("Test H5 file not found")
        dl = H5FileDownloader()
        self.assertTrue(dl._is_hdf5_valid(h5_path))

    def test_is_hdf5_valid_returns_false_for_text_file(self):
        with tempfile.NamedTemporaryFile(suffix=".h5", mode="w", delete=False) as f:
            f.write("this is not an hdf5 file")
            path = pathlib.Path(f.name)
        try:
            dl = H5FileDownloader()
            self.assertFalse(dl._is_hdf5_valid(path))
        finally:
            path.unlink(missing_ok=True)

    def test_is_hdf5_valid_returns_false_for_missing_file(self):
        dl = H5FileDownloader()
        self.assertFalse(dl._is_hdf5_valid(pathlib.Path("/nonexistent/file.h5")))

    def test_download_returns_early_for_valid_existing_file(self):
        """If the final file already exists and is valid HDF5, download returns without fetching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dl = H5FileDownloader(download_path=tmpdir)
            granule_key = "TEST_GRANULE"
            product = MagicMock()
            product.name = "ATL08"
            product.value = "ATL08"

            # Copy the test H5 into the expected location
            src = (
                pathlib.Path(__file__).parent
                / "data"
                / "ATL08_20181014001049_02350102_007_01.h5"
            )
            if not src.exists():
                self.skipTest("Test H5 file not found")

            dest_dir = pathlib.Path(tmpdir) / granule_key
            dest_dir.mkdir()
            import shutil

            shutil.copy(src, dest_dir / "ATL08.h5")

            # download() should return without making any HTTP request
            with patch(
                "icesat2db.downloader.data_downloader._get_session"
            ) as mock_sess:
                result_key, (result_product, result_path) = dl.download(
                    granule_key, "http://example.com/fake.h5", product
                )
                # _get_session() is always called to obtain the session object,
                # but no HTTP request should be made on the fast path.
                mock_sess.return_value.get.assert_not_called()

            self.assertEqual(result_key, granule_key)
            self.assertEqual(result_product, "ATL08")
            self.assertTrue(pathlib.Path(result_path).exists())

    def test_download_creates_granule_directory(self):
        """Even on a download attempt that fails, the granule directory should be created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            dl = H5FileDownloader(download_path=tmpdir)
            granule_key = "NEW_GRANULE"
            product = MagicMock()
            product.name = "ATL08"
            product.value = "ATL08"

            mock_session = MagicMock()
            # RuntimeError is NOT in the @retry exception list, so it propagates
            # immediately without triggering retries (which have long sleep delays).
            # mkdir() is called before session.get(), so the directory is still created.
            mock_session.get.side_effect = RuntimeError(
                "stop — not a retried exception"
            )

            with patch(
                "icesat2db.downloader.data_downloader._get_session",
                return_value=mock_session,
            ):
                try:
                    dl.download(granule_key, "http://example.com/fake.h5", product)
                except Exception:
                    pass  # Expected

            expected_dir = pathlib.Path(tmpdir) / granule_key
            self.assertTrue(expected_dir.exists())


if __name__ == "__main__":
    unittest.main()
