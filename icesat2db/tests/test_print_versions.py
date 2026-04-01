# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import io
import unittest

from icesat2db.utils.print_versions import (
    get_sys_info,
    netcdf_and_hdf5_versions,
    show_versions,
)


class TestGetSysInfo(unittest.TestCase):
    def test_returns_list(self):
        result = get_sys_info()
        self.assertIsInstance(result, list)

    def test_all_entries_are_tuples(self):
        result = get_sys_info()
        for item in result:
            self.assertIsInstance(item, tuple)
            self.assertEqual(len(item), 2)

    def test_contains_python_key(self):
        result = get_sys_info()
        keys = [k for k, _ in result]
        self.assertIn("python", keys)

    def test_contains_commit_key(self):
        result = get_sys_info()
        keys = [k for k, _ in result]
        self.assertIn("commit", keys)

    def test_contains_os_key(self):
        result = get_sys_info()
        keys = [k for k, _ in result]
        self.assertIn("OS", keys)

    def test_contains_byteorder(self):
        result = get_sys_info()
        keys = [k for k, _ in result]
        self.assertIn("byteorder", keys)


class TestNetcdfAndHdf5Versions(unittest.TestCase):
    def test_returns_list(self):
        result = netcdf_and_hdf5_versions()
        self.assertIsInstance(result, list)

    def test_contains_libhdf5_key(self):
        result = netcdf_and_hdf5_versions()
        keys = [k for k, _ in result]
        self.assertIn("libhdf5", keys)

    def test_contains_libnetcdf_key(self):
        result = netcdf_and_hdf5_versions()
        keys = [k for k, _ in result]
        self.assertIn("libnetcdf", keys)

    def test_values_are_string_or_none(self):
        result = netcdf_and_hdf5_versions()
        for _, v in result:
            self.assertTrue(v is None or isinstance(v, str))


class TestShowVersions(unittest.TestCase):
    def test_runs_without_error(self):
        buf = io.StringIO()
        show_versions(file=buf)

    def test_output_contains_installed_versions_header(self):
        buf = io.StringIO()
        show_versions(file=buf)
        output = buf.getvalue()
        self.assertIn("INSTALLED VERSIONS", output)

    def test_output_contains_python(self):
        buf = io.StringIO()
        show_versions(file=buf)
        output = buf.getvalue()
        self.assertIn("python", output)

    def test_output_contains_numpy(self):
        buf = io.StringIO()
        show_versions(file=buf)
        output = buf.getvalue()
        self.assertIn("numpy", output)

    def test_output_contains_tiledb(self):
        buf = io.StringIO()
        show_versions(file=buf)
        output = buf.getvalue()
        self.assertIn("tiledb", output)

    def test_output_contains_pandas(self):
        buf = io.StringIO()
        show_versions(file=buf)
        output = buf.getvalue()
        self.assertIn("pandas", output)


if __name__ == "__main__":
    unittest.main()
