# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from icesat2db.downloader.authentication import EarthDataAuthenticator


def _write_valid_netrc(path: Path):
    """Write a .netrc file that contains the earthdata entry."""
    path.write_text("machine urs.earthdata.nasa.gov login user password pass\n")
    path.chmod(0o600)


def _write_valid_cookie(path: Path):
    """Write a non-empty cookie file."""
    path.write_text("# Netscape HTTP Cookie File\nsome_cookie_data\n")


class TestEarthDataAuthenticatorInit(unittest.TestCase):

    def test_default_earth_data_path_is_home(self):
        auth = EarthDataAuthenticator()
        self.assertEqual(auth.earth_data_path, Path.home())

    def test_custom_earth_data_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            self.assertEqual(auth.earth_data_path, Path(tmpdir))

    def test_strict_with_missing_netrc_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(FileNotFoundError):
                EarthDataAuthenticator(earth_data_dir=tmpdir, strict=True)

    def test_strict_with_valid_credentials_does_not_raise(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            _write_valid_netrc(p / ".netrc")
            _write_valid_cookie(p / ".cookies")
            # Should not raise
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir, strict=True)
            self.assertIsNotNone(auth)

    def test_non_strict_with_missing_netrc_does_not_raise(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # No .netrc — strict=False should not raise
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir, strict=False)
            self.assertIsNotNone(auth)


class TestCredentialsInNetrc(unittest.TestCase):

    def test_missing_netrc_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            self.assertFalse(auth._credentials_in_netrc())

    def test_netrc_without_earthdata_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            (p / ".netrc").write_text("machine other.host.com login x password y\n")
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            self.assertFalse(auth._credentials_in_netrc())

    def test_valid_netrc_missing_cookie_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            _write_valid_netrc(p / ".netrc")
            # No cookie file
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            self.assertFalse(auth._credentials_in_netrc())

    def test_valid_netrc_empty_cookie_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            _write_valid_netrc(p / ".netrc")
            (p / ".cookies").write_text("")  # empty
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            self.assertFalse(auth._credentials_in_netrc())

    def test_valid_netrc_and_cookie_returns_true(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            _write_valid_netrc(p / ".netrc")
            _write_valid_cookie(p / ".cookies")
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            self.assertTrue(auth._credentials_in_netrc())

    def test_exception_during_check_returns_false(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            # Patch Path.exists at the class level to raise an unexpected OSError
            with patch("pathlib.Path.exists", side_effect=OSError("disk error")):
                result = auth._credentials_in_netrc()
            self.assertFalse(result)


class TestAuthenticate(unittest.TestCase):

    def test_authenticate_when_credentials_present_does_not_prompt(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            p = Path(tmpdir)
            _write_valid_netrc(p / ".netrc")
            _write_valid_cookie(p / ".cookies")
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            # Should complete without prompting or raising
            with patch.object(auth, "_prompt_for_credentials") as mock_prompt:
                auth.authenticate()
                mock_prompt.assert_not_called()

    def test_authenticate_strict_without_credentials_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # strict=True set after construction to bypass __init__ check
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            auth.strict = True
            with self.assertRaises(FileNotFoundError):
                auth.authenticate()

    def test_authenticate_non_strict_calls_prompt_when_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir, strict=False)
            with (
                patch.object(auth, "_prompt_for_credentials") as mock_prompt,
                patch.object(auth, "_fetch_earthdata_cookies"),
            ):
                auth.authenticate()
                mock_prompt.assert_called_once()


class TestAddNetrcCredentials(unittest.TestCase):

    def test_adds_earthdata_entry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            auth.username = "testuser"
            auth.password = "testpass"
            auth._add_netrc_credentials()
            content = auth.netrc_file.read_text()
            self.assertIn("urs.earthdata.nasa.gov", content)
            self.assertIn("testuser", content)
            self.assertIn("testpass", content)

    def test_sets_secure_permissions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            auth.username = "u"
            auth.password = "p"
            auth._add_netrc_credentials()
            import stat

            mode = auth.netrc_file.stat().st_mode
            # Check that group and other have no permissions
            self.assertEqual(mode & stat.S_IRWXG, 0)
            self.assertEqual(mode & stat.S_IRWXO, 0)


class TestFetchEarthdataCookies(unittest.TestCase):

    def test_calls_wget_subprocess(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0)
                auth._fetch_earthdata_cookies()
                mock_run.assert_called_once()
                args = mock_run.call_args[0][0]
                self.assertEqual(args[0], "wget")

    def test_raises_on_subprocess_failure(self):
        import subprocess

        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            with patch(
                "subprocess.run", side_effect=subprocess.CalledProcessError(1, "wget")
            ):
                with self.assertRaises(subprocess.CalledProcessError):
                    auth._fetch_earthdata_cookies()

    def test_creates_cookie_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            auth = EarthDataAuthenticator(earth_data_dir=tmpdir)
            with patch("subprocess.run"):
                auth._fetch_earthdata_cookies()
            self.assertTrue(auth.cookie_file.exists())


if __name__ == "__main__":
    unittest.main()
