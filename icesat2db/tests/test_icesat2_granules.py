# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felixd@gfz.de and urbazaev@gfz.de
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import os
import pathlib
import unittest
import warnings

import h5py

from icesat2db.granule.granule_parser import parse_h5_file
from icesat2db.utils.constants import IceSat2Product

# THIS_DIR = pathlib.Path(__name__).parent
THIS_DIR = pathlib.Path.cwd().parent
ATL08_NAME = "./data/ATL08_20181014001049_02350102_007_01.h5"


class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        warnings.simplefilter("ignore", DeprecationWarning)
        os.chdir(os.path.dirname(__file__))

    _data_info = {
        "level_2a": {
            "quality_filter": "None",
            "variables": {
                "shot_number": {
                    "SDS_Name": "shot_number",
                },
                "beam_name": {
                    "SDS_Name": "name",
                },
                "lat_lowestmode": {
                    "SDS_Name": "lat_lowestmode",
                },
                "lon_lowestmode": {
                    "SDS_Name": "lon_lowestmode",
                },
                "rh": {
                    "SDS_Name": "rh",
                },
            },
        }
    }

    def _generic_test_parse_granule(self, file, data):
        # All beams are non-empty
        # (Not true for all files -- but true for the test files)
        beam_data = data.groupby("beam_name").count()
        self.assertEqual(len(beam_data), 8)
        for beam in beam_data.index:
            self.assertNotEqual(beam_data.loc[beam, "latitude"], 0)

        data_orig = h5py.File(file, "r")
        for beam in beam_data.index:
            hdf_beam_len = len(data_orig[beam]["latitude"])
            # this test will always return different results, as long as the quality filter gets applied
            # self.assertEqual(beam_data.loc[beam, "shot_number"], hdf_beam_len)

            # right now we check if the quality filter gets applied, i.e. we get less entries with the parsed data
            # than with the original data
            self.assertLessEqual(
                beam_data.loc[beam, "latitude"],
                hdf_beam_len,
                "Quality filter returned more data than before",
            )

    def test_parse_granule_atl08(self):
        data = parse_h5_file(
            ATL08_NAME,
            IceSat2Product.ATL08.value,
            data_info=self._data_info,
        )
        self._generic_test_parse_granule(ATL08_NAME, data)
        # Some of the data is correct
        data_orig = h5py.File(ATL08_NAME, "r")
        lat = data_orig["gt1l"]["latitude"][0]
        lon = data_orig["gt1l"]["longitude"][0]

        row = data.loc[data["latitude"] == lat]
        self.assertEqual(row["longitude"].values[0], lon)


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
