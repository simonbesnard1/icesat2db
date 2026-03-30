# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
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
        "level_atl08": {
            "variables": {
                "segment_id": {
                    "SDS_Name": "segment_id",
                },
                "latitude_20m": {
                    "SDS_Name": "land_segments/latitude_20m",
                },
                "longitude_20m": {
                    "SDS_Name": "land_segments/longitude_20m",
                },
                "canopy_openness": {
                    "SDS_Name": "land_segments/canopy/canopy_openness",
                },
            },
        }
    }

    def _generic_test_parse_granule(self, file, data):
        import h5py

        beams = ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]

        with h5py.File(file, "r") as f:
            present_beams = [b for b in beams if b in f]

            # all beams present
            self.assertEqual(len(present_beams), 6)

            for beam in present_beams:
                hdf_len = len(f[beam]["land_segments"]["latitude_20m"])

                # parsed data must not exceed original size
                self.assertLessEqual(
                    len(data),
                    hdf_len * len(present_beams),  # rough upper bound
                    "Parsed data larger than previous",
                )

                # basic sanity: original beam not empty
                self.assertGreater(hdf_len, 0)

    def test_parse_granule_atl08(self):
        data = parse_h5_file(
            ATL08_NAME,
            IceSat2Product.ATL08.value,
            data_info=self._data_info,
        )
        self._generic_test_parse_granule(ATL08_NAME, data)
        # Some of the data is correct
        data_orig = h5py.File(ATL08_NAME, "r")
        # original ATL08 values (first segment, first 20m bin)
        lat = data_orig["gt1l"]["land_segments"]["latitude_20m"][0][0]
        lon = data_orig["gt1l"]["land_segments"]["longitude_20m"][0][0]

        # match against flattened column
        row = data.loc[data["latitude_20m_1"] == lat]

        self.assertFalse(row.empty, "No matching latitude found in parsed data")

        self.assertEqual(
            row["longitude_20m_1"].values[0],
            lon,
        )


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
