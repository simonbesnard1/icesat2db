# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import unittest

from icesat2db.granule import granule_name

TEST_NAME = "ATL08_20181014001049_0235 01 02 _007_01.h5"
# ATL08_[year][month][day][hour][minute][second]_[ref_ground_track][cycle_number][segment_number]_[version]_[revision].h5


class TestCase(unittest.TestCase):
    def test_parse_name(self):
        parsed = granule_name.parse_granule_filename(TEST_NAME)
        self.assertEqual(parsed.product, "ATL08")
        self.assertEqual(parsed.year, "2018")
        self.assertEqual(parsed.month, "10")
        self.assertEqual(parsed.day, "14")
        self.assertEqual(parsed.hour, "00")
        self.assertEqual(parsed.minute, "10")
        self.assertEqual(parsed.second, "49")
        self.assertEqual(parsed.ref_ground_track, "0235")
        self.assertEqual(parsed.cycle_number, "01")
        self.assertEqual(parsed.segment_number, "02")
        self.assertEqual(parsed.version, "007")
        self.assertEqual(parsed.revision, "01")


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
