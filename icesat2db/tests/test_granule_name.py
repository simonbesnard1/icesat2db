# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de and urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


import unittest

from icesat2db.granule import granule_name

TEST_NAME = "ATL08_20181014001049_02350102_007_01.h5"
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
