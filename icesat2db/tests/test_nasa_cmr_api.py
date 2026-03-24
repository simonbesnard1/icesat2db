# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


"""Test that the NASA CMR API has not changed recently.
This test will probably be flaky because it depends on the NASA CMR API.
However, if we don't run this test, they tend to silently disable old API
endpoints, which can break our code.
"""

import datetime as dt
import os
import unittest
from pathlib import Path

import geopandas as gpd
import yaml

from icesat2db.downloader import cmr_query
from icesat2db.utils import constants

data_dir = Path(__file__).parent / "data"


class TestNasaCmrApi(unittest.TestCase):

    def _get_test_data(self):
        os.chdir(
            os.path.dirname(__file__)
        )  # this is needed to import local files such as the config

        return (
            gpd.read_file(data_dir / "ne_110m_land.zip").iloc[[50]],
            dt.datetime(2020, 10, 1),
            dt.datetime(2020, 11, 1),
            self._get_earthdata_from_config(),
        )

    def _get_earthdata_from_config(self):
        with open("data/data_config.yml", "r") as file:
            return yaml.safe_load(file)["earth_data_info"]

    def test_atl08(self):
        geom, start_date, end_date, earth_data_info = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.IceSat2Product.ATL08,
            geom,
            start_date,
            end_date,
            earth_data_info,
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)


suite = unittest.TestLoader().loadTestsFromTestCase(TestNasaCmrApi)
