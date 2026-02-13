# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from enum import Enum


class IceSat2Product(Enum):
    """
    Enum representing different GEDI data products.

    Attributes:
        ATL08 (str): Represents the ATL08 product.
    """

    ATL08 = "atl08"
    # ATL03 = "atl03"
    
    @classmethod
    def list_products(cls):
        """
        Get a list of all available GEDI product names.

        :return: List of product names as strings.
        """
        return [product.value for product in cls]


# Constant for the WGS84 coordinate reference system (CRS)
WGS84 = "EPSG:4326"
"""
WGS84 Coordinate Reference System (CRS), commonly used for global latitude and longitude representation.
EPSG:4326 is the code representing the WGS84 standard.
"""
