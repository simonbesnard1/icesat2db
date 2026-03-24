# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de and urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


from enum import Enum


class IceSat2Product(Enum):
    """
    Enum representing different IceSat-2 data products.

    Attributes:
        ATL08 (str): Represents the ATL08 land/vegetation product.
    """

    ATL08 = "atl08"
    # ATL03 = "atl03"

    @classmethod
    def list_products(cls):
        """
        Get a list of all available IceSat-2 product names.

        :return: List of product names as strings.
        """
        return [product.value for product in cls]


# Constant for the WGS84 coordinate reference system (CRS)
WGS84 = "EPSG:4326"
"""
WGS84 Coordinate Reference System (CRS), commonly used for global latitude and longitude representation.
EPSG:4326 is the code representing the WGS84 standard.
"""
