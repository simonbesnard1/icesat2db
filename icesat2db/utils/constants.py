# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


from enum import Enum
from typing import Any, Dict, List


class IceSat2Product(Enum):
    """
    Enum representing different IceSat-2 data products.

    Attributes:
        ATL08 (str): Represents the ATL08 land/vegetation product.
        ATL03 (str): Represents the ATL03 per-photon geolocated product.
    """

    ATL08 = "atl08"
    ATL03 = "atl03"

    @classmethod
    def list_products(cls):
        """
        Get a list of all available IceSat-2 product names.

        :return: List of product names as strings.
        """
        return [product.value for product in cls]


def configured_products(data_info: Dict[str, Any]) -> List[IceSat2Product]:
    """
    Return the IceSat2 products actually configured for ingestion, i.e. those
    with a ``level_<product>`` block present in ``data_info``.

    Products are independent: a config with only ``level_atl08`` requires and
    downloads only ATL08 (today's default behavior), while adding
    ``level_atl03`` opts into ATL03 as well, without forcing every granule to
    have both.
    """
    if (
        data_info.get("level_atl03", {}).get("link_atl08", False)
        and "level_atl08" not in data_info
    ):
        raise ValueError(
            "link_atl08 requires both level_atl03 and level_atl08 configuration"
        )
    return [
        product for product in IceSat2Product if f"level_{product.value}" in data_info
    ]


# Constant for the WGS84 coordinate reference system (CRS)
WGS84 = "EPSG:4326"
"""
WGS84 Coordinate Reference System (CRS), commonly used for global latitude and longitude representation.
EPSG:4326 is the code representing the WGS84 standard.
"""
