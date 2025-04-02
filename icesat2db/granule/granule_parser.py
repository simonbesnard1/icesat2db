# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from icesat2db.granule.Granule import granule_handler
from icesat2db.granule.atl08_granule import ATL08Granule
from icesat2db.granule.atl06_granule import ATL06Granule
from icesat2db.utils.constants import IceSat2Product


class GranuleParser:
    """
    Base class for parsing GEDI granule data into a GeoDataFrame.
    Provides common parsing logic for different GEDI product types.
    """

    def __init__(self, file: str, data_info: Optional[dict] = None):
        """
        Initialize the GranuleParser.

        Args:
            file (str): Path to the granule file.
            data_info (dict, optional): Dictionary containing relevant data structure information.
        """
        self.file = Path(file)
        if not self.file.exists():
            raise FileNotFoundError(f"Granule file {self.file} not found.")

        self.data_info = data_info if data_info else {}
        self.variables = None

    @staticmethod
    def parse_granule(granule: granule_handler) -> pd.DataFrame:
        """
        Parse a single granule and return a GeoDataFrame.

        Args:
            granule (Granule): The granule object to be parsed.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the parsed granule data.
        """
        granule_data = []
        for beam in granule.iter_beams():
            main_data = beam.main_data
            if main_data is not None:
                granule_data.append(main_data)

        if granule_data:
            try:
                df = pd.concat(granule_data, ignore_index=True)
                return df
            except Exception as e:
                raise ValueError(f"Error parsing granule data: {e}")

        return pd.DataFrame()  # Return empty dataframe if no data found

    def parse(self) -> pd.DataFrame:
        """
        Abstract method to be implemented by child classes for parsing specific granules.

        Raises:
            NotImplementedError: Child classes must implement this method.
        """
        raise NotImplementedError("This method should be implemented in child classes")


class ATL08GranuleParser(GranuleParser):
    """Parser for ATL08 granules."""

    def __init__(self, file: str, data_info: Optional[dict] = None):
        super().__init__(file, data_info)
        self.variables = self.data_info.get("level_atl08", {}).get("variables", [])

    def parse(self) -> pd.DataFrame:
        with ATL08Granule(self.file, self.variables) as granule:
            return self.parse_granule(granule)


class ATL06GranuleParser(GranuleParser):
    """Parser for L2B granules."""

    def __init__(self, file: str, data_info: Optional[dict] = None):
        super().__init__(file, data_info)
        self.variables = self.data_info.get("level_2b", {}).get("variables", [])

    def parse(self) -> pd.DataFrame:
        with ATL06Granule(self.file, self.variables) as granule:
            return self.parse_granule(granule)


def parse_h5_file(
    file: str, product: IceSat2Product, data_info: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Parse an HDF5 file based on the product type and return a GeoDataFrame.

    Args:
        file (str): Path to the HDF5 file.
        product (GediProduct): Type of GEDI product (L2A, L2B, L4A, L4C).
        data_info (dict, optional): Information about the data structure.

    Returns:
        gpd.GeoDataFrame: Parsed GeoDataFrame containing the granule data.

    Raises:
        ValueError: If the provided product is not supported.
    """
    parser_classes = {
        IceSat2Product.ATL08.value: ATL08GranuleParser,
        IceSat2Product.ATL06.value: ATL06GranuleParser,
    }

    parser_class = parser_classes.get(product)
    if parser_class is None:
        raise ValueError(f"Product {product.value} is not supported.")

    parser = parser_class(file, data_info or {})
    return parser.parse()
