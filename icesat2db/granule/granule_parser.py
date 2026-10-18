# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


from contextlib import ExitStack
from pathlib import Path
from typing import Dict, Optional

import h5py
import pandas as pd

from icesat2db.granule.atl03_granule import ATL03Granule
from icesat2db.granule.atl08_granule import ATL08Granule
from icesat2db.granule.Granule import granule_handler
from icesat2db.utils.atl03_atl08 import validate_pair
from icesat2db.utils.constants import IceSat2Product


class GranuleParser:
    """
    Base class for parsing IceSat2 granule data into a GeoDataFrame.
    Provides common parsing logic for different IceSat2 product types.
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


class ATL03GranuleParser(GranuleParser):
    """Parser for ATL03 granules."""

    def __init__(self, file: str, data_info: Optional[Dict] = None, atl08_file=None):
        super().__init__(file, data_info)
        self.atl08_file = atl08_file
        level_info = self.data_info.get("level_atl03", {})
        self.variables = level_info.get("variables", [])
        self.confidence_column = level_info.get("confidence_column", 0)
        self.confidence_threshold = level_info.get("confidence_threshold", 3)
        self.link_atl08 = level_info.get("link_atl08", False)
        self.retain_atl08_signal = level_info.get("retain_atl08_signal", True)

    def parse(self) -> pd.DataFrame:
        with ExitStack() as stack:
            companion = None
            if self.link_atl08:
                if self.atl08_file is None:
                    raise ValueError("link_atl08 requires the matching ATL08 file")
                validate_pair(self.file, self.atl08_file)
                companion = stack.enter_context(h5py.File(self.atl08_file, "r"))
            granule = stack.enter_context(
                ATL03Granule(
                    self.file,
                    self.variables,
                    confidence_column=self.confidence_column,
                    confidence_threshold=self.confidence_threshold,
                    atl08_file=companion,
                    retain_atl08_signal=self.retain_atl08_signal,
                )
            )
            if companion is not None:
                for key in ("rgt", "cycle_number"):
                    if (
                        granule[f"orbit_info/{key}"][0]
                        != companion[f"orbit_info/{key}"][0]
                    ):
                        raise ValueError("ATL03/ATL08 orbit metadata differ")
            data = self.parse_granule(granule)
            if companion is not None:
                data["atl08_source_granule"] = Path(self.atl08_file).name
            return data


def parse_h5_file(
    file: str,
    product: IceSat2Product,
    data_info: Optional[Dict] = None,
    atl08_file=None,
) -> pd.DataFrame:
    """
    Parse an HDF5 file based on the product type and return a GeoDataFrame.

    Args:
        file (str): Path to the HDF5 file.
        product (IceSat2Product): Type of IceSat2 product (ATL08).
        data_info (dict, optional): Information about the data structure.
        atl08_file: Companion ATL08 path, required when parsing ATL03 with
            ``level_atl03.link_atl08`` enabled. Matching occurs before filtering.

    Returns:
        gpd.GeoDataFrame: Parsed GeoDataFrame containing the granule data.

    Raises:
        ValueError: If the provided product is not supported.
    """
    parser_classes = {
        IceSat2Product.ATL08.value: ATL08GranuleParser,
        IceSat2Product.ATL03.value: ATL03GranuleParser,
    }

    parser_class = parser_classes.get(product)
    if parser_class is None:
        raise ValueError(f"Product {product.value} is not supported.")

    kwargs = {"atl08_file": atl08_file} if parser_class is ATL03GranuleParser else {}
    parser = parser_class(file, data_info or {}, **kwargs)
    data = parser.parse()
    if (data_info or {}).get("level_atl03", {}).get("link_atl08", False):
        data["source_granule"] = Path(file).name
    return data
