# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


import logging
import os
import shutil
from typing import Dict, List, Optional, Tuple

import pandas as pd

from icesat2db.granule import granule_parser
from icesat2db.utils.constants import RECORD_ID

# Configure the logger
logger = logging.getLogger(__name__)


class IceSat2Granule:
    """
    IceSat2Granule handles the processing and management of IceSat2 granules, including parsing, joining,
    and saving the data to TileDB, as well as querying processed granules from a database.

    Attributes:
    -----------
    download_path : str
        Path where granules are downloaded.
    data_info : dict
        Dictionary containing relevant information about data, such as table names.
    """

    def __init__(self, download_path: str, data_info: dict):
        """
        Initialize the IceSat2Granule class.

        Parameters:
        -----------
        download_path : str
            Path where granules are downloaded.
        data_info : dict
            Dictionary containing relevant information about data.
        """
        self.download_path = download_path
        self.data_info = data_info

    def process_granule(
        self, row: List[Tuple[str, Tuple[str, Optional[str]]]]
    ) -> Tuple[str, Dict[str, pd.DataFrame]]:
        """
        Parse and validate each product of one granule.

        Parameters:
        -----------
        row : list
            ``(granule_key, (product, path))`` download results, one per product.

        Returns:
        -------
        Tuple[str, Dict[str, pd.DataFrame]]
            The granule key and ``{product: DataFrame}``. A product without valid
            records is omitted; an empty dict is a successfully parsed, empty
            granule.

        Raises:
        -------
        ValueError
            If a file is missing or a product cannot be parsed. Errors are never
            converted into an empty result, which would checkpoint lost data.
        """
        if not row:
            raise ValueError("No product files supplied.")
        granule_key = row[0][0]
        if any(item[0] != granule_key for item in row):
            raise ValueError("Product files belong to different granules.")
        granules = [item[1] for item in row]
        missing_product = [level for level, path in granules if path is None]
        if missing_product:
            raise ValueError(
                f"Granule {granule_key}: Missing HDF5 file(s) for levels: {missing_product}"
            )

        frames = self.parse_granules(granules, granule_key)
        validated = self._validate_dfs(frames, granule_key)
        # Only successful parsing may discard local downloads.
        granule_dir = os.path.join(self.download_path, granule_key)
        if os.path.exists(granule_dir):
            shutil.rmtree(granule_dir, ignore_errors=True)
        return granule_key, validated

    def parse_granules(
        self, granules: List[Tuple[str, str]], granule_key: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Parse each product file of a granule.

        Returns:
        --------
        dict
            ``{product: DataFrame}`` for every supplied product.

        Raises:
        -------
        ValueError
            If a product fails to parse, or a linked ATL03 file has no ATL08
            companion.
        """
        data_dict = {}
        companion = dict(granules).get("atl08")
        linked = self.data_info.get("level_atl03", {}).get("link_atl08", False)
        for product, file in granules:
            kwargs = {}
            if product == "atl03" and linked:
                kwargs["atl08_file"] = companion
            data = granule_parser.parse_h5_file(
                file, product, data_info=self.data_info, **kwargs
            )
            if data is None:
                raise ValueError(f"Granule {granule_key}: Failed to parse {product}.")
            data_dict[product] = data
        return data_dict

    @staticmethod
    def _validate_dfs(
        df_dict: Dict[str, pd.DataFrame], granule_key: str
    ) -> Dict[str, pd.DataFrame]:
        """
        Validate each product's parsed DataFrame independently. Each product is
        written to its own TileDB array (see IceSat2Database's ``product``
        parameter), so there is no cross-product join here.

        Empty frames are dropped; a non-empty frame without its record id is an
        error rather than silently skipped data.
        """
        validated = {}
        for product, df in df_dict.items():
            if df is None or df.empty:
                continue
            record_id = RECORD_ID.get(product, "segment_id")
            for column in {"segment_id", record_id}:
                if column not in df.columns:
                    raise ValueError(
                        f"Granule {granule_key}: {product} DataFrame missing '{column}'."
                    )
            validated[product] = df

        return validated
