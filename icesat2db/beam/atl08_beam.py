# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#
import hashlib
from typing import Dict, Optional

import numpy as np
import pandas as pd

from icesat2db.beam.Beam import beam_handler
from icesat2db.granule.Granule import granule_handler


class ATL08Beam(beam_handler):
    """
    Represents a Level 2A (L2A) GEDI beam and processes the beam data.
    This class extracts geolocation and elevation data, applies quality filters,
    and returns the filtered beam data as a DataFrame.
    """

    def __init__(
        self, granule: granule_handler, beam: str, field_mapping: Dict[str, str]
    ):
        """
        Initialize the L2ABeam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, str]): A dictionary mapping fields to SDS names.
        """
        super().__init__(granule, beam, field_mapping)

        self._filtered_index: Optional[np.ndarray] = None  # Cache for filtered indices
        self.DEFAULT_QUALITY_FILTERS = {

            "h_te_uncertainty": lambda: (
                (self["land_segments/terrain/h_te_uncertainty"][()] < 3.402823e+23) &
                (self["land_segments/terrain/h_te_uncertainty"][()] > -999)
            ),

            "h_te_best_fit": lambda: (
                (self["land_segments/terrain/h_te_best_fit"][()] < 3.402823e+23) &
                (self["land_segments/terrain/h_te_best_fit"][()] > -999)
            ),

            "h_te_median": lambda: (
                    (self["land_segments/terrain/h_te_median"][()] < 3.402823e+23) |
                    (self["land_segments/terrain/h_te_median"][()] > -999)
            ),

            "h_canopy": lambda: self["land_segments/canopy/h_canopy"][()] < 3.402823e+23,
            "h_canopy_uncertainty": lambda: self["land_segments/canopy/h_canopy_uncertainty"][()] < 3.402823e+23,
            "urban_flag": lambda: self["land_segments/urban_flag"][()] == 0,
            "segment_watermask": lambda: self["land_segments/segment_watermask"][()] == 0,

        }

    def construct_shot_number(self) -> np.ndarray:
        """
        Construct shot numbers for the beam based on the number of shots.

        Returns:
            np.ndarray: An array of shot numbers.
        """

        dt = np.round(self["land_segments/delta_time"], 9)
        lat = np.round(self["land_segments/latitude"], 9)
        lon = np.round(self["land_segments/longitude"], 9)

        shot_str = (
                dt.astype(str) + "_" +
                lat.astype(str) + "_" +
                lon.astype(str)
        )

        return shot_str

    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Extract the main data for the beam, including time and elevation differences.
        This method applies quality filters to the data.

        Returns:
            Optional[Dict[str, np.ndarray]]: The filtered data as a dictionary or None if no data is present.
        """
        # Define IceSat-2 mission start time and calculate actual timestamps
        icesat2_count_start = pd.to_datetime("2018-01-01T00:00:00.000000Z")
        delta_time = self["land_segments/delta_time"][()]
        # instead of delta_time we can use start_delta_time and end_delta_time for segments as mid point


        # Initialize the data dictionary with calculated fields
        land_data = {
            "time": icesat2_count_start + pd.to_timedelta(delta_time, unit="seconds"),
            "longitude": self["land_segments/longitude"][()],
            "latitude": self["land_segments/latitude"][()],
            "shot_number": self.construct_shot_number()
        }

        # Populate data dictionary with fields from field mapping
        for key, source in self.field_mapper.items():
            sds_name = source["SDS_Name"]
            if "land_segments" in sds_name:
                land_data[key] = np.array(self[sds_name][()])

        # Apply quality filters and store filtered index
        self._filtered_index = self.apply_filter(
            land_data, filters=self.DEFAULT_QUALITY_FILTERS
        )

        # Filter the data based on the quality filters
        land_data_filtered = {
            key: value[self._filtered_index] for key, value in land_data.items()
        }

        return land_data_filtered if land_data_filtered else None
