# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felixd@gfz.de and urbazaev@gfz.de
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

from typing import Dict, Optional

import numpy as np
import pandas as pd

from icesat2db.beam.Beam import beam_handler
from icesat2db.granule.Granule import granule_handler


class ATL08Beam(beam_handler):
    """
    Represents a Level ATL08 IceSat2 beam and processes the beam data.
    This class extracts geolocation and elevation data, applies quality filters,
    and returns the filtered beam data as a DataFrame.
    """

    def __init__(
        self, granule: granule_handler, beam: str, field_mapping: Dict[str, str]
    ):
        """
        Initialize the ATL08Beam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, str]): A dictionary mapping fields to SDS names.
        """
        super().__init__(granule, beam, field_mapping)

        self._filtered_index: Optional[np.ndarray] = None  # Cache for filtered indices
        self.DEFAULT_QUALITY_FILTERS = {
            "h_te_uncertainty": lambda: (
                (self["land_segments/terrain/h_te_uncertainty"][()] < 3.402823e23)
                & (self["land_segments/terrain/h_te_uncertainty"][()] > -999)
            ),
            "h_te_best_fit": lambda: (
                (self["land_segments/terrain/h_te_best_fit"][()] < 3.402823e23)
                & (self["land_segments/terrain/h_te_best_fit"][()] > -999)
            ),
            "h_te_median": lambda: (
                (self["land_segments/terrain/h_te_median"][()] < 3.402823e23)
                | (self["land_segments/terrain/h_te_median"][()] > -999)
            ),
            "h_canopy": lambda: self["land_segments/canopy/h_canopy"][()] < 3.402823e23,
            "h_canopy_uncertainty": lambda: self[
                "land_segments/canopy/h_canopy_uncertainty"
            ][()]
            < 3.402823e23,
            "urban_flag": lambda: self["land_segments/urban_flag"][()] == 0,
            "segment_watermask": lambda: self["land_segments/segment_watermask"][()]
            == 0,
        }

    def construct_segment_id(self) -> np.ndarray:
        """
        Construct a globally unique segment ID for ATL08 land segments.
        Encodes: RGT (14 bits) | cycle (8 bits) | beam (3 bits) | segment_id_beg (32 bits)

        Bit layout (int64):
          [63..43] RGT (1–1387)
          [42..35] cycle
          [34..32] beam (0–5)
          [31..0]  segment_id_beg
        """
        rgt = int(self.parent_granule["orbit_info/rgt"][0])
        cycle = int(self.parent_granule["orbit_info/cycle_number"][0])
        beam_map = {"gt1l": 0, "gt1r": 1, "gt2l": 2, "gt2r": 3, "gt3l": 4, "gt3r": 5}
        beam_id = beam_map[self.beam_name]
        seg_ids = self["land_segments/segment_id_beg"][()].astype(
            np.int64
        )  # int32 → int64 before shifting

        return (
            (np.int64(rgt) << 43)
            | (np.int64(cycle) << 35)
            | (np.int64(beam_id) << 32)
            | seg_ids
        )

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
            "segment_id": self.construct_segment_id(),
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
