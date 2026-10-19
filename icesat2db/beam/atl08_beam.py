# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


from typing import Dict, Optional

import numpy as np
import pandas as pd

from icesat2db.beam.Beam import beam_handler
from icesat2db.granule.Granule import granule_handler
from icesat2db.utils.segment_id import BEAM_ID_MAP, pack_segment_id


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

        # Only a segment without a valid terrain height is dropped: it has no
        # usable surface. Everything else (terrain-only segments, water, urban)
        # is kept and filtered at query time with IceSat2Provider.
        def _h_te_best_fit():
            v = self["land_segments/terrain/h_te_best_fit"][()]
            return (v < self._FILL) & (v > self._INVALID_LOW)

        self.DEFAULT_QUALITY_FILTERS = {"h_te_best_fit": _h_te_best_fit}

    # ATL08 float fill value; any float field at or above it becomes NaN.
    _FILL = np.float32(3.4028235e38)
    # Heights at or below this are invalid (lowest land surface is ~ -430 m).
    _INVALID_LOW = np.float32(-999)
    # Fields with the additional low invalid bound.
    LOW_INVALID_FIELDS = (
        "h_te_best_fit",
        "h_te_best_fit_20m",
        "h_te_median",
        "h_te_uncertainty",
        "h_canopy_20m",
    )

    def construct_segment_id(self) -> np.ndarray:
        """
        Construct a globally unique segment ID for ATL08 land segments.
        See :func:`icesat2db.utils.segment_id.pack_segment_id` for the bit layout.
        """
        rgt = int(self.parent_granule["orbit_info/rgt"][0])
        cycle = int(self.parent_granule["orbit_info/cycle_number"][0])
        beam_id = BEAM_ID_MAP[self.beam_name]
        seg_ids = self["land_segments/segment_id_beg"][()]

        return pack_segment_id(rgt, cycle, beam_id, seg_ids)

    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Extract the main data for the beam, including time and elevation differences.
        This method applies quality filters to the data.

        Returns:
            Optional[Dict[str, np.ndarray]]: The filtered data as a dictionary or None if no data is present.
        """
        # Some beams exist as HDF5 groups but have no land_segments subgroup
        # (no valid land observations on that pass) — skip them silently.
        if "land_segments" not in self:
            return None

        icesat2_count_start = pd.to_datetime("2018-01-01T00:00:00.000000Z")
        delta_time = self["land_segments/delta_time"][()]

        n_segs = len(delta_time)

        # Initialize the data dictionary with calculated fields
        land_data = {
            "time": icesat2_count_start + pd.to_timedelta(delta_time, unit="seconds"),
            "longitude": self["land_segments/longitude"][()],
            "latitude": self["land_segments/latitude"][()],
            "segment_id": self.construct_segment_id(),
            "beam_id": np.full(n_segs, self.beam_name),
        }

        # Populate data dictionary with fields from field mapping
        for key, source in self.field_mapper.items():
            sds_name = source["SDS_Name"]
            if "land_segments" in sds_name:
                land_data[key] = np.array(self[sds_name][()])
            elif "orbit_info" in sds_name:
                # orbit_info fields are scalars (one value per granule) —
                # broadcast to segment length so they can be filtered and written.
                scalar = self.parent_granule[sds_name][0]
                land_data[key] = np.full(n_segs, scalar)

        # Invalid values become NaN instead of dropping the segment, so a
        # terrain-only segment keeps its terrain fields with NaN canopy fields.
        for key in self.field_mapper:
            value = land_data.get(key)
            if not isinstance(value, np.ndarray) or value.dtype.kind != "f":
                continue
            value = value.astype(np.float32)
            invalid = ~np.isfinite(value) | (value >= self._FILL)
            if key in self.LOW_INVALID_FIELDS:
                invalid |= value <= self._INVALID_LOW
            value[invalid] = np.nan
            land_data[key] = value

        # Apply quality filters and store filtered index
        self._filtered_index = self.apply_filter(
            land_data, filters=self.DEFAULT_QUALITY_FILTERS
        )

        # Filter the data based on the quality filters
        land_data_filtered = {
            key: value[self._filtered_index] for key, value in land_data.items()
        }

        return land_data_filtered if land_data_filtered else None
