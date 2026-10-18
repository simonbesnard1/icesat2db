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
from icesat2db.utils.atl03_atl08 import link_beam
from icesat2db.utils.segment_id import BEAM_ID_MAP, pack_segment_id


class ATL03Beam(beam_handler):
    """
    Represents a Level ATL03 IceSat2 beam and processes the photon-rate beam data.
    This class extracts per-photon geolocation and height data, applies a
    signal-confidence filter, and returns the filtered photon data as a DataFrame.
    """

    def __init__(
        self,
        granule: granule_handler,
        beam: str,
        field_mapping: Dict[str, str],
        confidence_column: int = 0,
        confidence_threshold: int = 3,
        atl08_file=None,
        retain_atl08_signal: bool = True,
    ):
        """
        Initialize the ATL03Beam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, str]): A dictionary mapping fields to SDS names.
            confidence_column (int): Column index into ``heights/signal_conf_ph``
                (photon x 5, one column per surface type: land, ocean, sea ice,
                land ice, inland water). Defaults to 0 (land).
            confidence_threshold (int): Minimum signal confidence to keep
                (0-4 scale: e.g. 3 = medium, 4 = high). Defaults to 3.
            atl08_file: Open, acquisition/release-matched ATL08 HDF5 file. The
                paired parser validates the files before constructing beams.
            retain_atl08_signal (bool): With a companion file, also retain
                ATL08 classes 1-3 regardless of ATL03 confidence (default True).
        """
        super().__init__(granule, beam, field_mapping)

        self._filtered_index: Optional[np.ndarray] = None
        self.confidence_column = confidence_column
        self.confidence_threshold = confidence_threshold
        self.atl08_file = atl08_file
        self.retain_atl08_signal = retain_atl08_signal

        def _signal_conf_filter():
            conf = self["heights/signal_conf_ph"][()]
            return conf[:, self.confidence_column] >= self.confidence_threshold

        self.DEFAULT_QUALITY_FILTERS = {"signal_conf_ph": _signal_conf_filter}

    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Extract the photon-rate data for the beam, expanding segment-rate fields
        (segment_id, and any configured geolocation/-sourced field) to photon
        rate, and applying the signal-confidence filter.

        Returns:
            Optional[Dict[str, np.ndarray]]: The filtered data as a dictionary or
            None if the beam has no photon data.
        """
        # Some beams exist as HDF5 groups but carry no heights/geolocation data
        if "heights" not in self or "geolocation" not in self:
            return None

        icesat2_count_start = pd.to_datetime("2018-01-01T00:00:00.000000Z")
        delta_time = self["heights/delta_time"][()]
        n_ph = len(delta_time)
        if n_ph == 0:
            return None

        seg_id = self["geolocation/segment_id"][()]
        seg_ph_cnt = self["geolocation/segment_ph_cnt"][()].astype(np.int64)
        if np.any(seg_ph_cnt < 0) or seg_ph_cnt.sum() != n_ph:
            raise ValueError("ATL03 segment photon counts do not match photon data")
        photon_seg_id = np.repeat(seg_id, seg_ph_cnt)

        rgt = int(self.parent_granule["orbit_info/rgt"][0])
        cycle = int(self.parent_granule["orbit_info/cycle_number"][0])
        beam_id = BEAM_ID_MAP[self.beam_name]

        photon_data = {
            "time": icesat2_count_start + pd.to_timedelta(delta_time, unit="seconds"),
            "longitude": self["heights/lon_ph"][()],
            "latitude": self["heights/lat_ph"][()],
            "segment_id": pack_segment_id(rgt, cycle, beam_id, photon_seg_id),
            "beam_id": np.full(n_ph, self.beam_name),
        }

        for key, source in self.field_mapper.items():
            sds_name = source["SDS_Name"]
            if "heights" in sds_name:
                photon_data[key] = np.array(self[sds_name][()])
            elif "geolocation" in sds_name:
                seg_val = np.array(self[sds_name][()])
                photon_data[key] = np.repeat(seg_val, seg_ph_cnt)
            elif "orbit_info" in sds_name:
                scalar = self.parent_granule[sds_name][0]
                photon_data[key] = np.full(n_ph, scalar)

        if self.atl08_file is not None:
            photon_data.update(
                link_beam(
                    self,
                    self.atl08_file.get(self.beam_name),
                    seg_id,
                    seg_ph_cnt,
                    n_ph,
                    rgt,
                    cycle,
                    beam_id,
                )
            )

        # Apply the signal-confidence filter and store the filtered index.
        self._filtered_index = self.apply_filter(
            photon_data, filters=self.DEFAULT_QUALITY_FILTERS
        )

        if self.atl08_file is not None and self.retain_atl08_signal:
            self._filtered_index |= photon_data["atl08_classed_pc_flag"] > 0

        photon_data_filtered = {
            key: value[self._filtered_index] for key, value in photon_data.items()
        }

        return photon_data_filtered if photon_data_filtered else None
