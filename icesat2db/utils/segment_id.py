# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


import numpy as np

BEAM_ID_MAP = {"gt1l": 0, "gt1r": 1, "gt2l": 2, "gt2r": 3, "gt3l": 4, "gt3r": 5}


def pack_segment_id(
    rgt: int, cycle: int, beam_id: int, segment_id_beg: np.ndarray
) -> np.ndarray:
    """
    Construct a globally unique segment ID.
    Encodes: RGT (14 bits) | cycle (8 bits) | beam (3 bits) | segment_id_beg (32 bits)

    Bit layout (int64):
      [63..43] RGT (1-1387)
      [42..35] cycle
      [34..32] beam (0-5)
      [31..0]  segment_id_beg

    Parameters
    ----------
    rgt : int
        Reference ground track.
    cycle : int
        Cycle number.
    beam_id : int
        Beam index (0-5), see ``BEAM_ID_MAP``.
    segment_id_beg : np.ndarray
        Segment id(s) to pack into the low 32 bits. Cast to int64 before
        shifting to avoid int32 overflow.

    Returns
    -------
    np.ndarray
        int64 array of packed, globally unique segment ids.
    """
    seg_ids = np.asarray(segment_id_beg).astype(np.int64)
    return (
        (np.int64(rgt) << 43)
        | (np.int64(cycle) << 35)
        | (np.int64(beam_id) << 32)
        | seg_ids
    )
