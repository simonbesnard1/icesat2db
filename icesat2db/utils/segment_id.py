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


# Field widths of :func:`pack_photon_id`; 11 + 8 + 3 + 22 + 19 = 63 bits.
_PHOTON_ID_BITS = {"rgt": 11, "cycle": 8, "beam": 3, "segment": 22, "offset": 19}

# Computed identity of every stored ATL03 photon (see pack_photon_id).
PHOTON_ID_VARIABLE = {
    "SDS_Name": "photon_id",
    "dtype": "int64",
    "description": "Unique photon id: RGT, cycle, beam, geolocation segment and "
    "photon offset within that segment, packed into int64",
    "long_name": "photon id",
    "units": "adimensional",
}


def pack_photon_id(
    rgt: int,
    cycle: int,
    beam_id: int,
    segment_ids: np.ndarray,
    offsets: np.ndarray,
) -> np.ndarray:
    """
    Construct a globally unique, release-independent ATL03 photon ID.

    Geolocation segment IDs count along the whole orbit, so a photon is
    identified by its track, cycle, beam, segment and offset within that
    segment without needing the granule region or filename.

    Bit layout (int64, sign bit unused):
      [62..52] RGT (1-1387)
      [51..44] cycle
      [43..41] beam (0-5)
      [40..19] geolocation segment_id
      [18..0]  zero-based photon offset within the segment

    Raises
    ------
    ValueError
        If any field exceeds its bit width.
    """
    segment_ids = np.asarray(segment_ids).astype(np.int64)
    offsets = np.asarray(offsets).astype(np.int64)
    fields = {
        "rgt": np.int64(rgt),
        "cycle": np.int64(cycle),
        "beam": np.int64(beam_id),
        "segment": segment_ids,
        "offset": offsets,
    }
    for name, values in fields.items():
        limit = 1 << _PHOTON_ID_BITS[name]
        if np.any((values < 0) | (values >= limit)):
            raise ValueError(f"Photon id field '{name}' outside [0, {limit}).")

    shift = 63
    packed = np.zeros(np.broadcast(segment_ids, offsets).shape, dtype=np.int64)
    for name, values in fields.items():
        shift -= _PHOTON_ID_BITS[name]
        packed |= values << shift
    return packed
