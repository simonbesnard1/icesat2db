# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import logging
import numpy as np
import tiledb
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class TileDBFilterPolicy:
    """
    Encapsulates TileDB filter selection logic for attributes and dimensions.

    Uses:
    - Config to control Zstd levels / tuning.
    - Dtype-based rules to avoid per-variable special cases.

    Key improvements over prior version:
    - DeltaFilter added before ByteShuffle for float attributes, exploiting
      spatial locality from Hilbert cell ordering (large gains on correlated
      GEDI variables like agbd, rh_*, elevation).
    - DoubleDeltaFilter on timestamp_ns (monotonic int64 ns values — BitWidthReduction
      was doing nothing useful on ~1.7e18 values).
    - Silent BitWidthReduction failures now logged as warnings instead of swallowed.
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None) -> None:
        """
        Parameters
        ----------
        cfg : dict, optional
            Sub-dictionary of your global config relevant to TileDB.
            Typically `config.get("tiledb", {})`.
        """
        self.cfg = cfg or {}

    # ------------------------------------------------------------------
    # Dimension filters
    # ------------------------------------------------------------------

    def spatial_dim_filters(self, scale_factor: float = 1e-6) -> tiledb.FilterList:
        """
        Filters for latitude/longitude dimensions.

        FloatScale converts float64 → int32 at write time (~0.11 m precision
        at factor=1e-6, well within GEDI's 25 m footprint). DoubleDelta then
        exploits the resulting near-monotonic integer sequences. BitWidthReduction
        shrinks the delta residuals further before Zstd.

        Note: if write CPU is the bottleneck, consider factor=1e-5 (bytewidth=4)
        to reduce FloatScale overhead while staying within GEDI shot precision.
        """
        return tiledb.FilterList(
            [
                tiledb.FloatScaleFilter(
                    factor=scale_factor,
                    offset=0.0,
                    bytewidth=4,
                ),
                tiledb.DoubleDeltaFilter(),
                tiledb.BitWidthReductionFilter(),
                tiledb.ZstdFilter(level=3),
            ]
        )

    def time_dim_filters(self) -> tiledb.FilterList:
        """
        Filters for time dimension (int64 days-since-epoch, monotonic).
        DoubleDelta is highly effective on monotonic integer sequences.
        """
        return tiledb.FilterList(
            [
                tiledb.DoubleDeltaFilter(),
                tiledb.ZstdFilter(level=3),
            ]
        )

    # ------------------------------------------------------------------
    # Attribute filters (dtype-based)
    # ------------------------------------------------------------------

    def _filters_float32(self) -> tiledb.FilterList:
        """
        float32 attributes (agbd, rh_*, most GEDI measurements).

        DeltaFilter is added before ByteShuffle to exploit spatial correlation
        between adjacent cells in Hilbert order. This is especially impactful
        for the 101 rh_* profile columns where neighbouring shots have highly
        correlated canopy profiles.
        """
        lvl = int(self.cfg.get("float32_zstd_level", 4))
        return tiledb.FilterList(
            [
                tiledb.DeltaFilter(),       # exploits Hilbert spatial locality
                tiledb.ByteShuffleFilter(),  # then reorder bytes by significance
                tiledb.ZstdFilter(level=lvl),
            ]
        )

    def _filters_float64(self) -> tiledb.FilterList:
        """float64 attributes — same strategy as float32."""
        lvl = int(self.cfg.get("float64_zstd_level", 4))
        return tiledb.FilterList(
            [
                tiledb.DeltaFilter(),
                tiledb.ByteShuffleFilter(),
                tiledb.ZstdFilter(level=lvl),
            ]
        )

    def _filters_flags_uint8(self) -> tiledb.FilterList:
        """
        Flags / enums (degrade_flag, etc.).
        BitWidthReduction + RLE is very effective for low-cardinality uint8 columns.
        """
        lvl = int(self.cfg.get("flags_zstd_level", 3))
        fl = []
        try:
            fl.append(tiledb.BitWidthReductionFilter())
        except Exception as e:
            logger.warning(f"BitWidthReductionFilter unavailable, skipping: {e}")
        fl.extend(
            [
                tiledb.RleFilter(),
                tiledb.ZstdFilter(level=lvl),
            ]
        )
        return tiledb.FilterList(fl)

    def _filters_int_generic(self) -> tiledb.FilterList:
        """Generic integer attributes."""
        lvl = int(self.cfg.get("int_zstd_level", 3))
        fl = []
        try:
            fl.append(tiledb.BitWidthReductionFilter())
        except Exception as e:
            logger.warning(f"BitWidthReductionFilter unavailable, skipping: {e}")
        fl.append(tiledb.ZstdFilter(level=lvl))
        return tiledb.FilterList(fl)

    def _filters_utf8(self) -> tiledb.FilterList:
        """String attributes."""
        lvl = int(self.cfg.get("string_zstd_level", 3))
        return tiledb.FilterList([tiledb.ZstdFilter(level=lvl)])

    def filters_for_dtype(self, dtype: Any) -> tiledb.FilterList:
        """
        Return a TileDB FilterList for a given numpy dtype.

        Dispatch table:
            float32         → DeltaFilter + ByteShuffle + Zstd
            float64         → DeltaFilter + ByteShuffle + Zstd
            uint8           → (BitWidthReduction) + RLE + Zstd
            other int/uint  → (BitWidthReduction) + Zstd
            unicode str     → Zstd
            fallback        → Zstd(level=3)
        """
        dt = np.dtype(dtype)
        kind = dt.kind  # 'f', 'i', 'u', 'U', ...

        if kind == "f":
            return self._filters_float32() if dt == np.float32 else self._filters_float64()

        if kind in ("i", "u"):
            return self._filters_flags_uint8() if dt == np.uint8 else self._filters_int_generic()

        if kind == "U":
            return self._filters_utf8()

        # bools, bytes, other oddballs
        return tiledb.FilterList([tiledb.ZstdFilter(level=3)])

    def timestamp_filters(self) -> tiledb.FilterList:
        """
        Filters for `timestamp_ns` attribute (int64 nanoseconds since epoch).

        DoubleDeltaFilter replaces BitWidthReductionFilter here — timestamp_ns
        values are ~1.7e18, so BitWidthReduction achieves no reduction at all.
        DoubleDeltaFilter on the other hand exploits near-monotonic ordering
        within a spatially-sorted write batch to achieve good compression.
        """
        return tiledb.FilterList(
            [
                tiledb.DoubleDeltaFilter(),
                tiledb.ZstdFilter(level=2),
            ]
        )