# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felixd@gfz.de and urbazaev@gfz.de
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


import re
from dataclasses import dataclass


@dataclass(frozen=True)
class IceSat2NameMetadata:
    """
    Container for metadata derived from IceSat2 file name conventions.

    Attributes:
        product (str): The product type (e.g. ATL08).
        year (str): The year of data acquisition (4 digits).
        month (str): The month of data acquisition (2 digits).
        day (str): The day of data acquisition (2 digits).
        hour (str): The hour (UTC) when data was acquired.
        minute (str): The minute (UTC) when data was acquired.
        second (str): The second (UTC) when data was acquired.
        ref_ground_track (str): The RTG number (4 digits) ranging from 0001 to 1387.
        cycle_number (str): The cycle number (2 digits).
        region (str): The region number (2 digits) ranging from 01-14.
        version (str): The version number (3 digits).
        revision (str): The revision number (2 digits).

    """

    product: str
    year: str
    month: str
    day: str
    hour: str
    minute: str
    second: str
    ref_ground_track: str
    cycle_number : str
    segment_number : str
    version: str
    revision: str


# Precompile the IceSat2 filename pattern for efficient reuse
IceSat2_FILENAME_PATTERN = re.compile(
    (
        r"^"
        r"(?P<product>[A-Z0-9]+)_"        # Product identifier (e.g. ATL08)
        r"(?P<year>\d{4})"                # Year of acquisition (YYYY)
        r"(?P<month>\d{2})"               # Month of acquisition (MM)
        r"(?P<day>\d{2})"                 # Day of acquisition (DD)
        r"(?P<hour>\d{2})"                # Hour of acquisition (UTC)
        r"(?P<minute>\d{2})"              # Minute of acquisition (UTC)
        r"(?P<second>\d{2})_"             # Second of acquisition (UTC)
        r"(?P<ref_ground_track>\d{4})"    # Reference Ground Track (0001–1387)
        r"(?P<cycle_number>\d{2})"        # Cycle number (2 digits)
        r"(?P<segment_number>\d{2})_"     # Segment / region number (01–14)
        r"(?P<version>\d{3})_"            # Product version number (3 digits)
        r"(?P<revision>\d{2})"            # Product revision number (2 digits)
        r"$"
    )
)



def parse_granule_filename(IceSat2_filename: str) -> IceSat2NameMetadata:
    """
    Parses the IceSat2 filename and extracts metadata into a structured format.

    Parameters:
        IceSat2_filename (str): The IceSat2 filename to parse.

    Returns:
        IceSat2NameMetadata: An instance of IceSat2NameMetadata with extracted components.

    Raises:
        ValueError: If the filename does not match the expected IceSat2 naming pattern.
    """
    match = IceSat2_FILENAME_PATTERN.search(IceSat2_filename)
    if match is None:
        raise ValueError(
            f"Filename '{IceSat2_filename}' does not match the expected IceSat2 naming pattern: {IceSat2_FILENAME_PATTERN.pattern}"
        )
    return IceSat2NameMetadata(**match.groupdict())
