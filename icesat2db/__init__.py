# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from importlib.metadata import version as _version

from icesat2db.utils.print_versions import show_versions

try:
    __version__ = _version("icesat2db")
except Exception:
    __version__ = "9999"

from icesat2db.beam import Beam, atl08_beam, atl03_beam
from icesat2db.beam.Beam import beam_handler
from icesat2db.beam.atl08_beam import ATL08Beam
from icesat2db.beam.atl03_beam import ATL03Beam
from icesat2db.core import gedidatabase, gedigranule, gediprocessor, gediprovider
from icesat2db.core.gedidatabase import GEDIDatabase
from icesat2db.core.gedigranule import GEDIGranule
from icesat2db.core.gediprocessor import GEDIProcessor
from icesat2db.core.gediprovider import GEDIProvider
from icesat2db.downloader import authentication, cmr_query, data_downloader
from icesat2db.downloader.authentication import EarthDataAuthenticator
from icesat2db.downloader.cmr_query import CMRQuery
from icesat2db.downloader.data_downloader import (
    CMRDataDownloader,
    GEDIDownloader,
    H5FileDownloader,
)
from icesat2db.granule import (
    Granule,
    granule_name,
    granule_parser,
    atl08_granule,
    atl03_granule,
)
from icesat2db.granule.Granule import granule_handler
from icesat2db.granule.granule_name import GediNameMetadata
from icesat2db.granule.granule_parser import GranuleParser
from icesat2db.granule.atl08_granule import ATL08Granule
from icesat2db.granule.atl03_granule import ATL03Granule
from icesat2db.providers import tiledb_provider
from icesat2db.providers.tiledb_provider import TileDBProvider
from icesat2db.utils import constants, geo_processing, print_versions, tiledb_consolidation
from icesat2db.utils.tiledb_consolidation import (
    SpatialConsolidationPlan,
    SpatialConsolidationPlanner,
)

__all__ = [
    "gedidatabase",
    "gedigranule",
    "gediprocessor",
    "gediprovider",
    "granule_parser",
    "tiledb_provider",
    "constants",
    "geo_processing",
    "print_versions",
    "tiledb_consolidation",
    "SpatialConsolidationPlan",
    "SpatialConsolidationPlanner",
    "GEDIProcessor",
    "GEDIGranule",
    "GEDIProvider",
    "GEDIDatabase",
    "GEDIDownloader",
    "CMRDataDownloader",
    "H5FileDownloader",
    "authentication",
    "cmr_query",
    "CMRQuery",
    "data_downloader",
    "EarthDataAuthenticator",
    "granule_parser",
    "GranuleParser",
    "ATL08Granule",
    "ATL03Granule",
    "Beam",
    "beam_handler",
    "atl08_beam",
    "atl03_beam",
    "ATL08Beam",
    "ATL03Beam",
    "granule_name",
    "Granule",
    "granule_handler",
    "GediNameMetadata",
    "atl08_granule",
    "atl03_granule",
    "TileDBProvider",
    "show_versions",
    "__version__",
]
