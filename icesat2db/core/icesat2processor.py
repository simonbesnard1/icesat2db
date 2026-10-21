# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences


import ctypes
import gc
import logging
import os
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set, Union
import time

import geopandas as gpd
import pandas as pd
import yaml
from dask.distributed import Client, wait as dask_wait
import concurrent.futures
from concurrent.futures import wait as futures_wait, FIRST_COMPLETED

from icesat2db.core.icesat2database import IceSat2Database
from icesat2db.core.icesat2granule import IceSat2Granule
from icesat2db.downloader.authentication import EarthDataAuthenticator
from icesat2db.downloader.data_downloader import CMRDataDownloader, H5FileDownloader
from icesat2db.utils.constants import IceSat2Product, configured_products
from icesat2db.utils.geo_processing import _temporal_tiling, check_and_format_shape
from icesat2db.utils.progress_ledger import ProgressLedger, Row

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("distributed").setLevel(logging.WARNING)
logging.getLogger("tornado").setLevel(logging.WARNING)
logger = logging.getLogger()


def _release_memory() -> None:
    """
    Force Python GC and ask the C allocator to return freed pages to the OS.
    The malloc_trim call is Linux-only; on other platforms it is silently skipped.
    """
    gc.collect()
    try:
        ctypes.cdll.LoadLibrary("libc.so.6").malloc_trim(0)
    except Exception:
        pass


class IceSat2Processor:
    """
    IceSat2Processor class is responsible for processing IceSat2 granules, handling metadata,
    and writing data into the database.
    """

    def __init__(
        self,
        geometry: Union[gpd.GeoDataFrame, str] = None,
        start_date: str = None,
        end_date: str = None,
        config_file: str = None,
        earth_data_dir: Optional["str"] = None,
        credentials: Optional[dict] = None,
        parallel_engine: Optional[object] = None,
        log_dir: Optional[str] = None,
    ):
        """
        Initializes the IceSat2Processor.

        Parameters:
        -----------
        config_file : str
            Path to the configuration YAML file.
        earth_data_dir : str
            Directory containing EarthData credentials.
        credentials : dict, optional
            Credentials for accessing the database.
        parallel_engine : object, optional
            A parallelization engine such as `dask.distributed.Client` or
            `concurrent.futures.Executor`. Defaults to single-threaded.
        geometry : geopandas.GeoDataFrame, optional
            Geometry defining the region of interest.
        log_dir : str, optional
            Directory to store logs.
        """

        # Validate config_file
        if not config_file or not isinstance(config_file, str):
            raise ValueError(
                "The 'config_file' argument must be a valid, non-empty string pointing to the configuration file."
            )

        config_path = Path(config_file)
        if config_path.suffix.lower() != ".yml":
            raise ValueError(
                f"The configuration file must have a '.yml' extension. Provided: {config_file}"
            )
        if not config_path.exists():
            raise FileNotFoundError(
                f"The configuration file does not exist: {config_file}"
            )

        # Validate credentials
        if credentials is not None and not isinstance(credentials, dict):
            raise ValueError(
                "The 'credentials' argument must be a dictionary if provided."
            )

        # Validate parallel_engine
        if parallel_engine is not None and not (
            isinstance(parallel_engine, concurrent.futures.Executor)
            or isinstance(parallel_engine, Client)
        ):
            raise ValueError(
                "The 'parallel_engine' argument must be either a 'concurrent.futures.Executor', "
                "'dask.distributed.Client', or None."
            )

        # Validate log_dir
        if log_dir is not None and not isinstance(log_dir, str):
            raise ValueError("The 'log_dir' argument must be a string if provided.")

        # Validate geometry
        if geometry is None:
            raise ValueError("The 'geometry' parameter must be provided.")
        self.geom = self._validate_and_load_geometry(geometry)

        # Validate and parse dates
        if not start_date or not end_date:
            raise ValueError("Both 'start_date' and 'end_date' must be provided.")
        self.start_date = self._validate_and_parse_date(start_date, "start_date")
        self.end_date = self._validate_and_parse_date(end_date, "end_date")
        if self.start_date > self.end_date:
            raise ValueError(
                "'start_date' must be earlier than or equal to 'end_date'."
            )

        # Set up logging to file if log_dir is provided
        if log_dir:
            # Ensure the log directory exists
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(
                log_dir,
                f"icesat2processor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            )

            # Create a FileHandler and set its level and format
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)

            # Add the FileHandler to the logger
            if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
                logger.addHandler(file_handler)

        # Load configurations and setup paths and components
        self.data_info = self._load_yaml_file(config_file)
        self.credentials = credentials

        # Validate Earth data directory
        earth_data_path = Path(earth_data_dir) if earth_data_dir else Path.home()
        if not earth_data_path.exists():
            raise FileNotFoundError(
                f"The specified Earth data credentials directory '{earth_data_dir}' does not exist. "
                "Please provide the correct directory or create the credentials using the EarthDataAuthenticator module."
            )

        # Validate Earthdata credentials in strict mode
        try:
            authenticator = EarthDataAuthenticator(
                earth_data_dir=earth_data_dir, strict=True
            )
            authenticator.authenticate()
        except FileNotFoundError as e:
            logger.error(e)
            raise

        # Initialize download_path
        self.download_path = self._ensure_directory(
            os.path.join(self.data_info["data_dir"], "download")
        )

        self.progress_dir = self._ensure_directory(
            os.path.join(
                self.data_info.get("progress_dir", self.data_info["data_dir"]),
                "progress",
            )
        )
        self.report_every = int(self.data_info["tiledb"].get("report_every", 25))
        self.flush_every = int(self.data_info["tiledb"].get("flush_every", 50))
        if self.report_every < 1 or self.flush_every < 1:
            raise ValueError("report_every and flush_every must be positive.")
        # Products each pending granule still has to write (set by
        # _filter_unprocessed_granules); companions are downloaded, not written.
        self._write_products: Dict[str, Set[str]] = {}

        # Determine which products are configured for ingestion (a
        # 'level_<product>' block present in data_info) — drives both the
        # downloader's required products and which TileDB writers/arrays get
        # created. Products are independent: a config with only 'level_atl08'
        # behaves exactly as before.
        self.products = configured_products(self.data_info)
        if not self.products:
            raise ValueError(
                "No IceSat2 product is configured: 'data_info' must contain at "
                "least one 'level_<product>' block (e.g. 'level_atl08')."
            )

        # Initialize one database writer per configured product (each writes
        # to its own TileDB array — see IceSat2Database's 'product' parameter).
        self.database_writers = self._initialize_database_writers(credentials)

        # Create the database schema for each product
        for writer in self.database_writers.values():
            writer._create_arrays()

        # Set the parallel engine
        self.parallel_engine = self._initialize_parallel_engine(parallel_engine)

    def _validate_and_load_geometry(self, geometry: object) -> gpd.GeoDataFrame:
        """
        Validates and loads the geometry from a file or GeoDataFrame.

        Parameters:
        ----------
        geometry : str or geopandas.GeoDataFrame
            Path to a GeoJSON file or a GeoDataFrame.

        Returns:
        --------
        geopandas.GeoDataFrame
            A validated and formatted GeoDataFrame.
        """
        if isinstance(geometry, gpd.GeoDataFrame):
            return check_and_format_shape(geometry, simplify=True)
        elif isinstance(geometry, str):
            if not os.path.exists(geometry):
                raise FileNotFoundError(f"Region file not found: {geometry}")
            gdf = gpd.read_file(geometry)
            return check_and_format_shape(gdf, simplify=True)
        else:
            raise ValueError(
                "Geometry must be a GeoDataFrame or a valid GeoJSON file path."
            )

    @staticmethod
    def _validate_and_parse_date(date_str: str, date_type: str) -> datetime:
        """
        Validates and parses a date string.

        Parameters:
        ----------
        date_str : str
            Date string in 'YYYY-MM-DD' format.
        date_type : str
            Type of the date being validated (e.g., 'start_date', 'end_date').

        Returns:
        --------
        datetime
            Parsed datetime object.
        """
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid format for {date_type}. Expected 'YYYY-MM-DD'.")

    def _initialize_database_writers(
        self, credentials: Optional[dict]
    ) -> Dict[str, IceSat2Database]:
        """
        Initialize and return one IceSat2Database instance per configured product.
        """
        return {
            product.value: IceSat2Database(
                config=self.data_info, credentials=credentials, product=product.value
            )
            for product in self.products
        }

    def _initialize_parallel_engine(self, parallel_engine: Optional[object]):
        """
        Initialize the parallel engine.

        Parameters:
        ----------
        parallel_engine : object, optional
            User-specified parallelization engine.

        Returns:
        --------
        object
            The initialized parallel engine. Defaults to single-threaded execution if no engine is provided.
        """
        if parallel_engine:
            logger.info("Using user-provided parallel engine.")
            return parallel_engine

        logger.info(
            "No parallel engine provided. Defaulting to single-threaded execution."
        )
        return concurrent.futures.ThreadPoolExecutor(max_workers=1)

    @staticmethod
    def _ensure_directory(path: str) -> str:
        """Ensure a directory exists and return its path."""
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _load_yaml_file(file_path: str) -> dict:
        """Load a YAML configuration file."""
        with open(file_path, "r") as file:
            return yaml.safe_load(file)

    def compute(self, consolidate: bool = True, consolidation_type: str = "spatial"):
        """
        Main method to download and process IceSat2 granules.

        Parameters:
        ----------
        consolidate : bool, default=True
            If True, consolidates fragments in the TileDB arrays after processing all granules.
        consolidation_type : str, default='spatial'
            Type of consolidation to perform ('default' or 'spatial').
        """
        try:

            # Download and filter CMR data
            cmr_data = self._download_cmr_data()
            unprocessed_cmr_data = self._filter_unprocessed_granules(cmr_data)

            if not unprocessed_cmr_data:
                logger.info("All requested granules are already processed.")
                if consolidate:
                    for writer in self.database_writers.values():
                        writer.consolidate_fragments(
                            consolidation_type=consolidation_type,
                            parallel_engine=None,
                        )
                return

            # Process unprocessed granules
            logger.info("Starting IceSat2 granules processing...")
            self._process_granules(unprocessed_cmr_data)

            # Consolidate fragments if required
            if consolidate:
                for writer in self.database_writers.values():
                    writer.consolidate_fragments(
                        consolidation_type=consolidation_type, parallel_engine=None
                    )
            logger.info("IceSat2 granule processing completed successfully.")
        except Exception as e:
            # Log the exception with traceback
            logger.error("An error occurred: %s", e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise

    def _download_cmr_data(self) -> pd.DataFrame:
        """Download the CMR metadata for the specified date range and region."""
        downloader = CMRDataDownloader(
            self.geom,
            self.start_date,
            self.end_date,
            self.data_info["earth_data_info"],
            required_products=self.products,
        )
        return downloader.download()

    def _filter_unprocessed_granules(self, cmr_data: dict) -> dict:
        """
        Filter out (granule_id, product) work that has already been processed.

        A granule's product list is narrowed to the products not yet marked
        processed in their respective writer, so a partially done granule (e.g.
        ATL08 written, ATL03 still pending) does not rewrite completed products.
        With ``link_atl08`` the ATL08 file of a pending ATL03 granule is still
        downloaded as the linkage companion, but not written again; an ATL03
        granule without any ATL08 file cannot be linked and is skipped.

        Parameters:
        ----------
        cmr_data : dict
            Dictionary of granule metadata from CMR API, with granule IDs as keys.

        Returns:
        --------
        dict
            A dictionary of unprocessed (granule_id -> product_info) downloads.
        """
        ids_by_product: Dict[str, list] = defaultdict(list)
        for granule_id, product_info in cmr_data.items():
            for entry in product_info:
                ids_by_product[entry[1]].append(granule_id)

        processed_by_product: Dict[str, dict] = {}
        for product, writer in self.database_writers.items():
            ids_for_product = ids_by_product.get(product, [])
            if ids_for_product:
                writer.register_granule_sources(cmr_data)
                processed_by_product[product] = writer.check_granules_status(
                    ids_for_product
                )

        linked = self.data_info.get("level_atl03", {}).get("link_atl08", False)
        unprocessed_granules = {}
        self._write_products = {}
        no_companion = []
        for granule_id, product_info in cmr_data.items():
            available = {entry[1] for entry in product_info}
            todo = {
                product
                for product in available
                if product in self.database_writers
                and not processed_by_product.get(product, {}).get(granule_id, False)
            }
            if linked and "atl03" in todo and "atl08" not in available:
                logger.debug(
                    f"Granule {granule_id}: ATL03 skipped, no ATL08 companion to link."
                )
                no_companion.append(granule_id)
                todo.discard("atl03")
            if not todo:
                continue
            needed = set(todo)
            if linked and "atl03" in todo:
                needed.add("atl08")
            unprocessed_granules[granule_id] = [
                entry for entry in product_info if entry[1] in needed
            ]
            self._write_products[granule_id] = todo

        if no_companion:
            # ATL08 exists only where there is land, so this is expected for
            # granules over ocean or ice; one line instead of one per granule.
            logger.info(
                f"{len(no_companion)} ATL03 granule(s) skipped: no ATL08 companion "
                f"to link (e.g. {', '.join(no_companion[:5])})."
            )

        return unprocessed_granules

    def _process_granules(self, unprocessed_cmr_data: dict):
        """Process bounded work queues and record success only after persistence.

        At most ``tiledb.max_in_flight`` granules are parsed concurrently, and
        buffered frames are flushed every ``flush_every`` granules or once they
        exceed ``tiledb.max_buffer_bytes``, so photon-rate ATL03 granules cannot
        accumulate unbounded in memory. Each product is written to its own
        array; a granule is checkpointed per product only after that product's
        write succeeded.

        Writes are serialized by this coordinator. Multiple independent processors
        must not write to the same database concurrently.
        """
        cfg = self.data_info["tiledb"]
        temporal_batching = cfg.get("temporal_batching")
        if temporal_batching in ("daily", "weekly", "annual"):
            batches = _temporal_tiling(unprocessed_cmr_data, temporal_batching)
        elif temporal_batching is None:
            batches = {"all": unprocessed_cmr_data}
        else:
            raise ValueError(
                "Invalid temporal batching option. Choose 'daily', 'weekly', 'annual', or null."
            )
        max_pending = int(cfg.get("max_in_flight", 4))
        max_bytes = int(cfg.get("max_buffer_bytes", 256 * 1024**2))
        if min(max_pending, max_bytes, self.flush_every, self.report_every) < 1:
            raise ValueError("Queue, buffer, flush and report limits must be positive.")
        is_dask = isinstance(self.parallel_engine, Client)
        if not is_dask and not isinstance(
            self.parallel_engine, concurrent.futures.Executor
        ):
            raise ValueError("Unsupported parallel engine.")
        wait = dask_wait if is_dask else futures_wait
        failures = []
        interrupted = {
            product: writer.interrupted_flush_granules()
            for product, writer in self.database_writers.items()
        }

        for timeframe, granules in batches.items():
            ledger = ProgressLedger(
                os.path.join(self.progress_dir, timeframe),
                timeframe,
                expected=len(granules),
            )
            remaining = iter(granules.items())
            pending = {}
            buffers = defaultdict(list)
            completed = []  # (granule_id, metrics, products to checkpoint)
            buffered_bytes = 0
            counter = 0

            def record(gid, metrics, status, error=None):
                finished = time.time()
                started = metrics.get("started_ts", finished)
                ledger.append(
                    Row(
                        granule_id=gid,
                        timeframe=timeframe,
                        submitted_ts=ledger._submits.get(gid, started),
                        started_ts=started,
                        finished_ts=finished,
                        duration_s=finished - started,
                        status=status,
                        n_records=metrics.get("n_records"),
                        bytes_downloaded=metrics.get("bytes_downloaded"),
                        products=",".join(metrics.get("products", [])) or None,
                        error_msg=error,
                    )
                )

            def write_products(gid, info):
                return self._write_products.get(gid) or {
                    entry[1] for entry in info if entry[1] in self.database_writers
                }

            def submit_more():
                while len(pending) < max_pending:
                    item = next(remaining, None)
                    if item is None:
                        break
                    gid, info = item
                    ledger.note_submit(gid)
                    products = sorted(write_products(gid, info))
                    options = {"pure": False} if is_dask else {}
                    future = self.parallel_engine.submit(
                        IceSat2Processor.process_single_granule,
                        gid,
                        info,
                        self.data_info,
                        self.download_path,
                        products,
                        **options,
                    )
                    pending[future] = (gid, set(products))

            def flush():
                if not completed:
                    return
                try:
                    for product, writer in self.database_writers.items():
                        gids = [gid for gid, _, prods in completed if product in prods]
                        if not gids:
                            continue
                        frames = buffers.get(product)
                        if frames:
                            # Only granules of a crashed write can already be stored.
                            check_existing = not interrupted[product].isdisjoint(gids)
                            writer.begin_flush(gids)
                            combined = pd.concat(frames, ignore_index=True)
                            for _, tile in writer.spatial_chunking(combined):
                                writer.write_granule(
                                    tile, check_existing=check_existing
                                )
                            del combined
                        # Empty, successfully parsed granules also receive a checkpoint.
                        writer.mark_granules_as_processed_batch(gids)
                except Exception as exc:
                    for gid, metrics, _ in completed:
                        ledger.write_error(gid, traceback.format_exc())
                        record(gid, metrics, "fail", str(exc))
                    raise
                for gid, metrics, _ in completed:
                    record(gid, metrics, "ok")
                buffers.clear()
                completed.clear()
                _release_memory()

            try:
                submit_more()
                while pending:
                    done, _ = wait(list(pending), return_when=FIRST_COMPLETED)
                    for future in done:
                        gid, products = pending.pop(future)
                        try:
                            result_id, frames, metrics = future.result()
                            if result_id != gid:
                                raise RuntimeError(
                                    f"Granule {gid} did not parse successfully."
                                )
                            completed.append((gid, metrics, products))
                            for product, frame in (frames or {}).items():
                                if product not in products:
                                    continue
                                if frame is not None and not frame.empty:
                                    buffers[product].append(frame)
                                    buffered_bytes += int(
                                        frame.memory_usage(index=True, deep=True).sum()
                                    )
                            del frames
                        except Exception as exc:
                            failures.append(gid)
                            ledger.write_error(gid, traceback.format_exc())
                            record(gid, {}, "fail", str(exc))
                            logger.error("Granule %s failed: %s", gid, exc)
                        finally:
                            if is_dask:
                                future.release()
                        counter += 1
                        if (
                            len(completed) >= self.flush_every
                            or buffered_bytes >= max_bytes
                        ):
                            # Refill the queue first so downloads keep running
                            # while this batch is written.
                            submit_more()
                            flush()
                            buffered_bytes = 0
                        if counter % self.report_every == 0:
                            ledger.write_status_md()
                            ledger.write_html()
                    done.clear()
                    del future
                    submit_more()
                flush()
            finally:
                for future in pending:
                    future.cancel()
                    if is_dask:
                        future.release()
                ledger.write_status_md()
                ledger.write_html()
        if failures:
            raise RuntimeError(
                f"{len(failures)} granule(s) failed; see progress logs: {failures[:10]}"
            )

    @staticmethod
    def process_single_granule(
        granule_id, product_info, data_info, download_path, write_products=None
    ):
        """
        Processes a single granule by downloading and processing sequentially.

        Parameters:
        ----------
        granule_id : str
            ID of the granule to process.
        product_info : list
            List of tuples containing URL, product type, and additional information for the granule.
        data_info : dict
            Dictionary containing configuration and metadata.
        download_path : str
            Path to the directory where downloaded files are stored.
        write_products : list of str, optional
            Products whose records are returned for writing. Other downloaded
            products (e.g. an already stored ATL08 companion) are only used for
            linkage. Defaults to every downloaded product.

        Returns:
        --------
        tuple
            A tuple of (granule_id, {product: DataFrame}, metrics). ``metrics
            ["products"]`` lists the products to checkpoint for this granule.
        """

        started_ts = time.time()
        downloader = H5FileDownloader(download_path)

        bytes_dl = 0
        download_results = []
        for entry in product_info:
            url, product = entry[:2]

            res = downloader.download(granule_id, url, IceSat2Product(product))
            path = res[1][1]
            if path and os.path.isfile(path):
                bytes_dl += os.path.getsize(path)
            download_results.append(res)

        if write_products is None:
            write_products = [entry[1] for entry in product_info]
        granule_processor = IceSat2Granule(download_path, data_info)
        ids_, gdf_dict = granule_processor.process_granule(download_results)
        gdf_dict = {p: df for p, df in gdf_dict.items() if p in write_products}
        n_records = sum(df.shape[0] for df in gdf_dict.values())

        metrics = {
            "started_ts": started_ts,
            "bytes_downloaded": bytes_dl or None,
            "products": [str(p) for p in write_products],
            "n_records": n_records,
        }
        return ids_, gdf_dict, metrics

    def close(self):
        """Close the parallelization engine if applicable."""
        if isinstance(self.parallel_engine, Client):
            # Close Dask client if it's the engine
            self.parallel_engine.close()
            self.parallel_engine = None
            logger.info("Dask client and cluster have been closed.")
        elif isinstance(self.parallel_engine, concurrent.futures.Executor):
            # Shutdown concurrent.futures executor if used
            self.parallel_engine.shutdown(wait=True)
            self.parallel_engine = None
            logger.info("ThreadPoolExecutor has been shut down.")
        else:
            logger.info("No parallel engine to close.")

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context and close resources."""
        self.close()
