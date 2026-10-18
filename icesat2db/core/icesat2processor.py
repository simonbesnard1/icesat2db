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
from typing import Dict, Optional, Union
import time

import geopandas as gpd
import pandas as pd
import yaml
from dask.distributed import Client, as_completed as dask_as_completed
import concurrent.futures
from concurrent.futures import as_completed

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
            os.path.join(self.data_info["progress_dir"], "progress")
        )
        self.report_every = int(self.data_info["tiledb"].get("report_every", 25))
        flush_every = self.data_info["tiledb"].get("flush_every", None)
        self.flush_every = int(flush_every) if flush_every is not None else None

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

        A granule's product list is narrowed to only the products not yet
        marked processed in their respective writer — a granule that's
        partially done (e.g. ATL08 written, ATL03 still pending) is not
        re-written for the product(s) already completed, since TileDB arrays
        allow duplicate rows and a redundant re-write would duplicate data.

        Parameters:
        ----------
        cmr_data : dict
            Dictionary of granule metadata from CMR API, with granule IDs as keys.

        Returns:
        --------
        dict
            A dictionary of unprocessed (granule_id -> product_info) work.
        """
        ids_by_product: Dict[str, list] = defaultdict(list)
        for granule_id, product_info in cmr_data.items():
            for _, product, _, _ in product_info:
                ids_by_product[product].append(granule_id)

        processed_by_product: Dict[str, dict] = {}
        for product, writer in self.database_writers.items():
            ids_for_product = ids_by_product.get(product, [])
            if ids_for_product:
                processed_by_product[product] = writer.check_granules_status(
                    ids_for_product
                )

        unprocessed_granules = {}
        for granule_id, product_info in cmr_data.items():
            remaining = [
                entry
                for entry in product_info
                if not processed_by_product.get(entry[1], {}).get(granule_id, False)
            ]
            if remaining:
                unprocessed_granules[granule_id] = remaining

        return unprocessed_granules

    def _process_granules(self, unprocessed_cmr_data: dict):
        """
        Process unprocessed granules in parallel, then write to TileDB in a
        fragment-friendly way: accumulate per spatial window and write once per
        window, per product (each product has its own TileDB array/writer).
        """
        temporal_batching = self.data_info["tiledb"].get("temporal_batching", None)
        if temporal_batching in ("daily", "weekly", "annual"):
            batches = _temporal_tiling(unprocessed_cmr_data, temporal_batching)
        elif temporal_batching is None:
            batches = {"all": unprocessed_cmr_data}
        else:
            raise ValueError(
                "Invalid temporal batching option. Choose 'daily', 'weekly', 'annual', or null."
            )

        def _append_ledger_row(
            ledger,
            gid,
            timeframe,
            started_ts,
            finished_ts,
            status,
            metrics=None,
            error_msg=None,
        ):
            metrics = metrics or {}
            row = Row(
                granule_id=gid,
                timeframe=timeframe,
                submitted_ts=ledger._submits.get(gid, finished_ts),
                started_ts=metrics.get("started_ts", started_ts),
                finished_ts=finished_ts,
                duration_s=finished_ts - metrics.get("started_ts", started_ts),
                status=status,
                n_records=metrics.get("n_records"),
                bytes_downloaded=metrics.get("bytes_downloaded"),
                products=(
                    ",".join(metrics.get("products", []))
                    if metrics.get("products")
                    else None
                ),
                error_msg=error_msg,
            )
            ledger.append(row)

        def _flush_buffers(buffers_by_product, processed_ids_by_product, timeframe):
            """
            Concatenate buffered DataFrames per product, split into spatial
            tiles, and write one TileDB fragment per tile per product-writer.
            Granule ids are marked processed only for the writer(s) they
            actually contributed to in this flush window.
            """
            for product, buffers in buffers_by_product.items():
                if not buffers:
                    continue
                writer = self.database_writers[product]
                try:
                    combined = pd.concat(buffers, ignore_index=True)
                    for _, tile_df in writer.spatial_chunking(combined):
                        writer.write_granule(tile_df)
                    ids_for_product = processed_ids_by_product.get(product)
                    if ids_for_product:
                        writer.mark_granules_as_processed_batch(ids_for_product)
                except Exception as e:
                    logger.error(
                        f"Write phase failed for timeframe {timeframe}, product {product}: {e}",
                        exc_info=True,
                    )
                    raise

        def _handle_result(
            gid,
            started_ts,
            finished_ts,
            ids_,
            gdf_dict,
            metrics,
            ledger,
            timeframe,
            buffers_by_product,
            processed_ids_by_product,
        ):
            ok = ids_ is not None

            # Mark every product that was attempted for this granule as
            # processed, even if its resulting DataFrame was empty — avoids
            # retrying forever a granule with zero valid segments/photons.
            if ok:
                for product in metrics.get("products", []):
                    processed_ids_by_product[product].append(ids_)

            if gdf_dict:
                for product, df in gdf_dict.items():
                    if df is not None and not df.empty:
                        buffers_by_product[product].append(df)

            _append_ledger_row(
                ledger,
                gid,
                timeframe,
                started_ts,
                finished_ts,
                status="ok" if ok else "fail",
                metrics=metrics,
                error_msg=None,
            )

        # ---- Executor path ----
        if isinstance(self.parallel_engine, concurrent.futures.Executor):
            with self.parallel_engine as executor:
                for timeframe, granules in batches.items():
                    ledger = ProgressLedger(
                        os.path.join(self.progress_dir, timeframe), timeframe
                    )

                    # Submit tasks
                    future_map = {}
                    for gid, pinf in granules.items():
                        ledger.note_submit(gid)
                        fut = executor.submit(
                            IceSat2Processor.process_single_granule,
                            gid,
                            pinf,
                            self.data_info,
                            self.download_path,
                        )
                        future_map[fut] = gid

                    buffers_by_product = defaultdict(list)
                    processed_ids_by_product = defaultdict(list)
                    counter = 0

                    for fut in as_completed(future_map):
                        # Pop immediately so the future (and its result) can be
                        # GC'd once we're done with its data below.
                        gid = future_map.pop(fut)
                        started_ts = time.time()
                        try:
                            ids_, gdf_dict, metrics = fut.result()
                            finished_ts = time.time()
                            _handle_result(
                                gid,
                                started_ts,
                                finished_ts,
                                ids_,
                                gdf_dict,
                                metrics,
                                ledger,
                                timeframe,
                                buffers_by_product,
                                processed_ids_by_product,
                            )

                        except Exception as e:
                            finished_ts = time.time()
                            tb = traceback.format_exc()
                            ledger.write_error(gid, tb)
                            _append_ledger_row(
                                ledger,
                                gid,
                                timeframe,
                                started_ts,
                                finished_ts,
                                status="fail",
                                metrics={},
                                error_msg=str(e),
                            )
                            logger.error(f"Granule {gid} failed: {e}")

                        finally:
                            counter += 1
                            if (
                                self.flush_every
                                and counter % self.flush_every == 0
                                and any(buffers_by_product.values())
                            ):
                                try:
                                    _flush_buffers(
                                        buffers_by_product,
                                        processed_ids_by_product,
                                        timeframe,
                                    )
                                    buffers_by_product.clear()
                                    processed_ids_by_product.clear()
                                    _release_memory()
                                except Exception as flush_exc:
                                    logger.error(
                                        f"Periodic flush failed (will retry at next flush): {flush_exc}"
                                    )
                            if counter % self.report_every == 0:
                                ledger.write_status_md()
                                ledger.write_html()

                    # Final flush for remaining buffer
                    if any(buffers_by_product.values()):
                        try:
                            _flush_buffers(
                                buffers_by_product, processed_ids_by_product, timeframe
                            )
                        except Exception:
                            # already logged; keep ledger finalization
                            pass

                    ledger.write_status_md()
                    ledger.write_html()
                    # Return pages from this year's allocation back to the OS
                    # before starting the next temporal batch.
                    _release_memory()
            return

        # ---- Dask path ----
        if isinstance(self.parallel_engine, Client):
            for timeframe, granules in batches.items():
                ledger = ProgressLedger(
                    os.path.join(self.progress_dir, timeframe), timeframe
                )

                future_map = {}
                for gid, pinf in granules.items():
                    ledger.note_submit(gid)
                    fut = self.parallel_engine.submit(
                        IceSat2Processor.process_single_granule,
                        gid,
                        pinf,
                        self.data_info,
                        self.download_path,
                    )
                    future_map[fut] = gid

                buffers_by_product = defaultdict(list)
                processed_ids_by_product = defaultdict(list)
                counter = 0

                # as_completed yields each future as it finishes — no serial blocking
                for fut in dask_as_completed(future_map):
                    gid = future_map.pop(fut)
                    started_ts = time.time()
                    try:
                        ids_, gdf_dict, metrics = fut.result()
                        finished_ts = time.time()
                        _handle_result(
                            gid,
                            started_ts,
                            finished_ts,
                            ids_,
                            gdf_dict,
                            metrics,
                            ledger,
                            timeframe,
                            buffers_by_product,
                            processed_ids_by_product,
                        )

                    except Exception as e:
                        finished_ts = time.time()
                        tb = traceback.format_exc()
                        ledger.write_error(gid, tb)
                        _append_ledger_row(
                            ledger,
                            gid,
                            timeframe,
                            started_ts,
                            finished_ts,
                            status="fail",
                            metrics={},
                            error_msg=str(e),
                        )
                        logger.error(f"Dask task for {gid} failed: {e}")

                    finally:
                        counter += 1
                        if (
                            self.flush_every
                            and counter % self.flush_every == 0
                            and any(buffers_by_product.values())
                        ):
                            try:
                                _flush_buffers(
                                    buffers_by_product,
                                    processed_ids_by_product,
                                    timeframe,
                                )
                                buffers_by_product.clear()
                                processed_ids_by_product.clear()
                                _release_memory()
                            except Exception as flush_exc:
                                logger.error(
                                    f"Periodic flush failed (will retry at next flush): {flush_exc}"
                                )
                        if counter % self.report_every == 0:
                            ledger.write_status_md()
                            ledger.write_html()

                if any(buffers_by_product.values()):
                    try:
                        _flush_buffers(
                            buffers_by_product, processed_ids_by_product, timeframe
                        )
                    except Exception:
                        pass

                ledger.write_status_md()
                ledger.write_html()
                _release_memory()
            return

        raise ValueError("Unsupported parallel engine.")

    @staticmethod
    def process_single_granule(granule_id, product_info, data_info, download_path):
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

        Returns:
        --------
        tuple
            A tuple of (granule_id, {product: DataFrame} or None, metrics).
        """

        started_ts = time.time()
        downloader = H5FileDownloader(download_path)

        bytes_dl = 0
        prods = []
        download_results = []
        for url, product, _, _ in product_info:

            res = downloader.download(granule_id, url, IceSat2Product(product))
            # If your downloader can expose sizes, insert here:
            if isinstance(res, tuple) and len(res) >= 2 and isinstance(res[1], int):
                bytes_dl += int(res[1])
            prods.append(str(product))
            download_results.append(res)

        granule_processor = IceSat2Granule(download_path, data_info)
        ids_, gdf_dict = granule_processor.process_granule(download_results)
        n_records = sum(df.shape[0] for df in gdf_dict.values()) if gdf_dict else None

        metrics = {
            "started_ts": started_ts,
            "bytes_downloaded": bytes_dl or None,
            "products": prods,
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
