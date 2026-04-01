.. _tiledb_database:

TileDB Global Database for ICESat-2 ATL08 Data
===============================================

.. important::

   If you use the database for your publications, please acknowledge that the dataset has been processed using `icesat2db`:

   **Dombrowski, F., Besnard, S., Urbazaev, M., & Holcomb, A.** icesat2db [Computer software]. `https://github.com/simonbesnard1/icesat2db <https://github.com/simonbesnard1/icesat2db>`_.

Overview
--------

The publicly available TileDB global database, managed by the `Global Land Monitoring group <https://www.gfz.de/en/section/remote-sensing-and-geoinformatics/topics/global-land-monitoring>`_ at GFZ-Potsdam, stores all processed ICESat-2 ATL08 version 7 data with a robust and scalable architecture. All granules for the ATL08 land and vegetation product have been ingested into the database, covering the full mission period from October 2018 onwards for all six ICESat-2 beams globally. The data is stored in a **Ceph object storage** managed by the GFZ data center. It enables efficient spatial, temporal, and attribute-based queries. This page provides an overview of the database setup, configuration, and access methods using the `icesat2db` package.

Ceph Object Storage Configuration
----------------------------------

The TileDB global database utilises a Ceph object storage backend to efficiently manage and distribute ICESat-2 ATL08 data. Below are the key characteristics of the Ceph bucket:

- **Bucket Name:** ``dog.icesat2db.icesat2-atl08-v007``
- **Access Endpoint:** ``https://s3.gfz-potsdam.de``
- **Region:** ``eu-central-1``
- **Access Control:** Public
- **Query Support:** Optimized for spatial and temporal queries

For users accessing the database programmatically, interactions with the Ceph bucket are abstracted by the `icesat2db` package, which retrieves data seamlessly from TileDB. Advanced users with direct access to the Ceph storage layer may utilise **S3-compatible tools** (such as ``aws s3api`` or ``rclone``) to interact with the data.

TileDB Database Configuration
------------------------------

The database configuration defines key parameters for data storage, spatial tiling, and query efficiency. Data are written in **30×30-degree spatial tiles** (``latitude_tile: 6``, ``longitude_tile: 6``), producing approximately 60 occupied fragments globally. Annual temporal batching is used at ingest time so that one flush per year per tile is performed, minimising S3 open/close overhead while keeping memory usage bounded via the ``flush_every`` parameter.

Below is the structure of the configuration file used to build the TileDB database:

.. code-block:: yaml

   # database parameters
   tiledb:
     storage_type: 's3'
     s3_bucket: "dog.icesat2db.icesat2-atl08-v007"
     url: "https://s3.gfz-potsdam.de"
     overwrite: false
     temporal_batching: "annual"
     latitude_tile: 6
     longitude_tile: 6
     flush_every: 20000
     time_range:
       start_time: "2018-01-01"
       end_time: "2030-12-31"
     spatial_range:
       lat_min: -90.0
       lat_max: 90.0
       lon_min: -180.0
       lon_max: 180.0
     dimensions: ['latitude', 'longitude', 'time']
     s3_settings:
       connect_timeout_ms: "300000"
       request_timeout_ms: "600000"
       connect_max_tries: "10"
       multipart_part_size: "52428800"
       backoff_scale: "2.0"
       backoff_max_ms: "120000"
     cell_order: "hilbert"
     capacity: 100000
     use_filters: true
     spatial_zstd_level: 1
     timestamp_zstd_level: 2

The configuration file contains:

- **Storage Type**: Specifies ``s3`` for cloud-based Ceph storage.
- **Temporal Batching**: ``annual`` — granules are batched by year before writing, reducing S3 open/close overhead.
- **Spatial Tiling**: ``latitude_tile: 6`` and ``longitude_tile: 6`` define 30×30-degree write tiles, producing spatially localised TileDB fragments for efficient regional queries.
- **flush_every**: Triggers a mid-batch write after every N granules to bound peak memory usage during ingest.
- **Time Range**: Defines the global temporal coverage (mission start to end).
- **Spatial Range**: Sets the global bounding box (full ±90° latitude, ±180° longitude).
- **Cell Order**: Hilbert space-filling curve ordering for optimised spatial locality within fragments.
- **Compression**: ``use_filters: true`` applies ByteShuffle+Zstd for float attributes and DoubleDelta+Zstd for time dimensions.

.. note::
   The current database architecture writes one TileDB fragment per spatial tile per annual batch. Consolidation is not applied post-ingest; the spatial fragment structure is established at write time. Users are encouraged to provide feedback and suggestions for optimising the TileDB database configuration.

.. figure:: /_static/images/tileDB_fragment_structure.png
   :alt: Data structure of the TileDB array
   :align: center
   :width: 100%

   **Figure 1**: The data structure in the TileDB Global Database for ICESat-2 ATL08 Data.


List of the available variables
---------------------------------

The database includes a wide range of variables from the ATL08 land and vegetation product, covering terrain elevation, canopy height metrics, quality flags, and ancillary data. Profile variables (e.g., ``canopy_h_metrics``) store multi-element arrays per 100 m segment, expanded into indexed columns in the database. Sub-segment variables (e.g., ``h_canopy_20m``) store values at 20 m resolution within each 100 m segment (5 values per segment).

.. csv-table:: Variable Descriptions
   :header: "Variable Name", "Description", "Units", "Category"
   :widths: 25, 55, 12, 10

   "asr", "Apparent surface reflectance", "adimensional", "Land Segment"
   "atlas_pa", "Off nadir pointing angle of the satellite", "radians", "Land Segment"
   "beam_azimuth", "Azimuth of the unit pointing vector for the reference photon in the local ENU frame", "radians", "Land Segment"
   "beam_coelev", "Co-elevation (direction from vertical) of the laser beam", "radians", "Land Segment"
   "brightness_flag", "Flag indicating a bright ground surface (e.g. snow-covered)", "adimensional", "Land Segment"
   "can_noise", "Number of noise photons falling within the canopy height per 100 m segment", "count/meter", "Canopy"
   "canopy_h_metrics", "Canopy height metrics at 10–95th percentiles of the canopy relative height distribution (18 values per segment)", "meters", "Canopy"
   "canopy_h_metrics_abs", "Absolute canopy height metrics above WGS84 Ellipsoid at 10–95th percentiles (18 values per segment)", "meters", "Canopy"
   "canopy_openness", "Standard deviation of canopy photon heights, providing inference of canopy openness", "adimensional", "Canopy"
   "canopy_rh_conf", "Canopy relative height confidence flag (0=<5% canopy; 1=>5% canopy, <5% ground; 2=>5% canopy and ground)", "adimensional", "Canopy"
   "centroid_height", "Optical centroid height of canopy and ground photons above the reference ellipsoid", "meters", "Canopy"
   "cloud_flag_atm", "Cloud/aerosol confidence flag from ATL09 (0–10; >0 means aerosols or clouds may be present)", "adimensional", "Land Segment"
   "cloud_fold_flag", "Flag indicating likely cloud signal folded down from above 15 km", "adimensional", "Land Segment"
   "delta_time", "Mean GPS seconds since the ATLAS SDP epoch for the segment", "seconds since 2018-01-01", "Land Segment"
   "delta_time_beg", "GPS seconds since ATLAS SDP epoch for the first photon in the segment", "seconds since 2018-01-01", "Land Segment"
   "delta_time_end", "GPS seconds since ATLAS SDP epoch for the last photon in the segment", "seconds since 2018-01-01", "Land Segment"
   "dem_flag", "Source of the DEM height (0=None, 1=Arctic, 2=Global, 3=MSS, 4=Antarctic)", "adimensional", "Land Segment"
   "dem_h", "Best available DEM height above the WGS84 Ellipsoid at the geolocation point", "meters", "Land Segment"
   "dem_removal_flag", "Flag indicating >20% of segment removed due to failing DEM quality tests", "adimensional", "Land Segment"
   "h_canopy", "98th percentile of relative canopy heights within the segment above the estimated terrain surface", "meters", "Canopy"
   "h_canopy_20m", "Canopy height for each 20 m geosegment within the 100 m land segment (5 values per segment)", "meters", "Canopy"
   "h_canopy_abs", "98th percentile of absolute canopy heights above the WGS84 Ellipsoid", "meters", "Canopy"
   "h_canopy_quad", "Quadratic mean height of relative canopy photon heights above terrain", "meters", "Canopy"
   "h_canopy_uncertainty", "Uncertainty of the relative canopy height for the segment", "meters", "Canopy"
   "h_dif_canopy", "Difference between h_canopy and h_median_canopy", "meters", "Canopy"
   "h_dif_ref", "Difference between h_te_median and the reference DEM", "meters", "Land Segment"
   "h_max_canopy", "Maximum relative canopy height within segment (equivalent to RH100)", "meters", "Canopy"
   "h_max_canopy_abs", "Maximum absolute canopy height above WGS84 Ellipsoid within segment", "meters", "Canopy"
   "h_mean_canopy", "Mean relative canopy height within segment", "meters", "Canopy"
   "h_mean_canopy_abs", "Mean absolute canopy height above WGS84 Ellipsoid within segment", "meters", "Canopy"
   "h_median_canopy", "Median relative canopy height within segment (equivalent to RH50)", "meters", "Canopy"
   "h_median_canopy_abs", "Median absolute canopy height above WGS84 Ellipsoid within segment", "meters", "Canopy"
   "h_min_canopy", "Minimum relative canopy height within segment", "meters", "Canopy"
   "h_min_canopy_abs", "Minimum absolute canopy height above WGS84 Ellipsoid within segment", "meters", "Canopy"
   "h_te_best_fit", "Best-fit terrain elevation at the mid-point of each 100 m segment", "meters", "Terrain"
   "h_te_best_fit_20m", "Best-fit terrain height at the centre of each 20 m geosegment (5 values per segment)", "meters", "Terrain"
   "h_te_interp", "Interpolated terrain surface height above WGS84 Ellipsoid at segment midpoint", "meters", "Terrain"
   "h_te_max", "Maximum terrain photon height above WGS84 Ellipsoid within segment", "meters", "Terrain"
   "h_te_mean", "Mean terrain photon height above WGS84 Ellipsoid within segment", "meters", "Terrain"
   "h_te_median", "Median terrain photon height above WGS84 Ellipsoid within segment", "meters", "Terrain"
   "h_te_min", "Minimum terrain photon height above WGS84 Ellipsoid within segment", "meters", "Terrain"
   "h_te_mode", "Mode of terrain photon heights above WGS84 Ellipsoid within segment", "meters", "Terrain"
   "h_te_rh25", "Terrain elevation at the 25th percentile of classified ground photon heights", "meters", "Terrain"
   "h_te_skew", "Skewness of terrain photon heights above WGS84 Ellipsoid within segment", "meters", "Terrain"
   "h_te_std", "Standard deviation of terrain photon heights above WGS84 Ellipsoid (terrain roughness)", "meters", "Terrain"
   "h_te_uncertainty", "Uncertainty of the mean terrain height for the segment", "meters", "Terrain"
   "last_seg_extend", "Distance the last ATL08 segment is extended or overlapped with the previous segment", "kilometers", "Land Segment"
   "latitude_20m", "Centre latitude of 20 m geosegments within each 100 m land segment (5 values per segment)", "degrees", "Land Segment"
   "layer_flag", "Consolidated cloud/blowing snow flag (0=absent, 1=likely present)", "adimensional", "Land Segment"
   "longitude_20m", "Centre longitude of 20 m geosegments within each 100 m land segment (5 values per segment)", "degrees", "Land Segment"
   "msw_flag", "Multiple scattering warning flag (-1 to 5; 0=no scattering, 5=highest scattering)", "adimensional", "Land Segment"
   "n_ca_photons", "Number of photons classified as canopy within the segment", "adimensional", "Canopy"
   "n_seg_ph", "Total number of photons within each land segment", "adimensional", "Land Segment"
   "n_te_photons", "Number of photons classified as terrain within the segment", "adimensional", "Terrain"
   "n_toc_photons", "Number of photons classified as top of canopy within the segment", "adimensional", "Canopy"
   "night_flag", "Day/night flag derived from solar elevation (0=day, 1=night)", "adimensional", "Land Segment"
   "ph_ndx_beg", "Index (1-based) of the first photon in this land segment within the photon-rate data", "adimensional", "Land Segment"
   "ph_removal_flag", "Flag indicating >50% of segment photons removed due to failing quality tests", "adimensional", "Land Segment"
   "photon_rate_can", "Photon rate of canopy photons within each 100 m segment", "s^-1", "Canopy"
   "photon_rate_can_nr", "Noise-removed photon canopy rate within each 100 m segment", "s^-1", "Canopy"
   "photon_rate_te", "Photon rate of terrain photons within each 100 m segment", "s^-1", "Terrain"
   "psf_flag", "Flag set to 1 if the point spread function (sigma_atlas_land) exceeds 1 m", "adimensional", "Land Segment"
   "rgt", "Reference ground track number (1–1387)", "adimensional", "Land Segment"
   "sat_flag", "Saturation flag derived from full_sat_fract on ATL03, averaged over 5 geosegments", "adimensional", "Land Segment"
   "segment_cover", "Average Copernicus fractional canopy cover percentage for each 100 m segment", "adimensional", "Canopy"
   "segment_id", "Unique segment identifier", "adimensional", "Reference"
   "segment_id_beg", "Geolocation segment number of the first photon in the land segment", "adimensional", "Land Segment"
   "segment_id_end", "Geolocation segment number of the last photon in the land segment", "adimensional", "Land Segment"
   "segment_landcover", "UN-FAO land cover surface type from Copernicus Land Cover (ANC18) at 100 m", "adimensional", "Land Segment"
   "segment_snowcover", "Daily snow/ice cover flag (0=ice-free water, 1=snow-free land, 2=snow, 3=ice)", "adimensional", "Land Segment"
   "segment_watermask", "Water mask from the Global Raster Water Mask (ANC33) at 250 m resolution", "adimensional", "Land Segment"
   "sigma_across", "Total cross-track geolocation uncertainty due to PPD and POD knowledge", "meters", "Land Segment"
   "sigma_along", "Total along-track geolocation uncertainty due to PPD and POD knowledge", "meters", "Land Segment"
   "sigma_atlas_land", "Total vertical geolocation error due to ranging and local surface slope", "meters", "Land Segment"
   "sigma_h", "1-sigma uncertainty of the reference photon bounce point ellipsoid height", "meters", "Land Segment"
   "sigma_topo", "Total uncertainty including sigma_h plus geolocation uncertainty due to local slope", "meters", "Land Segment"
   "snr", "Signal-to-noise ratio of geolocated photons", "adimensional", "Land Segment"
   "solar_azimuth", "Direction (eastwards from north) of the sun vector at the laser ground spot", "degrees_east", "Land Segment"
   "solar_elevation", "Solar angle above or below the ellipsoid tangent plane at the laser spot", "degrees", "Land Segment"
   "subset_can_flag", "Quality flag for canopy segments derived from <100 m or <5 ATL03 20 m segments (5 values per segment)", "adimensional", "Canopy"
   "subset_te_flag", "Quality flag for terrain segments derived from <100 m or <5 ATL03 20 m segments (5 values per segment)", "adimensional", "Terrain"
   "surf_type", "Surface type flags (land, ocean, sea ice, land ice, inland water; 5 values per segment)", "adimensional", "Land Segment"
   "terrain_flg", "Terrain flag indicating deviation above threshold from the reference DEM height", "adimensional", "Terrain"
   "terrain_slope", "Along-track slope of terrain computed by linear fit of terrain photons", "meters", "Terrain"
   "toc_roughness", "Standard deviation of top-of-canopy photon heights within segment", "meters", "Canopy"
   "urban_flag", "Flag indicating the segment is likely located over an urban area", "adimensional", "Land Segment"


Accessing the database
-----------------------

The `icesat2db` Python package simplifies access to the TileDB global database. Below is an example workflow for querying data.

**Example Code**:

.. code-block:: python

   import geopandas as gpd
   import icesat2db as isdb

   # Instantiate the IceSat2Provider
   provider = isdb.IceSat2Provider(
       storage_type='s3',
       s3_bucket="dog.icesat2db.icesat2-atl08-v007",
       url="https://s3.gfz-potsdam.de"
   )

   # Load region of interest (ROI)
   region_of_interest = gpd.read_file('path/to/region.geojson')

   # Query data
   atl08_data = provider.get_data(
       variables=["h_canopy", "h_te_best_fit"],
       query_type="bounding_box",
       geometry=region_of_interest,
       start_time="2019-01-01",
       end_time="2023-12-31",
       return_type='xarray'
   )

**Explanation**:

- **IceSat2Provider**: Initialises the provider with S3 storage details.
- **Region of Interest**: Defines the geographic area for the query using a GeoJSON file.
- **Variables**: Specifies the variables to extract (e.g., ``h_canopy``, ``h_te_best_fit``).
- **return_type**: ``'xarray'`` returns an ``xr.Dataset`` with coordinates and metadata attached; ``'dataframe'`` returns a ``pd.DataFrame``.

Examples and use cases
-----------------------

Here are some example use cases:

1. **Retrieve canopy height for a region**:

   .. code-block:: python

      atl08_data = provider.get_data(
          variables=["h_canopy", "h_max_canopy", "h_te_best_fit"],
          query_type="bounding_box",
          geometry=region_of_interest,
          start_time="2019-01-01",
          end_time="2023-12-31",
          return_type='xarray')

2. **Apply quality filters when querying**:

   .. code-block:: python

      atl08_data = provider.get_data(
          variables=["h_canopy", "h_te_best_fit"],
          query_type="bounding_box",
          geometry=region_of_interest,
          start_time="2019-01-01",
          end_time="2023-12-31",
          return_type='xarray',
          night_flag="== 1",
          layer_flag="== 0")

3. **Retrieve nearest shots to a point**:

   .. code-block:: python

      atl08_data = provider.get_data(
          variables=["h_canopy", "h_te_best_fit", "snr"],
          query_type="nearest",
          point=(13.4, 52.5),   # (longitude, latitude)
          num_shots=50,
          radius=0.5,
          start_time="2020-01-01",
          end_time="2023-12-31",
          return_type='dataframe')

Resources
---------
- `TileDB Documentation <https://tiledb.com/docs>`_
- `icesat2db GitHub Repository <https://github.com/simonbesnard1/icesat2db>`_
- `ICESat-2 ATL08 Product Overview <https://nsidc.org/data/atl08>`_
- `ATL08 Algorithm Theoretical Basis Document <https://nsidc.org/sites/default/files/documents/technical-reference/icesat2_atl08_atbd_v006.pdf>`_
