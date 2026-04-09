.. _fundamentals-setup:

*******************
Configuration files
*******************

To maximize the functionality of icesat2DB, it’s essential to configure key settings using the `data_config.yml` file. These files specify important parameters, ensuring efficient data handling, database connection, and alignment with your processing needs.

The `data_config.yml` file is the main configuration file for settings related to data retrieval, database connectivity, and file management. Key configurations include:

 - **Database Connection Details**: Define database connection variables like `storage_type`, `dimensions`, `temporal_batching`, and `consolidation_settings`.
 - **File Paths**: Specify directories for storing downloaded ICESat-2 data, processed files, and metadata.
 - **Environment Settings**: Configure parameters for parallel processing and resource allocation.
 - **Data Extraction Settings**: Control which variables to extract from ICESat-2 `.h5` files to streamline storage and improve processing efficiency.

A default data configuration file (`data_config.yml`) can be downloaded here:

:download:`Download data_config.yml <../_static/test_files/data_config.yml>`

**Extracted data from .h5 Files**

ICESat-2 `.h5` files contain extensive data, but icesat2DB allows you to specify only the essential variables you need. This configuration not only reduces storage requirements but also speeds up data processing.

For instance, each ICESat-2 product, like **Level ATL08**, can have a dedicated configuration section, allowing tailored data extraction. Below is an example specifying selected variables for **Level ATL08**:

.. code-block:: yaml

    level_atl08:
        variables:
            asr:
                SDS_Name: "land_segments/asr"
                dtype: "float32"
                DIMENSION_LIST: "[array([<HDF5 object reference>], dtype=object)]"
                _FillValue: "3.4028234663852886e+38"
                contentType: "auxiliaryInformation"
                coordinates: "delta_time latitude longitude"
                description: "Apparent surface reflectance"
                long_name: "apparent surface reflectance"
                source: "ATL09"
                units: "1"

**Spatial and Temporal Parameters**

Define **spatial** and **temporal** parameters to set boundaries for the data queries. These settings specify which ICESat-2 granules to retrieve, based on the region and time range of interest.

.. code-block:: yaml

    region_of_interest: './path/to/file.geojson'
    start_date: '2019-01-01'
    end_date: '2022-01-01'

- **`region_of_interest`**: Path to a GeoJSON file defining the spatial area of interest, such as a polygon or multipolygon.
- **`start_date`** and **`end_date`**: Define the time range for data retrieval.

**Example GeoJSON polygon**

Here is an example of a GeoJSON polygon file that could be used for the `region_of_interest`:

.. code-block:: json

    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": {},
          "geometry": {
            "type": "Polygon",
            "coordinates": [
              [
                [30.256673359035123, -15.85375449790373],
                [30.422423359035125, -15.85375449790373],
                [30.422423359035125, -15.62525449790373],
                [30.256673359035123, -15.62525449790373],
                [30.256673359035123, -15.85375449790373]
              ]
            ]
          }
        }
      ]
    }

Download an example `test.geojson` file here:

:download:`Download test.geojson <../_static/test_files/test.geojson>`

**tileDB Configuration**

The `data_config.yml` file also includes settings for configuring the database connection. These settings include:

.. code-block:: yaml

    tiledb:
      storage_type: 'local'                             # either local or s3
      local_path: ''                                    # TileDB URI for storing data
      overwrite: true                                   # Whether to overwrite existing arrays
      temporal_batching: "weekly"                       # either daily, weekly, or annual
      latitude_tile: 6                                  # spatial tile size in degrees (latitude)
      longitude_tile: 6                                 # spatial tile size in degrees (longitude)
      flush_every: 20000                                # flush buffers every N granules to bound memory
      time_range:                                       # Global time range for data
        start_time: "2018-01-01"                        # Global start time for data
        end_time: "2030-12-31"                          # Global end time for data
      spatial_range:                                    # Global spatial range (bounding box)
        lat_min: -90.0
        lat_max: 90.0
        lon_min: -180.0
        lon_max: 180.0
      dimensions: ['latitude', 'longitude', 'time']     # Dimensions for the TileDB array
      consolidation_settings:
       fragment_size: 200000000000                      # 200GB fragment size
       memory_budget: "150000000000"                    # 150GB total memory budget
       memory_budget_var: "50000000000"                 # 50GB for variable-sized attributes
      cell_order: "hilbert"
      capacity: 100000

Users are free to modify these settings to suit their specific requirements, such as changing the `storage_type` to `s3` for cloud storage or adjusting the `temporal_batching` to `daily` or `annual` for different temporal granularity.
Be aware that modifying these settings are for advanced users and may require additional knowledge of the TileDB library.
