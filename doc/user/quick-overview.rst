.. _overview:

##############
Quick Overview
##############

This section provides brief examples of using :py:class:`icesat2db.IceSat2Processor` and :py:class:`icesat2db.IceSat2Provider` to process and query **IceSat2** data. For advanced features and detailed use cases, refer to the :ref:`fundamentals`.

Start by importing the **icesat2db** package:

.. code-block:: python

    import icesat2db as isdb
    import concurrent.futures

Processing ICESat-2 Data
------------------------

To process ICESat-2 data, specify paths to a ``YAML`` configuration file (`config_file`). See :ref:`fundamentals-setup` for more information on the data configuration files.

This setup initiates the download, processing, and storage of ICESat-2 data in your database.

.. code-block:: python

    # Paths to configuration files
    config_file = 'path/to/config_file.yml'
    geometry = 'path/to/test.geojson'

    # Initialize a parallel engine
    concurrent_engine= concurrent.futures.ThreadPoolExecutor(max_workers=10)

    # Initialize the IceSat2Processor and compute
    with isdb.IceSat2Processor(
        config_file=config_file,
        geometry=geometry,
        start_date='2020-01-01',
        end_date='2020-12-31',   
        earth_data_dir= '/path/to/earthdata_credential/folder',
        parallel_engine=concurrent_engine, 
    ) as processor:
        processor.compute(consolidate=True)


In this example, the :py:class:`icesat2db.IceSat2Processor` performs:

- **Downloading** ICESat-2 ATL08 product.
- **Filtering** data by quality.
- **Storing** the processed data in the tileDB database.

Querying ICESat-2 Data
----------------------

Once the data is processed and stored, use :py:class:`icesat2db.IceSat2Provider` to query it. The results can be returned in either **Xarray** or **Pandas** format, providing flexibility for various workflows.

Example query using :py:class:`icesat2db.IceSat2Provider`:

.. code-block:: python
    
    import geopandas as gpd
    import icesat2db as gdb

    # Create IceSat2Provider instance
    provider = gdb.IceSat2Provider(storage_type='local', 
                                local_path= "path/to/your/database/")

    # Load region of interest
    region_of_interest = gpd.read_file('./data/geojson/BR-Sa1.geojson')

    # Define the columns to query and additional parameters
    vars_selected = ["brightness_flag"]
    
    # Profile the provider's `get_data` function
    icesat2_data = provider.get_data(
        variables=vars_selected,
        query_type="bounding_box",
        geometry=region_of_interest,
        start_time="2018-01-01",
        end_time="2024-07-25",
        return_type='xarray'
    )

This :py:class:`provider.get_data()` function allows you to:

- **Select specific columns** (e.g., `brightness_flag`).
- **Apply spatial and temporal filters** using `geometry`, `start_time`, and `end_time`.
- **Return data** in either `xarray` or `pandas` format based on `return_type`.

This functionality offers a flexible, scalable approach to querying ICESat-2 data, streamlining its integration into your data workflows.

---

