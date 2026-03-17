.. _faq:

################################
Frequently Asked Questions (FAQ)
################################

How should I cite icesat2db?
----------------------------

Please use the following citation when referencing icesat2db in your work:

> Besnard, S., Dombrowski, F., & Holcomb, A. (2025). icesat2db (2025.2.0). Zenodo. https://doi.org/10.5281/zenodo.13885229

What are the main features of icesat2db?
----------------------------------------

GediDB is a TileDB-based Python package designed to efficiently manage, query, and analyze large-scale IceSat2 data. Its main features include:

- **Efficient data storage**: Stores IceSat2 data using TileDB arrays, enabling optimized access and scalability.
- **Geospatial querying**: Provides spatially enabled querying for regions of interest with support for bounding boxes and polygons.
- **Automated processing**: Facilitates loading, pre-filtering, and processing of IceSat2 ATL08 data product.
- **Parallelized operations**: Leverages parallel engines (e.g., Dask) for distributed data processing, enhancing performance on large datasets.
- **Integration with Python libraries**: Outputs data in formats compatible with pandas, geopandas, and xarray for seamless analysis.

How do I set up the database for IceSat2?
-----------------------------------------

The TileDB database is set up automatically using the `icesat2db` package. By default, it creates and manages the schema required for IceSat2 data. If you prefer to use a pre-existing database, ensure that the structure aligns with the schema defined by `icesat2db`.

What data products does icesat2db support?
------------------------------------------

icesat2db (currently) supports the following IceSat2 data products:

- **Level ATL08**

Can I use the IceSat2 database on cloud-hosted databases?
---------------------------------------------------------

Yes, the IceSat2 database can be deployed on cloud-hosted storage systems like AWS S3. Use TileDB’s integration with cloud platforms to store and access IceSat2 data seamlessly. Refer to the cloud storage documentation in TileDB for setup instructions.

Can I add data to my IceSat2 database?
--------------------------------------

Yes, you can add data to an existing IceSat2 database using `icesat2db`. Simply configure the database with the appropriate schema and use the :py:class:`icesat2db.IceSat2Processor` class to process and ingest new data. Make sure to backup your database before making modifications.

How do I write IceSat2 data into the database?
----------------------------------------------

IceSat2 data can be written to the database using the :py:class:`icesat2db.IceSat2Processor` class. Steps include:

1. Configure the `data_config.yml` file with paths to your IceSat2 HDF5 files and database settings.
2. Use the :py:class:`icesat2db.IceSat2Processor` to process and insert data into the TileDB database.
3. Monitor logs for any errors or warnings during processing.

How do I contribute to icesat2db development?
---------------------------------------------

We welcome contributions to icesat2db! Here’s how you can help:

- **Report issues**: If you encounter bugs or have suggestions, report them on our GitHub issue tracker.
- **Submit pull requests**: Contribute code for bug fixes, new features, or performance improvements.
- **Improve documentation**: Help expand the documentation by providing additional examples or clarifications.

For detailed contribution guidelines, please check the :ref:`devindex`. Additionally, join discussions on our GitHub repository to engage with the development community.

