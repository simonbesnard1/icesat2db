<p align="center">
  <a href="https://github.com/simonbesnard1/icesat2db">
    <img src="https://raw.githubusercontent.com/simonbesnard1/icesat2db/main/doc/_static/logos/icesat2db_logo.png"
         alt="icesat2db Logo" width="400" style="margin: 30px 0;">
  </a>
</p>

# icesat2db: A toolbox for Ice, Cloud, and land Elevation Satellite 2 (IceSat2) ATL08 data

[![Pipelines](https://github.com/simonbesnard1/icesat2db/actions/workflows/ci.yaml/badge.svg)](https://github.com/simonbesnard1/icesat2db/actions?query=workflow%3ACI)
[![Code coverage](https://codecov.io/gh/simonbesnard1/icesat2db/branch/main/graph/badge.svg?flag=unittests)](https://codecov.io/gh/simonbesnard1/icesat2db)
[![Docs](https://readthedocs.org/projects/icesat2db/badge/?version=latest)](https://icesat2db.readthedocs.io/en/latest/)
<!--- [![Available on PyPI](https://img.shields.io/pypi/v/gedidb.svg)](https://pypi.python.org/pypi/gedidb/) --->
<!--- [![PyPI Downloads](https://static.pepy.tech/badge/gedidb)](https://pepy.tech/projects/gedidb) --->
<!--- [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.13885229.svg)](https://doi.org/10.5281/zenodo.13885228) --->
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**icesat2db** is an open-source Python package designed to streamline the processing, analysis, and management of IceSat2 ATL08 data. This toolbox enables efficient and flexible data querying and management of large IceSat2 datasets stored with [TileDB](https://tiledb.com/), a high-performance, multi-dimensional array database.

**icesat2db** integrates key functionalities such as structured data querying, multi-dimensional data processing, and metadata management. With built-in support for parallel engines (e.g. Dask), the toolbox ensures scalability for large datasets, allowing efficient parallel processing on local machines or clusters.

## Key Features of icesat2db

- **TileDB-Based Storage**: IceSat2 data is stored and managed in TileDB arrays, providing efficient, scalable, multi-dimensional data storage, enabling fast and flexible access to large volumes of data.
- **Flexible Data Querying**: Easily query IceSat2 data across spatial, temporal, and variable dimensions. Access data within bounding boxes, or retrieve the nearest shots to a specific location, using intuitive filtering options for precision.
- **Parallel Processing**: Process large IceSat2 datasets in parallel, enabling concurrent downloading, processing, and TileDB insertion of IceSat2 products. The number of concurrent processes can be easily controlled based on available system resources.
- **Metadata-Driven**: Maintain and manage metadata for each dataset, ensuring that important contextual information like units, descriptions, and source details are stored and accessible.
- **Geospatial Data Management**: Integrate seamlessly with tileDB to enable spatial queries, transformations, and geospatial analyses.

## Why icesat2db?
**icesat2db** simplifies and automates the workflow for IceSat2 data processing, making it easier to retrieve, filter, and analyze complex datasets in an efficient, scalable manner. Whether you're investigating biomass distribution, monitoring forest dynamics, or conducting large-scale ecological studies, **icesat2db** supports users with tools to handle and analyze large IceSat2 datasets with ease.

## Documentation

Learn more about icesat2db in its official documentation at
<https://icesat2db.readthedocs.io/en/latest/>.

## Contributing

You can find information about contributing to icesat2db on our
[Contributing page](https://icesat2db.readthedocs.io/en/latest/user/contributing.html).

## About the authors

Felix Dombrowski is a Bachelor’s student in Computer Science at the University of Potsdam and a research intern in the Global Land Monitoring Group at GFZ Helmholtz Centre Potsdam. At GFZ, his work has focused on developing toolboxes to process Earth Observation data efficiently.

Mikhail Urbazaev is a senior researcher in the Global Land Monitoring Group at GFZ Helmholtz Centre Potsdam.

Simon Besnard, a senior researcher in the Global Land Monitoring Group at GFZ Helmholtz Centre Potsdam, studies terrestrial ecosystems' dynamics and their feedback on environmental conditions. He specializes in developing methods to analyze large EO and climate datasets to understand ecosystem functioning in a changing climate. His current research focuses on forest structure changes over the past decade and their links to the carbon cycle. 

Amelia Holcomb is a PhD candidate in Computer Science at the University of Cambridge, researching remote sensing and machine learning to study carbon sequestration and forest regrowth. Previously, she worked as a site reliability engineer at Google on Bigtable. She holds an MMath from the University of Waterloo and a B.A. in Mathematics from Yale.

## Contact

For any questions or inquiries, please contact:
- Felix Dombrowski (felixd@gfz.de)
- Mikhail Urbazaev (urbazaev@gfz.de)
- Simon Besnard (besnard@gfz.de)
- Amelia Holcomb (ah2174@cam.ac.uk)

## Acknowledgments
The development of gediDB was supported by the European Union through the [FORWARDS](https://forwards-project.eu/) and [NextGenCarbon](https://www.nextgencarbon-project.eu/) projects, and by the Helmholtz Association via the Helmholtz Foundation Model Initiative ([3D-ABC project](https://www.3d-abc.ai/)). Amelia Holcomb acknowledges funding from the Harding Distinguished Postgraduate Scholarship.

## License
This project is licensed under the EUROPEAN UNION PUBLIC LICENCE v.1.2 License - see the LICENSE file for details.
