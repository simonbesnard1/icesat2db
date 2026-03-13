# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#


import geopandas as gpd

import icesat2db as idb

import time
start_time_ = time.time()


# %% Instantiate the GEDIProvider
provider = idb.IceSat2Provider(
    storage_type='local', 
    local_path ='/home/simon/Documents/science/GFZ/projects/icesat2db/data/',
)

# %% Load region of interest
region_of_interest = gpd.read_file(
    "/home/simon/Downloads/thuringia_bbox.geojson"
)

# Define the columns to query and additional parameters
vars_selected = ["h_canopy"]

# Profile the provider's `get_data` function
icesat2_data = provider.get_data(
    variables=vars_selected,
    query_type="bounding_box",
    geometry=region_of_interest,
    start_time="2018-01-01",
    end_time="2024-07-25",
    return_type="xarray"
)
print(icesat2_data)
print("--- %s seconds ---" % (time.time() - start_time_))
