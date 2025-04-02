# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
import json
import os

import boto3
import tiledb

# Initialize boto3 session for S3 credentials
session = boto3.Session()
creds = session.get_credentials()
# S3 TileDB context with consolidation settings
tiledb_config = tiledb.Config(
    {
        # Consolidation settings
        "sm.consolidation.steps": 10,
        "sm.consolidation.step_max_frags": 100,  # Adjust based on fragment count
        "sm.consolidation.step_min_frags": 10,
        "sm.consolidation.buffer_size": 5_000_000_000,  # 5GB buffer size per attribute/dimension
        "sm.consolidation.step_size_ratio": 0.5,  #  allow fragments that differ by up to 50% in size to be consolidated.
        "sm.consolidation.amplification": 1.2,  #  Allow for 20% amplification
        # Memory budget settings
        "sm.memory_budget": "150000000000",  # 150GB total memory budget
        "sm.memory_budget_var": "50000000000",  # 50GB for variable-sized attributes
        # S3-specific configurations (if using S3)
        "vfs.s3.aws_access_key_id": creds.access_key,
        "vfs.s3.aws_secret_access_key": creds.secret_key,
        "vfs.s3.endpoint_override": "https://s3.gfz-potsdam.de",
        "vfs.s3.region": "eu-central-1",
    }
)


ctx = tiledb.Ctx(tiledb_config)

bucket = "dog.gedidb.gedi-l2-l4-v002"
scalar_array_uri = os.path.join(f"s3://{bucket}", "array_uri")

with tiledb.open(scalar_array_uri, mode="r", ctx=ctx) as scalar_array:
    scalar_metadata = {
        key: scalar_array.meta[key]
        for key in scalar_array.meta.keys()
        if "_status" in key
    }

fragment_info = tiledb.FragmentInfoList(scalar_array_uri, ctx=ctx)

i = 0
for fragment in fragment_info:

    # Extract the nonempty domain
    nonempty_domain = fragment.nonempty_domain
    latitude_range = nonempty_domain[0]
    longitude_range = nonempty_domain[1]

    # Create a GeoJSON Feature for the nonempty domain
    geojson_feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [longitude_range[0], latitude_range[0]],
                    [longitude_range[1], latitude_range[0]],
                    [longitude_range[1], latitude_range[1]],
                    [longitude_range[0], latitude_range[1]],
                    [longitude_range[0], latitude_range[0]],
                ]
            ],
        },
    }

    # Wrap in a GeoJSON FeatureCollection
    geojson = {"type": "FeatureCollection", "features": [geojson_feature]}

    # Save the GeoJSON to a file
    geojson_file_path = (
        "/home/simon/Downloads/fragments_/tiledb_fragment_{n_fragment}.geojson".format(
            n_fragment=str(i)
        )
    )
    with open(geojson_file_path, "w") as file:
        json.dump(geojson, file, indent=4)
    i += 1


# %% Check specific fragments

# Open the array and fetch the FragmentInfoList
fragment_info = tiledb.FragmentInfoList(scalar_array_uri, ctx=ctx)

# Target schema name to search for
target_schema_name = "__1734187714658_1734187714658_13d3a250e5442bf5a653574e8cb8cf46_22"

# Loop over the fragment info to find the matching schema name
matching_fragments = []
for fragment in fragment_info:
    if os.path.basename(fragment.uri) == target_schema_name:
        matching_fragments.append(fragment)

# Print results
if matching_fragments:
    for fragment in matching_fragments:
        print(
            f"Found matching fragment:\nURI: {fragment.uri}\nNon-empty domain: {fragment.nonempty_domain}\n"
        )
else:
    print(f"No fragment found with array_schema_name: {target_schema_name}")
