import os

import boto3
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.patches as patches
import matplotlib.pyplot as plt
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


fragment_info = tiledb.FragmentInfoList(scalar_array_uri, ctx=ctx)

# Select a fragment for the 3D spatio-temporal visualization
selected_fragment = fragment_info[0]
with tiledb.open(scalar_array_uri, mode="r", ctx=ctx) as array:
    # Non-empty domain for the selected fragment
    lat_min, lat_max = selected_fragment.nonempty_domain[0]
    lon_min, lon_max = selected_fragment.nonempty_domain[1]
    time_min, time_max = selected_fragment.nonempty_domain[2]

    # Query the GEDI shots within the fragment's bounding box
    data = array.query(attrs=["shot_number"])[
        lon_min:lon_max, lat_min:lat_max, time_min:time_max
    ]

# Extract latitude, longitude, and time from the query result
gedi_lats = data["latitude"]
gedi_lons = data["longitude"]
gedi_times = data["time"]


# %% Create the figure with subplots
fig = plt.figure(figsize=(18, 6.5), constrained_layout=True)

# 1. Global Distribution Map
ax1 = fig.add_subplot(1, 2, 1, projection=ccrs.Robinson())
ax1.add_feature(cfeature.LAND)
ax1.add_feature(cfeature.OCEAN)
ax1.add_feature(cfeature.COASTLINE)
ax1.add_feature(cfeature.BORDERS, linestyle=":")

# Plot all fragments
for fragment in fragment_info:
    frag_lat_min, frag_lat_max = fragment.nonempty_domain[0]
    frag_lon_min, frag_lon_max = fragment.nonempty_domain[1]
    rect = patches.Rectangle(
        (frag_lon_min, frag_lat_min),
        frag_lon_max - frag_lon_min,
        frag_lat_max - frag_lat_min,
        linewidth=1,
        edgecolor="red",
        facecolor="none",
        transform=ccrs.PlateCarree(),
    )
    ax1.add_patch(rect)

ax1.set_global()
ax1.gridlines(draw_labels=True, linewidth=0.5, color="gray", alpha=0.5, linestyle="--")
ax1.set_title("Global distribution of tileDB fragments", y=1.12, fontweight="bold")

# 2. Spatio-Temporal 3D Stack
ax2 = fig.add_subplot(1, 2, 2, projection="3d")

# Plotting the spatio-temporal data
scatter = ax2.scatter(
    gedi_lons,
    gedi_lats,
    gedi_times,
    c=gedi_times,
    cmap="viridis",
    s=1,
    alpha=0.8,
    marker="o",
)

# Labels and title
ax2.set_xlabel("Longitude")
ax2.set_ylabel("Latitude")
ax2.set_zlabel("Timestamp [since 1970-01-01]", rotation=90)
ax2.set_title(
    "Spatio-temporal structure of one TileDB fragment", y=1, fontweight="bold"
)

# Adjusting view for better perspective
ax2.view_init(elev=20, azim=500)
plt.savefig(
    "/home/simon/Documents/science/GFZ/projects/gedidb/doc/_static/images/tileDB_fragment_structure.png",
    dpi=300,
)
