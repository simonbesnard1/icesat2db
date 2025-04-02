#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 09:36:25 2025

@author: simon
"""
import os

import boto3
import tiledb

granule_ids_to_remove = [
    "O20419_02", "O20423_03", "O20426_02", "O20429_01", "O20431_04", "O20434_03", "O20437_02", "O20440_01", "O20442_04",
    "O20445_03", "O20448_02", "O20451_01", "O20454_01", "O20456_04", "O20459_03", "O20462_02", "O20465_01", "O20467_04",
    "O20470_03", "O20420_02", "O20423_04", "O20426_03", "O20429_02", "O20432_01", "O20434_04", "O20437_03", "O20440_02",
    "O20443_01", "O20445_04", "O20448_03", "O20451_02", "O20454_02", "O20457_01", "O20459_04", "O20462_03", "O20465_02",
    "O20468_01", "O20470_04", "O20420_03", "O20424_01", "O20426_04", "O20429_03", "O20432_02", "O20435_01", "O20437_04",
    "O20440_03", "O20443_02", "O20446_01", "O20448_04", "O20451_03", "O20454_03", "O20457_02", "O20460_01", "O20462_04",
    "O20465_03", "O20468_02", "O20471_01", "O20421_02", "O20424_02", "O20427_01", "O20429_04", "O20432_03", "O20435_02",
    "O20438_01", "O20440_04", "O20443_03", "O20446_02", "O20449_01", "O20452_01", "O20454_04", "O20457_03", "O20460_02",
    "O20463_01", "O20465_04", "O20468_03", "O20471_02", "O20421_03", "O20424_03", "O20427_02", "O20430_01", "O20432_04",
    "O20435_03", "O20438_02", "O20441_01", "O20443_04", "O20446_03", "O20449_02", "O20452_02", "O20455_01", "O20457_04",
    "O20460_03", "O20463_02", "O20466_01", "O20468_04", "O20471_03", "O20422_01", "O20424_04", "O20427_03", "O20430_02",
    "O20433_01", "O20435_04", "O20438_03", "O20441_02", "O20444_01", "O20446_04", "O20449_03", "O20452_03", "O20455_02",
    "O20458_01", "O20460_04", "O20463_03", "O20466_02", "O20469_01", "O20472_01", "O20422_02", "O20425_01", "O20427_04",
    "O20430_03", "O20433_02", "O20436_01", "O20438_04", "O20441_03", "O20444_02", "O20447_01", "O20449_04", "O20452_04",
    "O20455_03", "O20458_02", "O20461_01", "O20463_04", "O20466_03", "O20469_02", "O20472_02", "O20422_03", "O20425_02",
    "O20428_01", "O20430_04", "O20433_03", "O20436_02", "O20439_01", "O20441_04", "O20444_03", "O20447_02", "O20450_01",
    "O20453_01", "O20455_04", "O20458_03", "O20461_02", "O20464_01", "O20466_04", "O20469_03", "O20472_03", "O20422_04",
    "O20425_03", "O20428_02", "O20431_01", "O20433_04", "O20436_03", "O20439_02", "O20442_01", "O20444_04", "O20447_03",
    "O20450_02", "O20453_02", "O20456_01", "O20458_04", "O20461_03", "O20464_02", "O20467_01", "O20469_04", "O20472_04",
    "O20423_01", "O20425_04", "O20428_03", "O20431_02", "O20434_01", "O20436_04", "O20439_03", "O20442_02", "O20445_01",
    "O20447_04", "O20450_03", "O20453_03", "O20456_02", "O20459_01", "O20461_04", "O20464_03", "O20467_02", "O20470_01",
    "O20423_02", "O20426_01", "O20428_04", "O20431_03", "O20434_02", "O20437_01", "O20439_04", "O20442_03", "O20445_02",
    "O20448_01", "O20450_04", "O20453_04", "O20456_03", "O20459_02", "O20462_01", "O20464_04", "O20467_03", "O20470_02"
]

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
with tiledb.Array(scalar_array_uri, mode='w',  ctx=ctx) as A:
    for granule_id in granule_ids_to_remove:
        key = f"granule_{granule_id}_status"
        del A.meta[key]
        print(f"Removed metadata key: {key}")

