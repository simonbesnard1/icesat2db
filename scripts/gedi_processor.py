#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 19 19:57:34 2025

@author: simon
"""
import concurrent.futures
import icesat2db as idb
import boto3

# Get credentials
session = boto3.Session()
creds = session.get_credentials()
credentials = {
    "AccessKeyId": creds.access_key,
    "SecretAccessKey": creds.secret_key
}

# Paths to configuration files
config_file = "/home/simon/Documents/science/GFZ/projects/icesat2db/data/config_files/data_config.yml"

concurrent_engine = concurrent.futures.ThreadPoolExecutor(max_workers=6)

# Initialize the GEDIProcessor and compute
with idb.IceSat2Processor(
    config_file=config_file,
    geometry="/home/simon/Downloads/thuringia_bbox.geojson",
    start_date="2019-01-01",
    end_date="2025-01-31",
    earth_data_dir="/home/simon/",
    parallel_engine=concurrent_engine,
    credentials= credentials
) as processor:
    processor.compute(consolidate=True)




