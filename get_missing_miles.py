# import win32com.client as win32

import logging
from pathlib import Path
import configparser
import re

from DatabaseConnector import DatabaseConnector
from PCMilerApiConnector import PCMilerApiConnector

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

#
# Get database credentials from config.ini
config = configparser.ConfigParser()
config.read('config.ini')


# SOURCE DATABASE QUERY (get origin and destination information)
source_database = DatabaseConnector(server=config['source_database_credentials']['server'],
                                    database=config['source_database_credentials']['database'],
                                    username=config['source_database_credentials']['username'],
                                    password=config['source_database_credentials']['password'])
source_database.connect()

shipping_routes_query = (Path.cwd() / "source_database_sql_fetch_shipping_routes.txt").read_text()
shipping_routes_df = source_database.fetch(sql_statement=shipping_routes_query)

source_database.close()


# DESTINATION DATABASE QUERY (get mileage information)
destination_database = DatabaseConnector(server=config['destination_database_credentials']['server'],
                                         database=config['destination_database_credentials']['database'],
                                         username=config['destination_database_credentials']['username'],
                                         password=config['destination_database_credentials']['password'])
destination_database.connect()

mileages_query = (Path.cwd() / "destination_database_sql_fetch_mileages.txt").read_text()
mileages_df = destination_database.fetch(sql_statement=mileages_query)

zip_codes_query = (Path.cwd() / "destination_database_sql_fetch_zip_codes.txt").read_text()
zip_codes_df = destination_database.fetch(sql_statement=zip_codes_query)

destination_database.close()


# Filter rows that have mileage less than 1 and invalid key (using regex)
filtered_mileages_df = mileages_df[mileages_df.Mileage < 1.0]
filtered_mileages_df = filtered_mileages_df[filtered_mileages_df.PCMiler_Key.str.match(na=False, pat='^[a-zA-Z\s]+_[a-zA-Z\s]+_[a-zA-Z0-9\s]+_[a-zA-Z\s]+_[a-zA-Z\s]+_[a-zA-Z0-9\s]+$')]

a = filtered_mileages_df.PCMiler_Key.str.match(na=False, pat='^[a-zA-Z\s]+_[a-zA-Z\s]+_[a-zA-Z0-9\s]+_[a-zA-Z\s]+_[a-zA-Z\s]+_[a-zA-Z0-9\s]+$')


# Perform PCMiler API call on filtered rows
pcMiler = PCMilerApiConnector()


# Find invalid mileages (mileage < 1.0)
invalid_destination_entries = []
for item in destination_entries.items():
    mileage, count = item[1].values()
    if mileage < 1.0:
        implausible_destination_entries.append(item)


# Replace ZIP code and run PCMiler again (different ZIP code but same city, state. E.g. 79732 instead of 79733)

# Find rows that still have invalid mileages and call Google Maps API for mileage




# # Recreate key for source entries to match key in destination table and add to source entry tuples
# source_entries_with_key = {}
# for item in source_entries:
#     org_city = item[0]
#     org_state = item[1]
#     org_zip = item[2]
#     dest_city = item[4]
#     dest_state = item[5]
#     dest_zip = item[6]
#     key = f'{org_city}_{org_state}_{org_zip}_{dest_city}_{dest_state}_{dest_zip}'
#     source_entries_with_key[key] = item
#
# # Only perform API call for source entries that are not present in destination table
# request_from_api_list = []
# for key, value in source_entries_with_key.items():
#     if key not in destination_entries.keys():
#         request_from_api_list.append(value)
# print(len(request_from_api_list))
#
# # Run API call
# pcMiler.perform_pc_miler_api_request(request_from_api=request_from_api_list)
#
# # self.dataOutputSQLSuccess()
#
# # Write API results back to database
# pcMiler.set_destinations()
# if len(pcMiler.error_list) > 0:
#     pcMiler.mileageError()
