import configparser
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from Data_Connectors.DatabaseConnector import DatabaseConnector
from Data_Connectors.PCMilerApiConnector import PCMilerApiConnector
from Data_Models.Address import Address
from Data_Models.Route import Route
from Data_Models.Statistics import Statistics

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# Get database credentials from config.ini
config = configparser.ConfigParser()
config.read('config.ini')


######################################################################
# EXTRACT
######################################################################
# SOURCE DATABASE QUERY (get origin and destination information)
source_database = DatabaseConnector(server=config['source_database_credentials']['server'],
                                    database=config['source_database_credentials']['database'],
                                    username=config['source_database_credentials']['username'],
                                    password=config['source_database_credentials']['password'])

source_database.connect()
shipping_routes_query = (Path.cwd() / "SQL_Queries" / "source_database_sql_fetch_shipping_routes.txt").read_text()
shipping_routes_df = source_database.fetch(sql_statement=shipping_routes_query)
source_database.close()


# DESTINATION DATABASE QUERY (get mileage information)
destination_database = DatabaseConnector(server=config['destination_database_credentials']['server'],
                                         database=config['destination_database_credentials']['database'],
                                         username=config['destination_database_credentials']['username'],
                                         password=config['destination_database_credentials']['password'])

destination_database.connect()
mileages_query = (Path.cwd() / "SQL_Queries" / "destination_database_sql_fetch_mileages.txt").read_text()
mileages_df = destination_database.fetch(sql_statement=mileages_query)
zip_codes_query = (Path.cwd() / "SQL_Queries" / "destination_database_sql_fetch_zip_codes.txt").read_text()
zip_codes_df = destination_database.fetch(sql_statement=zip_codes_query)
destination_database.close()

# TODO: remove this once resolved
# This is a temporary solution until the zip codes are on the database
zip_codes_df = pd.read_csv("zipcode-database.csv", usecols=['Zipcode', 'City', 'State', 'Country'])


######################################################################
# TRANSFORM
######################################################################
pcMiler = PCMilerApiConnector()

# Get mileages for shipping_routes_df (source database)
logging.info("Retrieving mileage for routes...")
processed_shipping_routes = []

for index, row in tqdm(shipping_routes_df.iterrows(), total=shipping_routes_df.shape[0]):
    # Create route object from Route class
    route = Route(origin=Address(row['Origin City'], row['Origin State'], row['Origin Zip'], row['Origin_Country']),
                  destination=Address(row['Dest City'], row['Dest State'], row['Dest Zip'], row['Destination_Country']),
                  mileage_type=pcMiler.mileage_type)
    # route = Route(origin=Address('NEW BEDFORD', 'MA', '2740', 'USA'),
    #               destination=Address('CLEVELAND', 'TN', '37311', 'USA'),
    #               mileage_type=pcMiler.mileage_type)
    logging.info(f"Origin: {route.origin}, Destination: {route.destination}")

    # Check if route exists in mileages_df (destination database) and mileage is valid
    route.mileage = route.find_mileage_in(df=mileages_df)

    # If no mileage found, run PCMiler API call
    if not route.mileage:
        route.mileage = pcMiler.get_mileage(*route.get_pcmiler_input())
        route.mileage_from_pcmiler_api = True

    # If mileage not valid, try to alternate the origin ZIP code
    if not route.has_valid_mileage:
        pcMiler.get_mileage_with_alternative_zip_code(alternate='origin', route=route, zip_codes_df=zip_codes_df)

    # If no improvement, try alternating the destination ZIP code
    if not route.has_valid_mileage:
        pcMiler.get_mileage_with_alternative_zip_code(alternate='destination', route=route, zip_codes_df=zip_codes_df)

    # TODO: include GoogleMaps API call

    # Save route information
    processed_shipping_routes.append(route)


######################################################################
# LOAD
######################################################################
# Get stats on source database data (processed_shipping_routes_df)
statistics = Statistics(records=processed_shipping_routes)
statistics.overview.to_csv("processed_routes_statistics.csv")

# Write mileages back to destination database
destination_database = DatabaseConnector(server=config['destination_database_credentials']['server'],
                                         database=config['destination_database_credentials']['database'],
                                         username=config['destination_database_credentials']['username'],
                                         password=config['destination_database_credentials']['password'])

destination_database.connect()
insert_query = (Path.cwd() / "SQL_Queries" / "destination_database_sql_commit_mileages.txt").read_text()
insert_data = [[route.origin.city, route.origin.state, route.origin.zip_code,
                route.origin.zip_code_modified, route.origin.country,
                route.destination.city, route.destination.state, route.destination.zip_code,
                route.destination.zip_code_modified, route.destination.country,
                route.mileage_type, route.mileage, datetime.now()] for route in processed_shipping_routes]
destination_database.commit(sql_statement=insert_query, insert_data=insert_data)
destination_database.close()
