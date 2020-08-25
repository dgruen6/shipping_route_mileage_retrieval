import logging
from pathlib import Path
import configparser
import pandas as pd

from Data_Models.Address import Address
from Data_Connectors.DatabaseConnector import DatabaseConnector
from Data_Connectors.PCMilerApiConnector import PCMilerApiConnector
from itertools import product

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
processed_shipping_routes = []
count = 0
for index, row in shipping_routes_df.iterrows():
    # Create route object from Route class
    route = Route(origin=Address(row['Origin City'], row['Origin State'], row['Origin Zip'], row['Origin_Country']),
                  destination=Address(row['Dest City'], row['Dest State'], row['Dest Zip'], row['Destination_Country']))
    # route = Route(origin=Address('NEW BEDFORD', 'MA', '2740', 'USA'),
    #               destination=Address('CLEVELAND', 'TN', '37311', 'USA'))
    # TODO: print route for bug fixing
    print(route)

    # Check if route exists in mileages_df (destination database) and mileage is valid
    route.mileage = route.find_mileage_in(df=mileages_df)
    if route.mileage:
        route.mileage_from_pcmiler_db = True

    # If no mileage found, run PCMiler API call
    if not route.mileage:
        route.mileage = pcMiler.get_mileage(*route.get_pcmiler_input())
        route.mileage_from_pcmiler_api = True

    # If mileage not valid, try to alternate the ZIP codes
    if not route.has_valid_mileage:
        new_org_zips = route.get_alternative_zip_code(for_='origin', zip_code_df=zip_codes_df)
        new_dest_zips = route.get_alternative_zip_code(for_='destination', zip_code_df=zip_codes_df)

        # Try API call with new origin ZIPs first (if possible)
        if new_org_zips:
            pcmiler_input = route.get_pcmiler_input()
            for new_org_zip in new_org_zips:
                pcmiler_input[2] = new_org_zip
                route.mileage = pcMiler.get_mileage(*pcmiler_input)
                if route.has_valid_mileage:
                    break

        # If no improvement, try with the new destination ZIPs (if possible)
        if not route.has_valid_mileage and new_dest_zips:
            pcmiler_input = route.get_pcmiler_input()
            for new_dest_zip in new_dest_zips:
                pcmiler_input[6] = new_dest_zip
                route.mileage = pcMiler.get_mileage(*pcmiler_input)
                if route.has_valid_mileage:
                    break

        # If still no improvement, try with both new origin and destination ZIPs (if possible)
        if not route.has_valid_mileage and new_org_zips and new_dest_zips:
            pcmiler_input = route.get_pcmiler_input()
            for new_org_zip, new_dest_zip in product(new_org_zips, new_dest_zips):
                pcmiler_input[2] = new_org_zip
                pcmiler_input[6] = new_dest_zip
                route.mileage = pcMiler.get_mileage(*pcmiler_input)
                if route.has_valid_mileage:
                    break

    # If alternating the ZIP code did not help, use GoogleMaps API to determine mileage
    # if not route.has_valid_mileage:
    #     route.mileage = route.get_mileage_from_google()

    # Save route information
    processed_shipping_routes.append(route)

    count += 1
    if count > 100:
        break


######################################################################
# LOAD
######################################################################
# Get stats on source database data (processed_shipping_routes_df)
statistics = Statistics(records=processed_shipping_routes)

# Write mileages back to destination database
destination_database = DatabaseConnector(server=config['destination_database_credentials']['server'],
                                         database=config['destination_database_credentials']['database'],
                                         username=config['destination_database_credentials']['username'],
                                         password=config['destination_database_credentials']['password'])

destination_database.connect()
destination_database_insert_query = (Path.cwd() / "SQL_Queries" / "destination_database_sql_commit_mileages.txt")\
    .read_text()
destination_database.commit(sql_statement=destination_database_insert_query, insert_data=processed_shipping_routes)
destination_database.close()
