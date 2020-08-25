import logging
from pathlib import Path
import configparser
from typing import List, Optional
import pandas as pd

from DatabaseConnector import DatabaseConnector
from PCMilerApiConnector import PCMilerApiConnector

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


######################################################################
# TRANSFORM
######################################################################
class Address:
    def __init__(self, city: str, state: str, zip_code: str, country: str = None):
        self.city: str = city
        self.state: str = state
        self.zip: str = zip_code
        self.country: str = country


class Route:
    def __init__(self, origin: Address, destination: Address):
        self.origin: Address = origin
        self.destination: Address = destination
        self.mileage: float

    def get_alternative_zip_code(self, for_: str, zip_code_df: pd.DataFrame) -> Optional[str]:
        """
        Get alternative zip code based on given zip code dataframe

        :param for_: 'origin' or 'destination'
        :param zip_code_df: Pandas dataframe which will be used to find an alternative ZIP code
        :return: Alternative zip code if possible, otherwise None
        """
        pass

    def get_key(self) -> str:
        """
        Returns route key used to identify a route in destination database

        :return: Route key
        """
        key = f"{self.origin.city.upper()}_{self.origin.state.upper()}_{self.origin.zip.upper()}" \
              f"_{self.destination.city.upper()}_{self.destination.state.upper()}_{self.destination.zip.upper()}"
        return key

    def get_pcmiler_input(self) -> List:
        """
        Get the input list for the PCMiler API call (city, state, zip, country) for origin and destination

        :return: List [origin(city, state, zip, country), destination(city, state, zip, country)]
        """
        return [self.origin.city, self.origin.state, self.origin.zip, self.origin.country,
                self.destination.city, self.destination.state, self.destination.zip, self.destination.country]

    def find_mileage_in(self, df: pd.DataFrame) -> Optional[float]:
        """
        Find route in given dataframe (extracted from database)

        :param df: Pandas dataframe
        :return: FLoat mileage if route found in given dataframe, None otherwise
        """
        pass

    def get_mileage_from_google(self) -> float:
        pass


pcMiler = PCMilerApiConnector()

# Get mileages for shipping_routes_df (source database)
processed_shipping_routes = []
for index, row in shipping_routes_df.iterrows():
    # Create route object from Route class
    route = Route(origin=Address(row['Origin City'], row['Origin State'], row['Origin Zip'], row['Origin_Country']),
                  destination=Address(row['Dest City'], row['Dest State'], row['Dest Zip'], row['Destination_Country']))

    # Check if route exists in mileages_df (destination database) and mileage > 1.0
    route.mileage = route.find_mileage_in(df=mileages_df)
    if route.mileage and route.mileage > 1.0:
        continue

    # If no mileage found, run PCMiler API call
    if not route.mileage:
        route.mileage = pcMiler.get_mileage(*route.get_pcmiler_input())

    # If mileage < 1.0, try to alternate the ZIP codes
    if route.mileage < 1.0:
        new_org_zip = route.get_alternative_zip_code(for_='origin', zip_code_df=zip_codes_df)
        new_dest_zip = route.get_alternative_zip_code(for_='destination', zip_code_df=zip_codes_df)

        # Try API call with new origin ZIP first (if possible)

        # If no improvement, try with the new destination ZIP (if possible)

        # If still no improvement, try with both new origin and destination ZIPs (if possible)

    # If alternating the ZIP code did not help, use GoogleMaps API to determine mileage
    if route.mileage < 1.0:
        route.mileage = route.get_mileage_from_google()

    # Save route information
    processed_shipping_routes.append(route)

    break


######################################################################
# LOAD
######################################################################
# # Write mileages back to destination database
# pcMiler.set_destinations()

