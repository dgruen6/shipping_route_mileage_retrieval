from typing import Optional, List

import pandas as pd

from Data_Models.Address import Address


class Route:
    def __init__(self, origin: Address, destination: Address, mileage_type: str):
        self.origin: Address = origin
        self.destination: Address = destination
        self.mileage: Optional[float] = None
        self.mileage_type: str = mileage_type
        self.mileage_from_pcmiler_db: bool = False
        self.mileage_from_pcmiler_api: bool = False
        self.zip_code_alternation_loops: int = 0

    def __repr__(self):
        return f"{self.origin}, {self.destination}"

    @property
    def has_valid_mileage(self) -> bool:
        return self.mileage and self.mileage > 1.0

    @property
    def mileage_from_zip_code_alternation(self) -> bool:
        return self.zip_code_alternation_loops > 0

    @property
    def has_valid_mileage_from_zip_code_alternation(self) -> bool:
        return self.has_valid_mileage and self.mileage_from_zip_code_alternation

    def get_alternative_zip_code(self, for_: str, zip_code_df: pd.DataFrame) -> Optional[List[str]]:
        """
        Get alternative zip code based on given zip code dataframe

        :param for_: 'origin' or 'destination'
        :param zip_code_df: Pandas dataframe which will be used to find an alternative ZIP code
        :return: Alternative zip code if possible, otherwise None
        """
        if for_ == 'origin':
            address = self.origin
        elif for_ == 'destination':
            address = self.destination
        else:
            raise ValueError(f"Given value >{for_}< is invalid. Valid options are ['origin', 'destination']")

        country_dict = {'USA': 'US',
                        'CAN': 'CA'}
        country = country_dict.get(address.country.upper())

        result_df = zip_code_df.query('City == @address.city.upper() &'
                                      'State == @address.state.upper() &'
                                      'Country == @country &'
                                      'Zipcode != @address.zip_code.upper()')

        if not result_df.empty:
            return list(result_df.Zipcode)

    def get_key(self) -> str:
        """
        Returns route key used to identify a route in destination database

        :return: Route key
        """
        key = f"{self.origin.city.upper()}_{self.origin.state.upper()}_{self.origin.zip_code.upper()}" \
              f"_{self.destination.city.upper()}_{self.destination.state.upper()}_{self.destination.zip_code.upper()}"
        return key

    def get_pcmiler_input(self) -> List:
        """
        Get the input list for the PCMiler API call (city, state, zip, country) for origin and destination

        :return: List [origin(city, state, zip, country), destination(city, state, zip, country)]
        """
        return [self.origin.city, self.origin.state, self.origin.zip_code, self.origin.country,
                self.destination.city, self.destination.state, self.destination.zip_code, self.destination.country]

    def find_mileage_in(self, df: pd.DataFrame) -> Optional[float]:
        """
        Find route in given dataframe (extracted from database)

        :param df: Pandas dataframe
        :return: Float type mileage if route found in given dataframe, None otherwise
        """
        result_df = df.query('Origin_City == @self.origin.city.upper() &'
                             'Origin_State == @self.origin.state.upper() &'
                             'Origin_Postal == @self.origin.zip_code.upper() &'
                             'Origin_Country == @self.origin.country.upper() &'
                             'Destination_City == @self.destination.city.upper() &'
                             'Destination_State == @self.destination.state.upper() &'
                             'Destination_Postal == @self.destination.zip_code.upper() &'
                             'Destination_Country == @self.destination.country.upper()')

        if not result_df.empty:
            self.mileage_from_pcmiler_db = True
            return float(result_df.iloc[0].Mileage)
