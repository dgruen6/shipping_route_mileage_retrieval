import requests
import xmltodict

from Data_Models import Route
import pandas as pd
from typing import Optional
import time
import logging
import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read('config.ini')


class PCMilerApiConnector:
    def __init__(self):
        self._url = config['pcmiler_api_credentials']['url']
        self.mileage_type = 'PCMILER32'
        self._mileage_template = (Path.cwd() / "API_Templates" / "pcmiler_api_mileage_template.txt").read_text()

    def get_mileage(self, org_city: str, org_state: str, org_zip: str, org_country: str,
                    dest_city: str, dest_state: str, dest_zip: str, dest_country: str) -> Optional[float]:
        """
        Perform the PCMiler API call

        :param org_city:
        :param org_state:
        :param org_zip:
        :param org_country:
        :param dest_city:
        :param dest_state:
        :param dest_zip:
        :param dest_country:
        :return: mileage (str)
        """
        request_body = self._mileage_template.format(org_city, org_state, org_zip, org_country,
                                                     dest_city, dest_state, dest_zip, dest_country,
                                                     self.mileage_type)

        # Try to post 5 times, then give up
        tries = 0
        sleep_time = 1.0
        mileage = None
        while tries < 5:
            try:
                request = requests.post(self._url,
                                        data=request_body,
                                        headers={"Content-Type": "text/xml; charset=utf-8"})
                response = xmltodict.parse(request.text)
                mileage = float(response["soap:Envelope"]["soap:Body"]["GetMileageResponse"]["GetMileageResult"]["miles"])
            except:
                logging.warning(f"Error during PCMiler API call. {tries+1}/5 retry attempt in {sleep_time}s")
                tries += 1
                time.sleep(sleep_time)
            else:
                tries = 5

        return mileage

    def get_mileage_with_alternative_zip_code(self, alternate: str, route: Route, zip_codes_df: pd.DataFrame) -> None:
        """
        Get mileage from PCMiler API with alternating the initial ZIP code

        :param alternate: 'origin' or 'destination'
        :param route: Route object containing origin and destination info
        :param zip_codes_df: Dataframe containing all US ZIP codes
        :return: None
        """
        if alternate == 'origin':
            input_pos = 2
            address = route.origin
        elif alternate == 'destination':
            input_pos = 6
            address = route.destination
        else:
            raise ValueError

        new_zips = route.get_alternative_zip_code(for_=alternate, zip_code_df=zip_codes_df)
        pcmiler_input = route.get_pcmiler_input()

        if new_zips:
            route.mileage_from_pcmiler_api = True
            for new_zip in new_zips:
                route.zip_code_alternation_loops += 1
                pcmiler_input[input_pos] = new_zip
                route.mileage = self.get_mileage(*pcmiler_input)
                if route.has_valid_mileage:
                    address.zip_code_modified = new_zip
                    break
