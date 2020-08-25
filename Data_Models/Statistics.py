from typing import List

import pandas as pd

from Data_Models.Route import Route


class Statistics:
    def __init__(self, records: List[Route]):
        self.records: List[Route] = records

    @property
    def records_in_source_table(self) -> int:
        return len(self.records)

    @property
    def records_matched_with_pcmiler_database(self) -> int:
        return sum([r.mileage_from_pcmiler_db for r in self.records])

    @property
    def records_called_pcmiler_api(self) -> int:
        return sum([r.mileage_from_pcmiler_api for r in self.records])

    @property
    def records_called_pcniler_api_and_valid(self) -> int:
        return sum([r.mileage_from_pcmiler_api and r.has_valid_mileage for r in self.records])

    @property
    def records_valid(self) -> int:
        return sum([r.has_valid_mileage for r in self.records])

    @property
    def records_invalid(self) -> int:
        return sum([not r.has_valid_mileage for r in self.records])

    def abs_and_perc_based_on_source(self, value: int) -> (int, float):
        return value, (float(value) / float(self.records_in_source_table)) * 100

    @property
    def data_frame(self) -> pd.DataFrame:
        data_ = [(self.abs_and_perc_based_on_source(self.records_in_source_table)),
                    (self.abs_and_perc_based_on_source(self.records_matched_with_pcmiler_database)),
                    (self.abs_and_perc_based_on_source(self.records_called_pcmiler_api)),
                    (self.abs_and_perc_based_on_source(self.records_called_pcniler_api_and_valid)),
                    (self.abs_and_perc_based_on_source(self.records_valid)),
                    (self.abs_and_perc_based_on_source(self.records_invalid))]
        columns_ = ['Abs', '%']
        index_ = ['Records in source table',
                  'Records matched with PCMiler database',
                  'Records called PCMiler API',
                  'Records called PCMiler API and valid mileage',
                  'Valid mileages',
                  'Invalid mileages']

        return pd.DataFrame(data_, columns=columns_, index=index_)