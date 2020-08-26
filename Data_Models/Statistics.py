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

    @property
    def records_mileage_from_zip_code_alternation_and_valid(self) -> int:
        return sum([r.has_valid_mileage_from_zip_code_alternation for r in self.records])

    @property
    def zip_code_alternation_avg_loop_count(self) -> float:
        loops = [r.zip_code_alternation_loops for r in self.records if r.mileage_from_zip_code_alternation]
        avg_loop_count = sum(loops) / len(loops) if loops else 0
        return avg_loop_count

    def abs_and_perc_based_on_source(self, value: int) -> (int, float):
        return value, (float(value) / float(self.records_in_source_table)) * 100

    @property
    def overview(self) -> pd.DataFrame:
        data_ = [(self.abs_and_perc_based_on_source(self.records_in_source_table)),
                 (self.abs_and_perc_based_on_source(self.records_matched_with_pcmiler_database)),
                 (self.abs_and_perc_based_on_source(self.records_called_pcmiler_api)),
                 (self.abs_and_perc_based_on_source(self.records_called_pcniler_api_and_valid)),
                 (self.abs_and_perc_based_on_source(self.records_mileage_from_zip_code_alternation_and_valid)),
                 (self.zip_code_alternation_avg_loop_count, 'N/A'),
                 (self.abs_and_perc_based_on_source(self.records_valid)),
                 (self.abs_and_perc_based_on_source(self.records_invalid))]
        columns_ = ['Abs', '%']
        index_ = ['Records in source table',
                  'Records matched with PCMiler database',
                  'Records called PCMiler API',
                  'Records called PCMiler API and valid mileage',
                  'Records applied ZIP code alternation and valid',
                  'Average ZIP code alternation loop length',
                  'Valid mileages',
                  'Invalid mileages']

        return pd.DataFrame(data_, columns=columns_, index=index_)