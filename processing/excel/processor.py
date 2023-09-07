import pandas as pd
from dataclasses import dataclass
from typing import List
import re, os, datetime

# indicators module in the same folder
from ..indicator import Indicator
from .metadata import Metadata

# ExcelFile class
class ExcelFile:

    # constants
    NA_VALUES = '-'

    # init
    def __init__(self, path):
        self.full_path = path
        # self.columns_to_keep = columns_to_keep
        self.metadata = Metadata()
        self.indicator = Indicator()

    # --------------------------------- metadata --------------------------------- #

    def set_metadata(self, metadata: Metadata):
        self.metadata = metadata
    
    def get_metadata(self) -> Metadata:
        return self.metadata

    # ------------------------------------- < ------------------------------------ #

    # -------------------------------- indicators -------------------------------- #

    def set_indicator(self, indicator: Indicator):
        self.indicator = indicator
    
    def get_indicator(self) -> Indicator:
        return self.indicator
    
    # ------------------------------------- < ------------------------------------ #


    # name
    # get file name
    def get_file_name(self):
        return os.path.basename(self.full_path).split('.')[0]


class ExcelFileProcessor:

    # NA_VALUES = '-'
    
    def __init__(self, excel_file: ExcelFile = None):

        self.excel_file = excel_file
        if self.excel_file:
            self.load_file(self.excel_file)


    # --------------------------------- load file -------------------------------- #

    # load file
    def load_file(self, file):
        
        if file:
            self.excel_file = file

            # init process to load metadata and indicator
            self.process_data()
        else:
            raise ValueError("ExcelFile object is not passed.")
        
        return


    def process_data(self):

        # set metadata
        self.excel_file.set_metadata(
            self.get_metadata()
        )

        # set indicator
        self.excel_file.set_indicator(
            self.get_indicator()
        )

    # --------------------------------- metadata --------------------------------- #

    # get file metadata

    def get_metadata(self):

        # get metadata from xlsx file

        file_name = os.path.basename(self.excel_file.full_path)

        # return Metadata(field, indicator_code, alternative_indicator_name)
        pattern = r'^(\w{4})_(\d{4})_.*?_(\d{14})'
        match = re.match(pattern, file_name)

        # convert timestamp to datetime object
        timestamp = datetime.datetime.strptime(match[3], '%Y%m%d%H%M%S')

        if match:
            # Check for optional indicator name
            indicator_name_match = re.search(r'^(?:[^_]*_){4}(.+?)(?:\.xlsx)?$', file_name)
            return Metadata(
                category=match[1],
                indicator_code=match[2],
                timestamp=timestamp,
                alternative_indicator_name=indicator_name_match[1] if indicator_name_match else None,
            )

        return None

    # ------------------------------------- < ------------------------------------ #


    # -------------------------------- indicators -------------------------------- #

    def get_indicator_values(self) -> pd.DataFrame:
        """Returns the second sheet as a DataFrame.

        Returns:
            pd.DataFrame: The second sheet of the Excel file. Still needing to be processed.
        """

        return pd.read_excel(self.excel_file.full_path, sheet_name=1, na_values=self.excel_file.NA_VALUES)
        
    def get_indicator_name(self):
        return pd.read_excel(self.excel_file.full_path, sheet_name=0, na_values=self.excel_file.NA_VALUES, header=None).iloc[4, 1]
    
    def get_indicator_years(self):
        return pd.read_excel(self.excel_file.full_path, sheet_name=1, na_values=self.excel_file.NA_VALUES)['Rok'].unique().tolist()
    
    def get_indicator(self):
        
        indicator = self.excel_file.indicator
        
        # Read the indicator information from the Excel file
        indicator.name = self.get_indicator_name()

        # indicator_code = self.excel_file.metadata.indicator_code
        indicator.years = self.get_indicator_years()

        # code
        indicator.code = self.excel_file.metadata.indicator_code
        indicator.df = self.get_indicator_values()
        
        return Indicator(
            name=indicator.name,
            code=indicator.code,
            years=indicator.years,
            df=indicator.df,
        )
    
    # ------------------------------------- < ------------------------------------ #