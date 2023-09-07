# manage indicators

# imports
from dataclasses import dataclass
import pandas as pd

# import __init__ from df folder below
from .df import *

# create Indicator dataclass
@dataclass
class Indicator:
    
    # indicator name and code

    # 'LUDN' -> indicator name
    name: str = None
    
    # '1869' -> indicator code
    code: str = None

    # list of years; 2nd sheet
    years: list = None

    # values -> df, 2nd sheet
    df: pd.DataFrame = None

    # str
    def __str__(self) -> str:
        return f"""
        # Indicator object
        Name: {self.name}
        Code: {self.code}
        Years: {self.get_indicator_year_ranges()}
        DataFrame head: {self.df.head()}
        """
    
    # make a string of years list

    def get_indicator_year_ranges(self):
        """
        Returns a string of year ranges from a list of years.
        """

        years_list = self.years

        def format_year_ranges(years_list):
            def format_year_range(start_year, end_year):
                if start_year == end_year:
                    return str(start_year)
                else:
                    return f"{start_year}-{end_year}"

            years_list.sort()
            formatted_ranges = []
            start_year = end_year = years_list[0]

            for year in years_list[1:]:
                if year == end_year + 1:
                    end_year = year
                else:
                    formatted_ranges.append(format_year_range(start_year, end_year))
                    start_year = end_year = year

            formatted_ranges.append(format_year_range(start_year, end_year))

            formatted_ranges_string = ", ".join(formatted_ranges)

            return formatted_ranges_string
        
        return format_year_ranges(years_list)
    

# create indicators from dfs and instructions
def create_new_indicator(indicators, instructions):
    
    # merge dfs
    merged_df = df.creator.merge_indicator_dfs([indicator.df for indicator in indicators], instructions)

    # store instructions.get('name') and instructions.get('code') in variables
    name = instructions.get('name', '-'.join([indicator.name for indicator in indicators]))
    code = instructions.get('code', '-'.join([indicator.code for indicator in indicators]))

    # create new indicator
    indicator = Indicator(
        name=name,
        code=code,
        years=list(merged_df['Year'].unique()),
        df=merged_df,
    )

    indicator.df = general_processing(indicator, 'Indicator', 'Indicator code', **{
        'subset': NEEDED_COLUMNS
    })

    return indicator


# manage dfs
def process_indicator_df(indicator):
    # process indicator df
    indicator.df = general_processing(indicator, 'Indicator', 'Indicator code', **BASIC_DATAFRAME_PROCESSING_CONFIG)
    # custom process for specific indicator
    indicator.df = custom_processing(indicator)
    # subset df
    indicator.df = indicator.df[NEEDED_COLUMNS]
    return indicator.df