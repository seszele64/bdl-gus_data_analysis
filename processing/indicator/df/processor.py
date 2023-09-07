import logging
import pandas as pd


BASIC_DATAFRAME_PROCESSING_CONFIG = {
    'rename': {
        'Kod': 'Code',
        'Nazwa': 'Name',
        'Rok': 'Year',
        'Wartosc': 'Value',
    },
    'change_type': {
        'Code': str,
        'Name': str,
        'Year': int,
        'Value': float,
    },
}

NEEDED_COLUMNS = [
    'Code',
    'Name',
    'Year',
    'Indicator',
    'Indicator code',
    'Value',
]

# --------------------------------- functions -------------------------------- #

# process indicator df
def general_processing(indicator, indicator_col, indicator_code_col, **kwargs) -> pd.DataFrame:
    """
    Process the indicator data.

    Args:
        indicator (Indicator): The indicator object.
        indicator_col (str): The name of the column for the indicator.
        indicator_code_col (str): The name of the column for the indicator code.
        **kwargs: Additional keyword arguments.

    Returns:
        pandas.DataFrame: The processed indicator data.
    """
    if 'rename' in kwargs:
        indicator.df = indicator.df.rename(columns=kwargs['rename'])
    if 'change_type' in kwargs:
        indicator.df = indicator.df.astype(kwargs['change_type'])
    if 'subset' in kwargs:
        indicator.df = indicator.df[kwargs['subset']]
    
    # indicator info
    indicator.df = indicator.df.assign(**{
        indicator_col: indicator.name,
        indicator_code_col: indicator.code,
    })
    
    return indicator.df

class IndicatorProcessor:
    def process_2167(self, df):
        """
        Process indicator 2167 dataframe.
        """
        # convert value '12 i mniej' in column 'Wiek matki' to 12, convert value '50 i więcej' in column 'Wiek matki' to 50
        df["Wiek matki"] = df["Wiek matki"].replace("12 i mniej", 12)
        df["Wiek matki"] = df["Wiek matki"].replace("50 i więcej", 50)

        # convert wiek matki to int, just in case
        df["Wiek matki"] = df["Wiek matki"].astype(int)

        # drop columns Jednostka miary,Atrybut
        df = df.drop(columns=["Jednostka miary", "Atrybut"])

        # add column with sum of births in each year for every Kod
        df["Suma urodzeń"] = df.groupby(["Code", "Year"])["Value"].transform("sum")

        # divide Wartosc by Suma urodzeń as 'odsetek urodzeń w danym wieku matki'
        df["odsetek urodzeń w danym wieku matki"] = df["Value"] / df["Suma urodzeń"]

        # multiply odsetek urodzeń w danym wieku matki by wiek matki as 'Wiek ważony'
        df["Value"] = df["odsetek urodzeń w danym wieku matki"] * df["Wiek matki"]

        df["Indicator"] = "Średni wiek matki"

        # sum Wiek ważony for every Kod and Rok, group by Kod and Rok, keep only Wiek ważony
        df = df.groupby(['Code', 'Name', 'Year', 'Indicator', 'Indicator code'])['Value'].sum().reset_index()

        return df

    def process_3923(self, df):
        """
        Process indicator 3923 dataframe.
        """
        # Continue with your data processing using the optimized code
        df = (
            df
            .pivot(index=['Code', 'Name', 'Year', 'Indicator code'], columns='Małżeńskie/pozamałżeńskie', values='Value')
            .reset_index()
            .dropna()
        )
        
        # convert urodzenia żywe pozamałżeńskie to numeric with coerce
        df['urodzenia żywe pozamałżeńskie'] = pd.to_numeric(df['urodzenia żywe pozamałżeńskie'], errors='coerce')

        # Set the name of the DataFrame, reset column name, filter NA values, change data type, calculate ratio, subset columns
        df.name = 'odsetek urodzeń pozamałżeńskich'
        df.columns.name = None
        df = df.dropna()
        df['urodzenia żywe pozamałżeńskie'] = df['urodzenia żywe pozamałżeńskie'].astype(int)
        df['Value'] = df['urodzenia żywe pozamałżeńskie'] / (df['urodzenia żywe pozamałżeńskie'] + df['urodzenia żywe małżeńskie'])
        
        df['Indicator'] = 'odsetek urodzeń pozamałżeńskich'

        return df
    
    def process_4112(self, df):
        """
        Process indicator 4112 dataframe.
        """
        # optimize data processing
        df = (
            # subset df to where column 'Płeć' == 'ogółem'
            df[df['Płeć'] == 'ogółem']
            # delete Płeć column
            .drop(columns=['Płeć'])
        )

        df['Indicator'] = 'Wskaźnik zatrudnienia'

        return df


def custom_processing(indicator):
    """
    Custom processing function for indicators.
    """
    processor = IndicatorProcessor()

    if indicator.code in ['2167', '3923', '4112']:
        indicator.df = processor.__getattribute__(f'process_{indicator.code}')(indicator.df)
    else:
        logging.error(f'No custom processing function defined for indicator {indicator.code}.')

    return indicator.df