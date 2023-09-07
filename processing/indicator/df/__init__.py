import pandas as pd

from .creator import *
from .processor import *


# create MASTER_DF to hold all indicator data
MASTER_DF = pd.DataFrame(
    columns=['Code', 'Name', 'Year', 'Indicator', 'Indicator code', 'Value']
    )

# add indicator df to master_df
def add_indicator_df_to_master_df(indicator, master_df):
    return pd.concat([master_df, indicator.df], ignore_index=True)