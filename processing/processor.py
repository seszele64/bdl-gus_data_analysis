# ---------------------------------- imports --------------------------------- #

import pandas as pd
import logging
import os

# local
from paths import paths
from processing import *
from .zip.manage_files import ZipFileManager

# ---------------------------------- globals --------------------------------- #

INSTRUCTIONS = [
    {
        'codes': ['1869', '3787'],
        'merge': {
            'on': ["Code", "Year", "Name"],
            'how': 'inner',
        },
        'rename': {
            'columns': {
                'Indicator code_x': 'Indicator code',
                'Indicator_x': 'Indicator',
                },
        },
        'type': {
            'Indicator code': str,
            'Indicator': str
        },
        'operations': {
            'Value': lambda x: x['Value_y'] / x['Value_x']
        },
        'name': 'Obciążenie zakupem mieszkania',
        # 'code': '1869-3787',
    }
]

# ---------------------------------- functions --------------------------------- #

def extract_file(file_path, output_path):
    zip_manager = ZipFileManager(file_path, output_path)
    zip_manager.extract_with_progress()

# settings for cleaning data

# -------------------------------- excel files ------------------------------- #

# iterate over all xlsx files in folder
def xlsx_files_in_folder(folder_path):
    files = os.listdir(folder_path)
    files = [file for file in files if file.endswith('.xlsx')]
    return [os.path.join(folder_path, file) for file in files]

# create files = list of excel files in folder, using xlsx_files_in_folder and ExcelFile
def create_files(folder_path):
    files = xlsx_files_in_folder(folder_path)
    return [ExcelFile(file) for file in files]


def excel_file_data_to_indicator(processor: ExcelFileProcessor,
                                     file: ExcelFile) -> Indicator:
    # load file
    processor.load_file(file)

    # process file
    indicator = processor.get_indicator()
    indicator.df = process_indicator_df(indicator)

    return indicator


# create, process and delete files
def excel_files_to_indicators(output_path) -> list:

    # create files = list of excel files in folder, using xlsx_files_in_folder
    excel_files = create_files(output_path)

    # for every excel file add indicator to master_df
    processor = ExcelFileProcessor()

    indicator_list = []

    for file in excel_files:
        # create indicator from file
        indicator = excel_file_data_to_indicator(processor, file)

        # add to list
        indicator_list.append(indicator)

        # remove file
        os.remove(file.full_path)

    return indicator_list


# --------------------------------- Indicator -------------------------------- #
def process_indicators(indicators, indicator_codes, buffer, instruction, master_df):
    """
    Process a list of indicators and update the master dataframe.

    Args:
        indicators (list): List of Indicator objects.
        indicator_codes (list): List of indicator codes to filter.
        buffer (list): List to store indicators temporarily.
        instruction (str): Instruction for creating new indicator.
        master_df (pd.DataFrame): Master dataframe to update.

    Returns:
        tuple: Tuple containing the updated buffer and master dataframe.
    """
    for indicator in indicators:
        if indicator.code in indicator_codes:
            buffer.append(indicator)

            if len(buffer) == len(indicator_codes):
                new_indicator = create_new_indicator(buffer, instruction)
                master_df = add_indicator_to_master_df(new_indicator, master_df)
                buffer = []
                # logger.info("new indicator created: %s", new_indicator)
        else:
            master_df = add_indicator_to_master_df(indicator, master_df)

    return buffer, master_df



def add_indicator_to_master_df(indicator, master_df):
    """
    Add indicator to master dataframe.

    Args:
        indicator (Indicator): Indicator object to add.
        master_df (pd.DataFrame): Master dataframe to update.

    Returns:
        pd.DataFrame: Updated master dataframe.
    """
    return master_df.append(indicator.df, ignore_index=True)

def add_indicator_to_master_df(indicator, master_df):
    # Add indicator to master dataframe
    return pd.concat([master_df, indicator.df], ignore_index=True)

def save_master_df_to_csv(master_df, output_path):
    # Save master dataframe to CSV file
    master_df.to_csv(f"{output_path}/master_df.csv", index=False)
    logging.info("master_df saved to csv")

def indicators_to_master_df(indicators: list, indicator_codes: list, instruction: dict, master_df = MASTER_DF, buffer=None):
    """
    Process a set of indicators, either creating a new indicator or adding it to the master dataframe based on the
    indicator codes provided.

    Args:
        indicators: A list of Indicator objects to be processed.
        indicator_codes: A list of indicator codes to be processed.
        instruction: A dictionary mapping indicator codes to their corresponding instructions.
        master_df: The master dataframe to which new indicators will be added. Defaults to None.
        buffer: A buffer to store indicators until all indicator codes are present. Defaults to None.

    Returns:
        pd.DataFrame: The resulting master dataframe with all the processed indicators.
    """
    # if master_df is None:
    #     master_df = pd.DataFrame()

    if buffer is None:
        buffer = []

    buffer, master_df = process_indicators(indicators, indicator_codes, buffer, instruction, master_df)

    return master_df


# 

# if master_df.csv exists, ask user if he wants to overwrite it or read it
def read_or_overwrite_file(
        file_path: str,
):
    
    if not os.path.exists(file_path):
        return False
    while True:
        user_input = input(f"File {file_path} already exists. Do you want to overwrite it? [Y/N]: ")
        if user_input.lower() == 'y':
            return True
        elif user_input.lower() == 'n':
            return False
        else:
            print("Invalid input. Please try again.")


# ---------------------------------- main --------------------------------- #

def main(zip_file_path):

    # check if master_df.csv exists -> if yes, ask user if he wants to overwrite it or read it
    if read_or_overwrite_file(f"{paths.output_data_path}/master_df.csv"):
        # if yes, delete it
        os.remove(f"{paths.output_data_path}/master_df.csv")

        # extract files from zip
        extract_file(zip_file_path, paths.output_data_path)

        indicator_list = excel_files_to_indicators(paths.output_data_path)

        for instruction in INSTRUCTIONS:
            # add custom indicator
            master_df = indicators_to_master_df(indicator_list, instruction['codes'], instruction, MASTER_DF, BUFFER)

        # save as csv
        save_master_df_to_csv(master_df, paths.output_data_path)

    else:
        # if no, read master_df.csv
        master_df = pd.read_csv(f"{paths.output_data_path}/master_df.csv")
    
    return master_df