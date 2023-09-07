import logging
import pandas as pd

BUFFER = []

def merge_dataframes(dfs, merge_kwargs):
    """
    Merge multiple DataFrames using provided keyword arguments.

    Args:
        dfs (list): A list of DataFrames to be merged.
        merge_kwargs (dict): A dictionary of keyword arguments for the merge operation.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    # Ensure there are at least two DataFrames to merge
    if len(dfs) < 2:
        raise ValueError("At least two DataFrames are required for merging.")
    if len(dfs) == 2:
        return pd.merge(dfs[0], dfs[1], **merge_kwargs)
    else:
        return pd.merge(dfs[0], merge_dataframes(dfs[1:], merge_kwargs), **merge_kwargs)
    

def run_operation(df, output_column, function):
    """
    Run an operation on a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to run the operation on.
        operation (dict): A dictionary of instructions for the operation.
        # example operation:
        'operations': {
            'Value': lambda x: x['Value_y'] / x['Value_x']
        }

    Returns:
        pd.DataFrame: The resulting DataFrame.
    """
    
    # run operation -> key is output column, value is function
    df[output_column] = df.apply(
        function,
        axis=1
    )

    return df

def merge_indicator_dfs(dfs, instructions):
    """
    Create a new indicator based on two DataFrames and instructions.

    Args:
        dfs (list): A list of DataFrames to be merged and used for creating the new indicator.
        instructions (dict): A dictionary of instructions for creating the new indicator.

    Returns:
        pd.DataFrame: The resulting DataFrame with the new indicator.
    """

    df = merge_dataframes(
        dfs=dfs,
        merge_kwargs=instructions.get("merge")
    )
    # use walrus operator to check if operations exist
    if (operations := instructions.get("operations")) is not None:
        # key: output column, value: function
        for key, value in operations.items():
            df = run_operation(df, key, value)

    # rename columns
    if instructions.get("rename") is not None:
        
        if instructions.get("rename").get("columns") is not None:
            df = df.rename(columns=instructions.get("rename").get("columns"))
            logging.info([f"column {column} renamed to {new_column}" for column, new_column in instructions.get("rename").get("columns").items()])
        # rename index
        if instructions.get("rename").get("index") is not None:
            df = df.rename(index=instructions.get("rename").get("index"))
            logging.info(f"index {instructions.get('rename').get('index')} renamed")

    # ensure column type
    if instructions.get("type") is not None:
        for column, column_type in instructions.get("type").items():
            df[column] = df[column].astype(column_type)
            logging.info(f"column {column} changed to type {column_type}")
    
    return df