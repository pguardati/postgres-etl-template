import os
import glob
import pandas as pd


def get_files(directory):
    """Get all json files and subfiles from a directory
    Args:
        directory:

    Returns:
        list
    """
    all_files = []
    for root, dirs, files in os.walk(directory):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def get_df_from_dir(directory):
    """Get a unique dataframe from multiple files in a directory
    Args:
        directory(str):

    Returns:
        pd.DataFrame
    """
    files = get_files(directory)
    df_all = []
    for file in files:
        df_all.append(pd.read_json(file, lines=True))
    df_all = pd.concat(df_all)
    return df_all


