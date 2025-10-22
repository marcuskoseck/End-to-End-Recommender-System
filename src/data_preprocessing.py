import numpy as np
import pandas as pd
import re
from scripts.store_data import DataOrganizer

def read_file(file_path):
    '''
    loads in a file with the raw data

    args:
        file_path (str): the path to the file
    
    return:
        df (pd.DataFrame): a dataframe of all of the data.
    '''
    if file_path[-3:].lower() == "csv":
        df = pd.read_csv(file_path)
        return df

    else:
        breakpoint()
        message = '''
        The file type in the config.yml isn't found. Please
        use one of the following file types:

        .CSV
        '''
        print(message)
        exit()

def run_data_preprocessing(settings):
    '''
    run the data preprocessing which includes organizing the data
    and shaping it to be ready for feature engineering.
    '''
    df = read_file(settings["file_path"])
    settings["df"] = df
    return settings
