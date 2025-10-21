from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import re
from typing import Union
import numpy as np
import pandas as pd

def cosine_similarity(matrix1: Union[np.ndarray],
                      matrix2: Union[np.ndarray]) -> np.ndarray:
    '''
    TODO: UPDATE TYPES AS NEEDED.
    computes the cosine similarity between two vectors/matrices

    args:
        matrix1: the first matrix in the cosine similarity
        matrix2: the second matrix in the cosine similarity
    
    return:
        A matrix of the computed cosine similarity
    '''
    return linear_kernel(matrix1,matrix2)

def remove_punctuation(text: str) -> str:
    '''
    a helper function for pandas df which removes
    punctuation from specific columns. Typically used with
    the apply function

    args:
        text (str): a paragraph or some combination of characters

    return:
        A string without the standard punctuation 
    '''
    #text = df["overview"].iloc[0]
    return re.sub(r'[^\w\s]', '', text)

def vectorize_text(df: pd.DataFrame) -> np.ndarray:
    '''
    creates the text embedding and computes the cosine similarity

    args:
        df (pd.DataFrame): a dataframe that contains text
    
    return:
        cosine similarity for the text in the dataframe
    '''
    text = df["overview"].str.replace(r'[^\w\s]', '',regex=True)
    text = text[~text.isna()]
    text = text[text.str.strip() != '']
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(text.tolist())
    return cosine_similarity(tfidf_matrix,tfidf_matrix),text

def run_feature_engineering(settings):
    '''
    Runs the feature engineering pipeline

    args: 
        settings (dict): contains all of the settings for the   
                        feature engineering pipeline

    return:
        settings (dict): updated settings/data as needed from running
                        feature engineering.
    '''

    cosine_sim,df = vectorize_text(settings["test_data"]["df"])
    settings["feature_engineering"]["cosine_sim"] = cosine_sim
    settings["test_data"]["df"]["overview"].loc[df.index] = df
    settings["test_data"]["df"] = settings["test_data"]["df"].loc[df.index]
    return settings
