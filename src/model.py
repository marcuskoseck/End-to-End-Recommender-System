import numpy as np
import pandas as pd

def recommend(title: str,
             cosine_sim: np.ndarray,
               df: pd.DataFrame) -> pd.Series:
    '''
    Runs the cosine recommender system
    
    args:
        title (str): the title in the database that we are 
                    finding recommendations based on
        cosine_sim (np.ndarray): the cosine recommender system
        df (pd.DataFrame): A dataframe containing movies to compare/select

    return:
        pd.Series: A list of the top 5 recommendations based on the title
                    selected for the comparison
    '''

    idx = df["title"] == title.iloc[0]
    sim_scores = cosine_sim[idx][0]
    top_idx = np.argsort(sim_scores)[::-1]
    sim_scores = sim_scores[top_idx]
    # sort by similarity score, highest first
    # skip itself (the first entry)
    top_idx = top_idx[1:6]
    # return the recommended titles
    #print(df['title'].iloc[top_idx])
    return df['title'].iloc[top_idx]



