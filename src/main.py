import os
import yaml

from data_preprocessing import run_data_preprocessing
from feature_engineering import run_feature_engineering
from model import recommend

def open_yml(file_path: str) -> dict:
    '''
    opens a yml file

    args:
        file_path (str): file path to the yml file

    return:
        settings (dict): a dictionary containing settings for 
                        the pipeline
    '''
    with open(file_path) as file:
        settings = yaml.safe_load(file)
    return settings

def run_test_pipeline(settings: dict) -> None:
    '''
    A testing function for the pipeline with a simple model and
    a csv file for the data. The path to this data can be configured
    in the config.yml

    args:
        settings (dict): a dictionary containing settings for the entire
                        pipeline

    return:
        None
    '''
    #data preprocessing
    settings["test_data"] = run_data_preprocessing(settings["test_data"])
    #feature engineering
    settings = run_feature_engineering(settings)
    #model building
    
    title = settings["test_data"]["df"]["title"].sample()
    print(f"\n\nRandomly selected title: {title}")
    recommendations = recommend(
        title,
        settings["feature_engineering"]["cosine_sim"],
        settings["test_data"]["df"]
        )
    
    print(recommendations)
    print("\n\n")


def run_recipe_recommender(settings: dict) -> None:
    '''
    Runs main pipeline

    args:
        settings (dict): a dictionary containing settings for the entire
                        pipeline

    return:
        None
    '''
    #data preprocessing
    settings["test_data"] = run_data_preprocessing(settings["data"])
    #feature engineering
    settings = run_feature_engineering(settings)
    #model building
    
    pass

def main(yml_file_path: str) -> None:
    '''
    main function for the pipeline

    args:
        yml_file_path (str): path to the config yaml file.

    return:
        None
    '''
    settings = open_yml(yml_file_path)
    if settings["test_data"]["enable_test_pipeline"]:
        run_test_pipeline(settings)
    else:
        run_recipe_recommender(settings)

    

if __name__=="__main__":
    yml_file_path = r"src\config.yml"
    main(yml_file_path)