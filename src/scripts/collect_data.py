import urllib.request
import json
import numpy as np
import pandas as pd

def obtain_raw_data(pageNum):
    base_url = "https://api.nal.usda.gov/fdc/v1/foods/list"
    api_key = "NhfPgJChrCiU2JaNEcZRTNkdkMw6RDKLhBsRarou"
    #query = "tomato"
    params = {
        "api_key": api_key,
        #"query": query,
        "dataType": "SR Legacy", #Branded, Foundation, Survey (FNDDS), SR Legacy
        "pageSize": 200,
        "pageNumber": pageNum
    }
    url = base_url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        content_type = response.getheader("Content-Type", "")
        if "application/json" not in content_type.lower():
            print(f"Error: Expected JSON, got {content_type}")
            
        raw_data = response.read().decode("utf-8")
        data = json.loads(raw_data)
        #data.get("foods", [])  # List of food objects
        df = pd.json_normalize(data)
        df.to_parquet(rf'C:\Users\marcu\Documents\machine_learning_projects\recipe_recommender\data\raw_data\dataframe_SR_Legacy.parquet', compression='brotli')

    return data

def save_raw_json(page_num,json_data):
    df = pd.json_normalize(json_data)
    df.to_parquet(rf'C:\Users\marcu\Documents\machine_learning_projects\recipe_recommender\data\raw_data\dataframe_{page_num}.parquet', compression='brotli')

for pageNum in range(1,2):
    save_raw_json(pageNum,obtain_raw_data(pageNum))


