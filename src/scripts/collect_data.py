import urllib.request
import json
import numpy as np
import pandas as pd
from itertools import product

def obtain_raw_data(data_type,pageNum):
    base_url = "https://api.nal.usda.gov/fdc/v1/foods/list"
    api_key = "NhfPgJChrCiU2JaNEcZRTNkdkMw6RDKLhBsRarou"
    #query = "tomato"
    params = {
        "api_key": api_key,
        #"query": query,
        "dataType": data_type, #Branded, Foundation, Survey (FNDDS), SR Legacy
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
        if not df.empty:
            df.to_parquet(rf'C:\Users\marcu\Documents\machine_learning_projects\recipe_recommender\data\raw_data\dataframe_{data_type}_{pageNum}.parquet', compression='brotli')

    return data

# def save_raw_json(page_num,json_data):
#     df = pd.json_normalize(json_data)
#     df.to_parquet(rf'C:\Users\marcu\Documents\machine_learning_projects\recipe_recommender\data\raw_data\dataframe_{page_num}.parquet', compression='brotli')

page_numbers = 1000
different_food_types = data_types=["Branded"]#, "Foundation", "Survey (FNDDS)", "SR Legacy"]
page_number_iters = range(300,page_numbers)

for data_type,pageNum in product(different_food_types,page_number_iters):
    obtain_raw_data(data_type,pageNum)


