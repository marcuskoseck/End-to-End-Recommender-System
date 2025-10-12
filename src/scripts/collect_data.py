import urllib.request
import json
import numpy as np
import pandas as pd

def obtain_raw_data():
    base_url = "https://api.nal.usda.gov/fdc/v1/foods/list"
    api_key = "NhfPgJChrCiU2JaNEcZRTNkdkMw6RDKLhBsRarou"
    #query = "tomato"
    params = {
        "api_key": api_key,
        #"query": query,
        "dataType": "Foundation",
        "pageSize": 200,
        "pageNumber": 1
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
    return data

def save_raw_json(json_data):
    df = pd.json_normalize(json_data)
    df.to_parquet(r'C:\marcus_projects\End-to-End-Recommender-System\data\raw_data\dataframe.parquet', compression='brotli')

save_raw_json(obtain_raw_data())


