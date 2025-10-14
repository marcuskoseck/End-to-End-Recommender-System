import pandas as pd
import numpy as np
import sqlite3
import os


class Data_organizer:
    def __init__(self):
        self.con = self.open_database()

    def open_parquet(self,file_path):
        return pd.read_parquet(file_path)

    def organize_nutrients(self,list_of_nutrients):
        pass

    def close_database(self):
        return self.con.close()
    
    def open_database(self):
        return sqlite3.connect("USDA_foods.db")

    def fill_tables(self):
        
        raw_data_dir = r"C:\Users\marcu\Documents\machine_learning_projects\recipe_recommender\data\raw_data"
        data_files = os.listdir(raw_data_dir)
        all_foods_dfs = []
        all_nut_dfs = []
        all_food_nut_dfs = []
        for data_path in data_files:
            df = self.open_parquet(os.path.join(raw_data_dir,data_path))
            place_holder = df.explode("foodNutrients",ignore_index=True)
            df = pd.concat(
                [
                place_holder.drop(columns=["foodNutrients"]),
                 place_holder["foodNutrients"].apply(pd.Series)
                ],
                axis=1
            )
            try:
                foods_df = df[["fdcId","description","dataType","publicationDate","ndbNumber"]]
                nutrients_df =  df[["number","name","unitName"]]
                food_nut_df = df[["fdcId","number","amount","derivationCode","derivationDescription"]]
            except:
                breakpoint()
            # all_foods_dfs.append(foods_df)
            # all_nut_dfs.append(nutrients_df)
            # all_food_nut_dfs.append(food_nut_df)
            # breakpoint()
            

a = Data_organizer()
#a.make_tables()
a.fill_tables()