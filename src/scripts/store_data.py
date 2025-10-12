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

    def make_tables(self):
        
        cur = self.con.cursor()
        cur.execute("CREATE TABLE foods(fdcId, Description, datatype, publicationDate, ndbNumber")

    def fill_tables(self):
        
        raw_data_dir = r"C:\marcus_projects\End-to-End-Recommender-System\data\raw_data"
        data_files = os.listdir(raw_data_dir)
        for data_path in data_files:
            df = self.open_parquet(os.path.join(raw_data_dir,data_path))
            breakpoint()
            

