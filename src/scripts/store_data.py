import pandas as pd
import numpy as np
import sqlite3
import os

class DataOrganizer:
    # TODO: connect database with file path.
    def __init__(self,save_directory_path):
        self.save_directory_path = r"data\usda"
        self.make_db_path()
        self.con = self.open_database(save_directory_path)
        
    def make_db_path(self):
        '''
        Makes the save path for the database in the datafolder

        args:
            None
        
        return:
            None
        '''
        os.makedirs(self.save_directory_path,exist_ok=True)

    def open_parquet(self,file_path):
        '''
        reads parquet files

        args:
            file_path: file path to the parquet file

        returns:
            pd.DataFrame: A DataFrame containing raw data
        '''
        
        return pd.read_parquet(file_path)

    def organize_nutrients(self,list_of_nutrients):
        pass

    def close_database(self):
        '''
        helper function to close the database

        args:
            None

        Return:
            None
        '''
        self.con.close()
    
    def open_database(self,file_path):
        '''
        opens a database given a filepath

        args:
            file_path (str): a file path to a database with extension db

        return:
             connection: a connection object for interacting with the database.
        '''
        return sqlite3.connect(
            os.path.join(self.save_directory_path,
                        file_path))

    def branded_foods(self,df):
        '''
        a function for appending tables with more data

        agrs:
            df (pd.DataFrame): a chunk of dataframe for the next append

        return:
            None
        '''
        foods_df = df[["fdcId","description","dataType","publicationDate","gtinUpc"]]
        nutrients_df =  df[["gtinUpc","number","name","unitName","amount"]]
        brands_df = df[["gtinUpc","brandOwner"]]
        
        foods_df.to_sql(name='foods', con=self.con, if_exists='append', index=False)
        nutrients_df.to_sql(name='nutrients', con=self.con, if_exists='append', index=False)
        brands_df.to_sql(name='brands', con=self.con, if_exists='append', index=False)


    def foundational_foods(self,df):
        '''
        a function for appending tables with more data

        agrs:
            df (pd.DataFrame): a chunk of dataframe for the next append

        return:
            None
        '''
        df.rename(columns={"ndbNumber":"gtinUpc"},inplace=True)
        foods_df = df[["fdcId","description","dataType","publicationDate","gtinUpc"]]
        nutrients_df =  df[["gtinUpc","number","name","unitName","amount"]]

        foods_df.to_sql(name='foods', con=self.con, if_exists='append', index=False)
        nutrients_df.to_sql(name='nutrients', con=self.con, if_exists='append', index=False)

    def survey_foods(self,df):
        '''
        a function for appending tables with more data

        agrs:
            df (pd.DataFrame): a chunk of dataframe for the next append

        return:
            None
        '''
        df.rename(columns={"foodCode":"gtinUpc"},inplace=True)
        foods_df = df[["fdcId","description","dataType","publicationDate","gtinUpc"]]
        nutrients_df =  df[["gtinUpc","number","name","unitName","amount"]]
        foods_df.to_sql(name='foods', con=self.con, if_exists='append', index=False)
        nutrients_df.to_sql(name='nutrients', con=self.con, if_exists='append', index=False)

    def fill_tables(self):
        '''
        reading data from parquet files and organizing them into dataframes
        to upload into sql tables

        args:
            None

        return: 
            None
        '''
        raw_data_dir = r"data\raw_data"
        data_files = os.listdir(raw_data_dir)

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
            data_type = df["dataType"].iloc[0]
            # TODO: save to sql tables.
            if data_type == "Branded":
                self.branded_foods(df)
            elif data_type == "Foundation" or data_type == "SR Legacy":
                self.foundational_foods(df)
            elif data_type == "Survey (FNDDS)":
                self.survey_foods(df)
            else:
                raise(f"{data_type} is an unknown type")
            
            # try:
            #     foods_df = df[["fdcId","description","dataType","publicationDate","ndbNumber"]]
            #     nutrients_df =  df[["number","name","unitName"]]
            #     food_nut_df = df[["fdcId","number","amount","derivationCode","derivationDescription"]]
            # except:
            #     breakpoint()
            # all_foods_dfs.append(foods_df)
            # all_nut_dfs.append(nutrients_df)
            # all_food_nut_dfs.append(food_nut_df)
            # breakpoint()
            

if __name__ == "__main__":
    save_directory_path = r"data\usda"
    a = DataOrganizer(save_directory_path)
    #a.make_tables()
    a.fill_tables()
    a.close_database()