"""A module to retrieve the sprint results from the jolpica API sprint end point
 use this as reference: https://github.com/jolpica/jolpica-f1/tree/main/docs
 """


import requests
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import types, create_engine
import psycopg2
import datetime as dt
import os
import openpyxl
from bosh_f1_season_schedule import  check_seasons,get_season_sch_db
from utils import db_update_check, get_data, get_missing_rounds, update_table, create_date_field, convert_df_types, check_db_for_season
from bosh_f1_race_results import get_race_url     
             

def get_sprint_results(sprint_url):
    """takes a sprint end point url and returns the results in a pandas df

    Args:
      sprint_url (str): 

    Returns:
       results of api endpoint as a pandas DataFrame

    """
    #if time out occurs print the url that made it happen
    print(sprint_url)
    data = requests.get(sprint_url).json()
    data = data['MRData']['RaceTable']['Races']
    #checks if sprint occured that weekend or available in jolpica if not then replaces it with empty df.
    if len(data) == 0:
        sprint_results = pd.DataFrame()
        return sprint_results
    else:
        data= data[0]
        df = pd.json_normalize(data['SprintResults'])
        #get season and round
        df.loc[:, "season"] = int(data['season'])
        df.loc[:, "round"] = int(data['round'])
        #show it's sprint data
        df.loc[:,'results_type'] = 'sprint'
        #drop not used cols
        df.drop(columns = ['positionText'], inplace= True)
        # convert all numeric cols to correct data type
        df = convert_df_types(df, ['round', 'season','number', 'position', 'points', 'grid', 'laps','Driver.permanentNumber'],'int64')
        #rename columns to be more python friendly to use panads
        df.columns = df.columns.str.replace(".","_")
        #tidy names to make it easier to query in sqlachemy
        df.rename(str.lower, axis =1, inplace=True)
        #rename driver_driverid to driverid
        df = df.rename(columns = {"driver_driverid": 'driverid'})
        #drop fastestlap_rank col
        df = df.drop(columns = ["fastestlap_rank"])
        return df

def db_sprint_update(engine, schema:str, table:str,year=None):
    """updates sprint table in the database for the season and round

    if year is not set function will check to see if the last sprint occured within the last 30 days. 
    if data was updated within last 30 days it will delete it from the database. 
    once deleted it will pull fresh from the api and replace the value in the database with the ones in the returned dataframe.
    If year is specified as 4 digit int, it will check what values are missing and update the missing rounds
    If no rounds are missing for the season and the season is not the latest season, it will print message all is up to date

    Args:
      engine: 
        the database sqlalchemy engine
      schema (str):
        name of the schema in your database 
      table (str): 
        name of table in database
      year:  (Default value = None)
        specifies the season of interest 4 digit int , default is none in which it takes the latest year

    Returns:
       message about database update 

    """

        
    #if erorr is through catch the time 
    time_to_try_again_datetime = dt.datetime.now() +dt.timedelta(hours=1)
    hour, mins = time_to_try_again_datetime.time().hour, time_to_try_again_datetime.time().minute

    #year to use in print out
    year = dt.datetime.now().year

    try:
        #get_race_url returns the right urls that need to be retrieved.
        sprint_urls, rounds = get_race_url(engine, schema, table)
        if len(sprint_urls) == 0:
            print(f"Table {table} all up to date for season {year}")
        else:
            for i in sprint_urls:
                df = get_sprint_results(i)
                #check table actually exists first
                db_update_check(df,engine,schema, table)
    except KeyError:
        print(f'Jolpica Api has reached it maximum rate limit of 500 runs per hour please try again after {hour}:{mins}')
