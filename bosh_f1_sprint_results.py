#bosh_f1_season_race_results gets the season race results from jolpica

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
    """takes a sprint end point url and returns the results in a df"""
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
        return df

def db_sprint_update(engine, schema:str, table:str,year=None):
    """updates sprint table in db for the season,
    if year is not set it will check see if the last sprint occured within the last 30 days if so it will delete it 
    extract from api and replace the value in the db with the ones in the df
    else year is set it will check what values are missing and update the missing rounds
    if there is nothing missing it will print message all is up to date"""

        
    #if erorr is through catch the time 
    time_to_try_again_datetime = dt.datetime.now() +dt.timedelta(hours=1)
    hour, mins = time_to_try_again_datetime.time().hour, time_to_try_again_datetime.time().minute

    try:
        #get_race_url returns the right urls that need to be retrieved.
        sprint_urls, rounds = get_race_url(engine, schema, table, year)
        if len(sprint_urls) == 0:
            print(f"Table {table} all up to date for season {year}")
        else:
            for i in sprint_urls:
                df = get_sprint_results(i)
                #check table actually exists first
                db_update_check(df,engine,schema, table)
    except KeyError:
        print(f'Jolpica Api has reached it maximum rate limit of 500 runs per hour please try again after {hour}:{mins}')