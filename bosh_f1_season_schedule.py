"""functions to help update in database and format help format api endpoints"""

import requests
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import types, create_engine, text
import psycopg2
import datetime as dt
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
import openpyxl
from utils import get_engine, convert_df_types, create_date_field, db_update_check



#bosh_f1_season_schedule
def get_season_schedule_url(year:int= None):
    """returns the season url for the jolpica API the given year entered"""
    current_year = dt.date.today().year
    # return the current year if empty else use the input
    Year = [current_year if year == None else year][0]
    season_url = f'https://api.jolpi.ca/ergast/f1/{Year}.json'
    return season_url


    
#bosh_f1_season_schedule
def get_season_schedule(year:int= None):
    #get season url
    season_url = get_season_schedule_url(year)
    #requests pull
    sch = requests.get(season_url).json()
    #filter for race schedule data from api and return as df
    data = pd.json_normalize(sch['MRData']['RaceTable']['Races'])
    #if schedule does not exist print error mesage and return empty df
    if data.empty == True:
        print(f"Season schedule for year {year} not yet avaliable please try again later")
        return data
    #if df is not empty clean it to fit the database
    else:
        #replace column names to be easier to use in python replace "." to "_"
        data.columns = data.columns.str.replace('.','_')
        data.rename(str.lower, axis = 1, inplace=True)
        data = convert_df_types(data,['season','round'],int)
        #make all date fields datetime
        date_cols = data.filter(like = 'date', axis = 1).columns
        sch = create_date_field(data, date_cols)
        return sch



def db_seasons_update(engine,schema:str, table:str,start:int,*end):
    """checks if season entered already in seasons table in db if so prints that data is present
    else updates with the range"""
    #this year
    this_year = dt.datetime.today().year
    #seasons currently in seasons table in db
    seasons_in_db = pd.read_sql(f'select distinct season from {schema}.{table}',engine).season.unique()

  
    #if end year given 
    if len(end) == 1:
        #create range of years from start to end
        years = sorted(np.arange(start,end[0]+1),reverse=True)
        #for each year in range, if year not currently in db then pull data from api
        for i in years:
            if i not in seasons_in_db:
                df = get_season_schedule(i)
                db_update_check(df,engine,schema, table)
            else:
                print(f"season:{i} already in season table")
    #if only start given then update database with season schedule if missing
    else:
        if start not in seasons_in_db or start == this_year:
            df = get_season_schedule(start)
            db_update_check(df,engine,schema, table)
        #ammended to be if start same as this year then update the schedule
        else:
          print(f"Season {start} already in seasons table in database")
            


#db_points_update(engine, 'f1_dash', 'race') 
def backdate_seasons_excel(engine,schema:str,col:str,season_table:str):
    """updates season schedule data to xlxs for tableau dash
    if season is missing current year """
    excel = "season_schedule_for_tableau.xlsx"
    date_check_max = dt.datetime.today()
    Year= date_check_max.year
    latest_season = pd.read_sql(f"SELECT MAX({col}) latest_season FROM {schema}.{season_table}",engine)['latest_season'][0]
    if latest_season == Year:
        print(f'The latest season avaliable is season {latest_season}') 
        #if latest season in excel is same as current year don't update        
        if pd.read_excel("season_schedule_for_tableau.xlsx", sheet_name='seasons')['season'].max() == Year:
            with pd.ExcelWriter(excel, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                season = pd.read_sql(f'SELECT * FROM {schema}.{season_table} ', engine)
                sheet_name = 'seasons'
                season .to_excel(writer,sheet_name=sheet_name, index = False)
                print(f"Latest season schedule for season {Year} successfully upated to {excel} with {len(season)} rows")
       
    else:
        #if the current date's year is not same as latest season in db pull from api to get result
        db_seasons_update(engine,schema, season_table,Year)
        #check if db now has latest season
        latest_season = pd.read_sql(f"SELECT MAX({col}) latest_season FROM {schema}.{season_table}",engine)['latest_season'][0]
        if latest_season == Year:
            with pd.ExcelWriter(excel) as writer:
                season = pd.read_sql(f'SELECT * FROM {schema}.{season_table} ', engine)
                sheet_name = 'seasons'
                season .to_excel(writer,sheet_name=sheet_name, index = False)
                print(f"Season successfully upated to {excel} with {len(season)} rows to include season {Year}")
        else:
            print(f'The latest season schedule for {Year} is not yet available. The latest season is still {latest_season}. No excel update made')
            
