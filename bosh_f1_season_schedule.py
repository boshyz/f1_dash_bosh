"""A module to retrieve the season schedule results from the jolpica API race end point and store 
in specified table of database
 uses this doc as reference: https://github.com/jolpica/jolpica-f1/blob/main/docs/endpoints/races.md
 """

import requests
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import types, create_engine
import psycopg2
import datetime as dt
#read in google sheets reading modules
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
import openpyxl
from utils import db_update_check, convert_df_types, create_date_field, update_table


def get_season_schedule_url(year:int= None):
    """
    for a given 4 digit year, constructs the season api end point as a string needed to extract the season schedule.

    Args:
      year:int:  (Default value = None)

    Returns:
      string of end season api end point

    """
    current_year = dt.date.today().year
    # return the current year if empty else use the input
    Year = [current_year if year == None else year][0]
    season_url = f'https://api.jolpi.ca/ergast/f1/{Year}.json'
    return season_url


def get_season_schedule(year:int= None):
    """
    gets the season schedule with the race name, for season, round and date from the api and stores it as a df

    Args:
      year:int:  (Default value = None)

    Returns:
       season schedule api end results stored a pandas dataframe

    """
    #get season url
    season_url = get_season_schedule_url(year)
    #requests pull
    sch = requests.get(season_url).json()
    #filter for race schedule data
    data = pd.json_normalize(sch['MRData']['RaceTable']['Races'])
    if data.empty == True:
        print(f"Season schedule for year {year} not yet avaliable please try again later")
        return data
    else:
        #replace column names to be easier to use in python replace "." to "_"
        data.columns = data.columns.str.replace('.','_')
        data.rename(str.lower, axis = 1, inplace=True)
        #convert columns to correct data type
        data = convert_df_types(data,['season','round'],int)
        #make all date fields datetime
        date_cols = data.filter(like = 'date', axis = 1).columns
        sch = create_date_field(data, date_cols)
        return sch

def get_season_sch_db(engine, schema:str, table:str, year=None)->pd.DataFrame:
    """uses given year to retrieve the season schedule for that year from the database
    to create race urls to extract race results data from the api endpoint.

    Args:
      engine:
        sqlalchemy engine for database updates and retrivals 
      schema (str):
        name of schema in database 
      table (str):
        name of table in database 
      year:  (Default value = None)
        default = None gets latest year, else enter 4 digit int to represent season 

    Returns:
      pandas dataframe of season schedule retrieved from database based on table and schema for the given year

    """

    date_check = dt.datetime.today() -dt.timedelta(days=1)
    Year= date_check.year

    if year == None :
        query = f'select * from {schema}.{table} where season = {Year}'
    else:
        query = f'select * from {schema}.{table} where season = {year}'
    sch = pd.read_sql(query, engine)
    return sch


def check_seasons( engine, schema:str, table:str):
    """Sense checks that the the latest year's season schedule
    is in database.
    if the latest season's schedule is not in database
    it pulls it from the api and updates it to the database
    check sees that the current year is equal to the latest year in the database

    Args:
      engine: 
        the database sqlalchemy engine
      schema (str):
        name of the schema in your database 
      table (str): 
        name of table in database

    Returns:
       message about if latest year's season schedule is in the database
    

    """
    

    # there is at least a 1 day delay so only return race rounds that have commenced atleast 5 days before today's date
    date_check = dt.datetime.today() -dt.timedelta(days=1)
    date_check = date_check.date()
    Year= date_check.year

    # check which schedules are in db
    years = pd.read_sql(f"SELECT DISTINCT season FROM {schema}.{table}", engine).season.unique()

    #if the year is not in db or new year has come pull the year and update the db with the year 
    if max(years) < Year:
        # pull the new schedule then update the db
        print(f'Pulling {Year} from api')
        sch = get_season_schedule(Year)
        
        #update the db with the missing season schedule
        if sch.empty != True:
            update_table(sch,engine, schema, table)
            print(f'Updating database seasons table with {Year}')
        else:
            print(f"Schedule for year {Year} not availiable in joplica API yet try again later")
    else:
        print(f"Latest season in {table} table is {max(years)}")



        
def db_seasons_update(engine,schema:str, table:str,start:int,*end):
    """checks if season entered already in seasons table in database
    if season present, prints that data is present, else updates database table with the range of season(s) entered

    Args:
      engine: 
        the database sqlalchemy engine
      schema (str):
        name of the schema in your database 
      table (str): 
        name of table in database
      start (int):
        specifies the season of interest as a 4 digit int, represents the oldest season data you want to store the database 
      end (int)   (Default value = None): 
        specifies the season of interest as a 4 digit int, represents the latest season data you want in the database, 
        default value = None will set it to current year

    Returns:
       message about what seasons are in season schedule table of database

    """
    #updated to include new refreshes incase current year's schedule changes
    date_check = dt.datetime.today() -dt.timedelta(days=1)
    date_check = date_check.date()
    Year= date_check.year

    #seasons currently in seasons table in db
    seasons_in_db = pd.read_sql(f'select distinct season from {schema}.{table}',engine).season.unique()
    #if end year given 
    if len(end) == 1:
        #create range of years from start to fin
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
        if start not in seasons_in_db or start == Year:
            df = get_season_schedule(start)
            db_update_check(df,engine,schema, table)
        else:
            print(f"season:{start} already in season table")

            
# back dates the season scheudles in excel file to populate tableau dash
def backdate_seasons_excel(engine,schema:str,season_table:str):
    """updates season schedule data to xlxs for tableau dashboard
    

    Args:
      engine: 
        sqlalchemy databasebase engine
      schema (str):
        name of database schemea
      col (str): 
        name of column you use to partition out the data from table by, should reflect the season for schedule table
      season_table(str):
        name of table that stores season

    Returns:
        xlxs file of season schedules in current repo to be used for tableau dash 
        and message about ammedments/updates if any made

    """
    excel = "season_schedule_for_tableau.xlsx"
    latest_season = pd.read_sql(f"SELECT MAX(season) latest_season FROM {schema}.{season_table}",engine)['latest_season'][0]
    
    with pd.ExcelWriter(excel) as writer:
      season = pd.read_sql(f'SELECT * FROM {schema}.{season_table} ', engine)
      sheet_name = 'seasons'
      season.to_excel(writer,sheet_name=sheet_name, index = False)
      print(f"Season successfully upated to {excel} with {len(season)} rows to include season {latest_season}")
         
