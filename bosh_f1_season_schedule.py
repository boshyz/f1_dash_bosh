"""bosh_f1_season_sch gets the season scehdule from jolpica"""

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
    current_year = dt.date.today().year
    # return the current year if empty else use the input
    Year = [current_year if year == None else year][0]
    season_url = f'https://api.jolpi.ca/ergast/f1/{Year}.json'
    return season_url


def get_season_schedule(year:int= None):
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
        data = convert_df_types(data,['season','round'],int)
        #make all date fields datetime
        date_cols = data.filter(like = 'date', axis = 1).columns
        sch = create_date_field(data, date_cols)
        return sch

def get_season_sch_db(engine, schema:str, table:str, year=None)->pd.DataFrame:
    """uses db for season sch to get race urls bc """

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
    is in db, if it's not the latest season's schedule is not in db
    it pulls it from the api and updates it to the database
    checks that the current year is equal to the latest year in the database"""
    

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
            print(f"Schdule for year {Year} not availiable in joplica API yet try again later")
    else:
        print(f"Latest season in {table} table is {max(years)}")
        
def db_seasons_update(engine,schema:str, table:str,start:int,*end):
    """checks if season entered already in seasons table in db if so prints that data is present
    else updates with the range"""
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
        if start not in seasons_in_db:
            df = get_season_schedule(start)
            db_update_check(df,engine,schema, table)
        else:
            print(f"season:{start} already in season table")

            
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
            print(f'The latest season is still {latest_season}. No excel update made')
        else:
            with pd.ExcelWriter(excel) as writer:
                season = pd.read_sql(f'SELECT * FROM {schema}.{season_table} ', engine)
                sheet_name = 'seasons'
                season .to_excel(writer,sheet_name=sheet_name, index = False)
                print(f"Season successfully upated to {excel} with {len(season)} rows to include season {Year}")
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