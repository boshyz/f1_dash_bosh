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
from bosh_f1_season_schedule import  check_seasons
from utils import db_update_check,  convert_df_types ,get_missing_rounds, get_rounds_date_for_season




def get_race_url_urgast(engine, schema:str, table:str,year =None):
    """returns the race or sprint url tha needs to pulled from the api for a season
    races urls are the base for race results data
    race urls also serve as basis to retrieve qualifying and pit stop data
    uses the season schedule to make the race url so table is seasons table
    note data for the latest race does not populate in jolipica api database till day after"""

    #check that seasons is latest seasons if not update to the latest year in db for use by using 
    #race schedule api endpoint 
    check_seasons( engine, schema, 'season')
    
    #there is at least a 1 day delay so only return race rounds that have commenced atleast 1 day before today's date
    #all race urls are made by using the race season(year) schdedule stored in the database
    date_check_max = dt.datetime.today()-dt.timedelta(days=1)
    Year= date_check_max.year
    #if year is left empty, this will use today's date's year for latest and will filter for races or sprints
    #that have date or sprint greater than todays date minus last 30 days
    
    #if year is none return todays' date's year:
    if year == None:
        year = Year
        #delete rounds are in db that exceed last 30 days
        missing_rounds_db, last_30_day_rounds =  get_missing_rounds(engine,schema,table,year)
        #filter for races from this years schedule's who qualifying date occured on or after 30 days before today's date
        if table == 'race' or table == 'races':
            race_result_url = [f'https://ergast.com/api/f1/{year}/{r}/results.json' for r in last_30_day_rounds]
            return race_result_url, last_30_day_rounds
        elif table == 'sprint' :
            race_result_url = [f'https://ergast.com/api/f1/{year}/{r}/sprint.json' for r in last_30_day_rounds]
            return race_result_url, last_30_day_rounds


    # if year is entered then return the urls for the missing rounds not currently in db
    else:
        missing_rounds, last_30_day_rounds =  get_missing_rounds(engine,schema,table,year)
        #if the missing rounds is an int i.e equal to 100 or 200 that means the table does not exist or is missing all of its data
        if type(missing_rounds)!= int:
            if table == 'race' or table == 'races':
                race_result_url = [f'https://ergast.com/api/f1/{year}/{r}/results.json' for r in missing_rounds]
                return race_result_url, missing_rounds
            elif table == 'sprint' :
                race_result_url = [f'https://ergast.com/api/f1/{year}/{r}/sprint.json' for r in missing_rounds]
                return race_result_url,  missing_rounds
        #if table does not exist then return all possible rounds for that year have date or sprint_date that are less
        #than today's date minus 1 day
        else:
            sch_rounds_season = get_rounds_date_for_season(engine, schema, table, year)
            if table == 'race' or table == 'races':
                missing_rounds = sch_rounds_season.loc[sch_rounds_season.date <= date_check_max, 'round'].to_list()
                race_result_url = [f'https://ergast.com/api/f1/{year}/{r}/results.json' for r in missing_rounds]
                return race_result_url,missing_rounds
            elif table == 'sprint' :
                missing_rounds = sch_rounds_season.loc[sch_rounds_season.sprint_date <= date_check_max, 'round'].to_list()
                race_result_url = [f'https://ergast.com/api/f1/{year}/{r}/sprint.json' for r in missing_rounds]
                return race_result_url, missing_rounds
            