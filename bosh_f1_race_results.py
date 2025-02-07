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



def get_race_url(engine, schema:str, table:str,year =None):
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
            race_result_url = [f'http://api.jolpi.ca/ergast/f1/{year}/{r}/results/' for r in last_30_day_rounds]
            return race_result_url, last_30_day_rounds
        elif table == 'sprint' :
            race_result_url = [f'http://api.jolpi.ca/ergast/f1/{year}/{r}/sprint/' for r in last_30_day_rounds]
            return race_result_url, last_30_day_rounds


    # if year is entered then return the urls for the missing rounds not currently in db
    else:
        missing_rounds, last_30_day_rounds =  get_missing_rounds(engine,schema,table,year)
        #if the missing rounds is an int i.e equal to 100 or 200 that means the table does not exist or is missing all of its data
        if type(missing_rounds)!= int:
            if table == 'race' or table == 'races':
                race_result_url = [f'http://api.jolpi.ca/ergast/f1/{year}/{r}/results/' for r in missing_rounds]
                return race_result_url, missing_rounds
            elif table == 'sprint' :
                race_result_url = [f'http://api.jolpi.ca/ergast/f1/{year}/{r}/sprint/' for r in missing_rounds]
                return race_result_url,  missing_rounds
        #if table does not exist then return all possible rounds for that year have date or sprint_date that are less
        #than today's date minus 1 day
        else:
            sch_rounds_season = get_rounds_date_for_season(engine, schema, table, year)
            if table == 'race' or table == 'races':
                missing_rounds = sch_rounds_season.loc[sch_rounds_season.date <= date_check_max, 'round'].to_list()
                race_result_url = [f'http://api.jolpi.ca/ergast/f1/{year}/{r}/results/' for r in missing_rounds]
                return race_result_url,missing_rounds
            elif table == 'sprint' :
                missing_rounds = sch_rounds_season.loc[sch_rounds_season.sprint_date <= date_check_max, 'round'].to_list()
                race_result_url = [f'http://api.jolpi.ca/ergast/f1/{year}/{r}/sprint/' for r in missing_rounds]
                return race_result_url, missing_rounds


def get_race_qualifying_results(qualifying_url)-> pd.DataFrame:
    """returns the RACE (excludes sprint results) results for a race selected using round and year
    and the laps urls by driver to retrieve the lap position per driver
    actual race figures includes points!!!
    results end point

    uses jolpica endpoint: http://api.jolpi.ca/ergast/f1/{year}/{r}/qualifying/'
    
    #for some weird reason austin 2015 has no Q3 in api which matches this
    https://www.formula1.com/en/results/2015/races/933/united-states/qualifying
    """

    #if time out occurs print the url that made it happen
    print(qualifying_url)
    # retrieve object and make it a json
    r = requests.get(qualifying_url).json()
    # get the qualifying results if not updated in jolipica return empty df
    data = r['MRData']['RaceTable']['Races']
    if len(data) == 0:
        #name columns so can still merge if empty
        #some reason season 2002 round 1 australia has not qualifying data even though its is in the f1 website-11/12/24
        #https://www.formula1.com/en/results/2002/races/720/australia/qualifying/0
        #https://api.jolpi.ca/ergast/f1/2002/1/qualifying/
        df = pd.DataFrame(columns = ['round', 'season','Q1', 'Q2', 'Q3', 'Driver_driverId','qualifying_result'])
        return df
    else:
        data = data[0]
        # the 0 is the index bc we only want to run 1 race at a time 
        df = pd.json_normalize(data['QualifyingResults'])
        df.loc[:,'round'] = int(data['round'])
        df.loc[:,'season'] = int(data['season'])
        #filter only needed cols
        #df = df.loc[:,['season','round', 'Q1', 'Q2', 'Q3', 'Driver.driverId']]
        # convert all numeric cols to correct data type
        df = convert_df_types(df, ['season','round','number','position'],'int64')
        df = convert_df_types(df, ['Driver.permanentNumber'],float)
        #make driver dob datetime
        df['Driver.dateOfBirth'] = pd.to_datetime(df['Driver.dateOfBirth'])
        #rename columns to be more python friendly to use panads
        df.columns = df.columns.str.replace(".","_")
        #the qualifying postion is called grid in race
        df.rename(columns={'position': 'qualifying_result'}, inplace=True)
        return df
    


def get_race_results(race_url)-> pd.DataFrame:
    """returns the RACE (excludes sprint results) results for a race selected using round and year
    and the laps urls by driver to retrieve the lap position per driver
    actual race figures includes points!!!
    results end point

    uses jolpica endpoint: http://api.jolpi.ca/ergast/f1/{year}/{r}/results/

           '"""

    #if time out occurs print the url that made it happen
    print(race_url)
    # retrieve object and make it a json
    r = requests.get(race_url).json()
    # get the race info and data
    #data is still list so you can need to check if it returns any values by using len
    data = r['MRData']['RaceTable']['Races']
    if len(data) == 0:
        #for races that have not occured yet but the sprint or qualifying has
        return pd.DataFrame()
    else:
        data = data[0]
        # the 0 is the index bc we only want to run 1 race at a time 
        df = pd.json_normalize(data['Results'])
        df.loc[:,'round'] = int(data['round'])
        df.loc[:,'season'] = int(data['season'])
        #show its race data
        df.loc[:,'results_type'] = 'race'
        #drop not needed cols
        df.drop(columns = ['positionText'], inplace= True)
        # convert all numeric cols to correct data type
        df = convert_df_types(df, ['round', 'season','number', 'position', 'grid', 'laps'],'int64')
        #had to convert points from int to float bc for season 2021 round 12 max get 12.5 points, some drivers don't 
        #have pernament numbers season 2014 round 1
        df = convert_df_types(df, ['points','Driver.permanentNumber'],'float')
        #not all columns have fastest lap info for some reason eg. season 24 round 8 
        if 'FastestLap.AverageSpeed.speed' in df.columns:
            df = convert_df_types(df,'FastestLap.AverageSpeed.speed', float)
        #make driver dob datetime
        df['Driver.dateOfBirth'] = pd.to_datetime(df['Driver.dateOfBirth'])
        #rename columns to be more python friendly to use panads
        df.columns = df.columns.str.replace(".","_")
        #drivers = df['Driver_driverId'].unique()
        #get the lap url to get laps data for each drivers
        #laps_url = [f"https://api.jolpi.ca/ergast/f1/{int(data['season'])}/{int(data['round'])}/drivers/{driver}/laps/?limit=100&offset=0" for driver in drivers]
        return df 


def get_fin_race_results(race_url):
    """combines, the qualifiyng Q1, Q2, Q3, race results for a given round in a season race
    will only qualifying if race has not occured yet"""
    
    #f1 sprints and qualifying occur before races

    # get qualifying url to get times for q1,q2,q3 grid is where people qualified
    qualifying_url = race_url.replace("results",'qualifying')
    #get qualifying results
    qualifying_results = get_race_qualifying_results(qualifying_url)
    #get race results and lap_urls
    race_results = get_race_results(race_url)
    #if race not occured yet it will return empty race_results df
    if race_results.empty == True: 
        #make names db friendly for sqlalchmey querying 
        qualifying_results.rename(str.lower, axis =1, inplace=True)
        # rename driverid to be same as in final results table
        qualifying_results.rename({'Driver_driverId':"driverid"}, axis =1, inplace=True)
        # if there is no sprint return the qualifying results only
        return qualifying_results
    #if race data is populated then combine race results with qualifying sprint data and         
    else:
        #use race results df as base df and pits df only applies to race data so concat (union) sprint at the end
        #only keep relevent qualifying cols 
    

        #for some weird reason austin 2015 has no Q3 in api which matches this  
        #https://www.formula1.com/en/results/2015/races/933/united-states/qualifying
        #also https://www.formula1.com/en/results/2005/races/777/europe/qualifying/0 does not have any results.
        #these are the desired columns that a qualifying data should have
        desired_qual_cols =  ['round', 'season','Q1', 'Q2', 'Q3', 'Driver_driverId','qualifying_result']
        #filer the data so that it only includes the columns that are in the desired_qual_cols so can account for fringe examples
        #in some races where the qualifying data is missing Q3, or Q2 or Q1. 
        qualifying_results_clean = qualifying_results.loc[:, [c for c in  qualifying_results.columns if c in desired_qual_cols]]
        #merge the race to cleaned qualifying
        race_qual = race_results.merge(qualifying_results_clean, how = 'left', on = ['season', 'round', 'Driver_driverId'] )
        race_qual.rename(columns = {'Driver_driverId': 'driverId'},inplace=True)
        race_qual.rename(str.lower, axis =1, inplace=True)
        return race_qual
    


def db_races_update(engine, schema:str, table:str,year=None):
    """updates db with data correctly
    
    #if only one round was entered
    """
    
    #if erorr is through catch the time 
    time_to_try_again_datetime = dt.datetime.now() +dt.timedelta(hours=1)
    hour, mins = time_to_try_again_datetime.time().hour, time_to_try_again_datetime.time().minute

    
    #all missing rounds if returned come in lists, if list is empty the table is up to date 
    try:
        race_urls, rounds = get_race_url(engine, schema, table,year)
        if len(rounds) ==0:
            print(f"Table {table} is all up to date for season {year}")
    
        # if only 1 round returned extract it so it can work
        elif len(rounds) ==1: 
            df = get_fin_race_results(race_urls[0])
            if df.empty ==True:
                print(f"Season {year} round {round} is not avaliable via api yet please try again later")
            else:
                #check table actually exists first
                db_update_check(df,engine,schema, table)
        #if multiple rounds were entered loop through list
        else:
            for race, r in zip(race_urls, rounds):
                df = get_fin_race_results(race)
                if df.empty ==True:
                    print(f"Season {year} round {r} is not avaliable via api yet please try again later")
                else:
                    #check table actually exists first
                    db_update_check(df,engine,schema, table)
            
        result = pd.read_sql(f'select round, count(*) from {schema}.{table} where round in {tuple(rounds)} group by 1', engine).sort_values(by = 'round',ascending = False)
        print(result)
    except KeyError:
        print(f'Jolpica Api has reached it maximum rate limit of 500 runs per hour please try again after {hour}:{mins}')
    
            