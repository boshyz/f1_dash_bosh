"""A module to retrieve the laps data for a race during a round and season from the jolpica API lap end point and store 
in specified table of database
 uses this doc as reference: https://github.com/jolpica/jolpica-f1/blob/main/docs/endpoints/laps.md
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
from utils import  get_data, append_or_replace, clean_lap_position, update_table, create_date_field, convert_df_types

def get_pit_url(year:int ,round:int)->str:
    """makes pit stops url from season (year) and race (round)

    Args:
      year (int):
       season you want to query as 4 digit int year 
      round (int):
       round to represent race in season must be int

    Returns (str):
      pit url the end point url to get the pit stop info a given round in a season

    """
    pit_url =f"https://api.jolpi.ca/ergast/f1/{year}/{round}/pitstops/"
    return pit_url
    
def get_pitstops(year:int ,round:int)->pd.DataFrame:
    """returns df of pitstops data for a season and round
    for some reason ergast and thus jolpica are both missing pit stops for
    f1 offical doesn't even have it when u can see it for other ones.
    https://www.formula1.com/en/results/2021/races/1074/belgium/race-result
    example same season with pits https://www.formula1.com/en/results/2021/races/1068/azerbaijan/race-result

    Args:
     year (int):
       season you want to query as 4 digit int year 
    round (int):
       round to represent race in season must be int

    Returns:
       dataframe, uses the year and round to make pit url and returns end point data stored as a pandas df

    """
    pit_url = get_pit_url(year,round)
    data = requests.get(pit_url).json()['MRData']['RaceTable']['Races']
    if len(data) == 0:
        #need column names bc it is used for joining even if empty
        df = pd.DataFrame(columns = ['driverid', 'lap_number', 'pit_stop_number', 'pit_time', 'pit_duration', 'season', 'round'])
        return df
    else:
        data = data[0]
        #make into df
        df = pd.json_normalize(data['PitStops'])
        df.loc[:,'season'] = int(data['season'])
        df.loc[:,'round'] = int(data['round'])
        df = convert_df_types(df,['lap','stop'],int)
        df.rename(columns = {'lap': 'lap_number', "time": "pit_time", 'duration':'pit_duration', "stop": "pit_stop_number"}, inplace =True)
        df.rename(str.lower, axis=1, inplace =True)
        df = convert_df_types(df ,['lap_number','season', 'round'] , int)
        return df
    

def get_laps_url(engine, schema, table, year:int, round:int)->list:
    """returns the lap url for the api retrival by driver based on season and round
    only returns drivers who are not already in lap table in database

    Args:
      engine: 
        sqlaclchemy create engine to access and modify database
      schema (str): 
        name of database schema
      table (str):
        name of database table for laps 
      year:int: 
        4 digit int to represent season
      round:int: 
        int to represent race/round in season

    Returns:
       laps_url: str/list of lap urls for drivers not in laps table in database
       drivers: str/list of driverids for drivers not in laps table in database
       

    """
    
    #check that seasons is latest seasons if not update to the latest year in db for use by using 
    #race schedule api endpoint 
    check_seasons( engine, schema, 'season')
    
    #there is at least a 1 day delay so only return race rounds that have commenced atleast 1 day before today's date
    #all race urls are made by using the race season(year) schdedule stored in the database
    date_check_max = dt.datetime.today()-dt.timedelta(days=1)
    Year= date_check_max.year
    #to make lap url have to fitler it for each driver
    #have to create a list of drivers who were in a race (round) for a year (season) # need to filter out drivers who completed zero laps
    #because they will not have any laps data
    make_drivers_query = f"SELECT DISTINCT driverid FROM {schema}.{'race'} WHERE season = {year} AND round = {round} and laps != 0"
    drivers_df = pd.read_sql(make_drivers_query,engine)
    #this is the offical list of drivers to use
    drivers_race= drivers_df.loc[drivers_df.driverid.isna() == False, 'driverid'].unique()
    #check if drivers for that season and round exists in the db in the laps table
    drivers_in_db_query = f"SELECT DISTINCT driverid FROM {schema}.{table} WHERE season = {year} AND round = {round}"
    try: 
        #read in query to see if the for the table in db the round and season exist
        drivers_in_db = pd.read_sql(drivers_in_db_query,engine)

             #if query returns empty df table exists but does not have any data for the season and round
        if drivers_in_db.empty == True:
            #create lap urls for each driver in the race for that season
            laps_url = [f"https://api.jolpi.ca/ergast/f1/{year}/{round}/drivers/{driver}/laps/?limit=100&offset=0" for driver in drivers_race]
            return laps_url, drivers_race
        #if query returns df with data loop through to find out what drivers are missing and only create lap urls for missing drivers 
        #for the season and round
        elif drivers_in_db.empty == False:
            #drivers currently in laps db
            drivers_db= drivers_in_db.driverid.unique()
            #find the drivers in the round and year in race table who are not currently in the laps table for same rounda and year
            drivers = [d for d in drivers_race if d not in drivers_db]
            #create laps url from them
            laps_url = [f"https://api.jolpi.ca/ergast/f1/{year}/{round}/drivers/{driver}/laps/?limit=100&offset=0" for driver in drivers]
            return laps_url, drivers
        
    except:
        #if the table does not exist return the laps urls for all drivers in the round and season entered
        psycopg2.errors.UndefinedTable
           
        print(f"Table {table} does not exist in db must be created")
        laps_url = [f"https://api.jolpi.ca/ergast/f1/{year}/{round}/drivers/{driver}/laps/?limit=100&offset=0" for driver in drivers_race]
        return laps_url, drivers_race

          
  
def merge_laps_pits(laps:pd.DataFrame,pits:pd.DataFrame)->pd.DataFrame:
    """merge laps data to pits stops data using the driverid, lap_number, season and round

    Args:
      laps (pd.DataFrame): takes a df of the laps data produced from previous function
      pits (pd.DataFrame): takes a df of the pits data produced from previous function

    Returns:
      merged dataframe of laps data to pits data based on driverid, lap_number, season and round

    """
    #if laps df is empty return a empty df with col names
    if laps.empty ==True:
        laps_data = pd.DataFrame()
        return laps_data
    else:
        laps_pits = laps.merge(pits, how = 'left', on = ['driverid','lap_number','season', 'round'])
        #converts lap_position to int
        int_cols = ['lap_number','season', 'round']
        laps_pits = convert_df_types(laps_pits, [i for i in laps_pits.columns if i in int_cols] , int)
        float_cols = ['lap_position','pit_stop_number']
        laps_pits = convert_df_types(laps_pits,[i for i in laps_pits.columns if i in float_cols]   , float)
        return  laps_pits
    
def get_laps_single(lap_url:str):
    """pulls from api to get data for a singular driver during a season and round entered as a str lap_url

    Args:
      lap_url (str):
        lap url for a singular driver 

    Returns:
       dataframe with laps data for a single driver in a given round during a season

    """
    #print lap_url so can trouble shoot which end point if breaks 
    print(lap_url)
    
    laps_main = requests.get(lap_url).json()['MRData']['RaceTable']['Races']
    #if laps df is empty return a empty df no names
    if len(laps_main) == 0:
        laps_data = pd.DataFrame()
        return laps_data
    #for all other drivers that went on to complete more than 1 lap in the race
    else:
        laps_main = laps_main[0]
        season = int(laps_main['season'])
        round = int(laps_main['round'])
        laps_data = pd.json_normalize(laps_main['Laps'], record_path =['Timings'])
        laps_data.loc[:,"lap_number"] = np.arange(1,len(laps_data)+1)
        laps_data.loc[:, "season"] = int(season)
        laps_data.loc[:, "round"] = int(round)
        laps_data.rename({'position':'lap_position','time':'lap_time'}, axis=1, inplace =True)
        laps_data.rename(str.lower, axis=1, inplace =True)
        int_cols = ['lap_number','season', 'round']
        float_cols = ['lap_position']
        #cleans "None" to be "" so can convert to int
        laps_data.loc[:, 'lap_position'] = laps_data['lap_position'].apply(lambda x: clean_lap_position(x))
        laps_data = convert_df_types(laps_data, [i for i in laps_data.columns if i in int_cols] , int)
        laps_data = convert_df_types(laps_data, [i for i in laps_data.columns if i in float_cols] ,float)
        return laps_data

def get_laps_round_update(laps_url)->pd.DataFrame:
    """"gets list of laps urls for each driver for a given round (race) and season and pulls requests
    for each and returns as one df

    Args:
      laps_url (str):
        string of url(s) end point to retrieve lap data for a given round in a season for a driver 

    Returns:
       dataframe of all laps data by drivers for a given round in a season

    """
    #when races have not occured yet there will be no laps data so return empty df
    if len(laps_url) ==0:
        fin_laps =  pd.DataFrame()
        return fin_laps

    #usually its a list of drivers bc gotten from the race      
    else: 
        
        fin_laps = pd.DataFrame()
        index = 0
        #loop through each driver's lap url
        while index < len(laps_url):
            laps_data = get_laps_single(laps_url[index] )
            index += 1
            if laps_data.empty ==True:
                continue
            else:
                fin_laps = pd.concat([fin_laps, laps_data], axis =0)

    return fin_laps



def get_laps_data(engine, schema, table, year=None):
    """updates and uploads to the db the laps data for a season and round
    There is no deleting bc once a race has been run, it doesn't change the out come of the race
    The laps data is joined to the pits data to know when each driver pitted
    for the latest update the year is left as none and is updated

    Args:
      engine:
       sqlalchemy engine to update/modify database 
      schema (str):
       database schema name 
      table (str):
        database table name to store laps data 
      year:  (Default value = None)
         4 digit int for season if default and left as none uses latest year

    Returns:
      message to check if laps table for a given season has data for all rounds
      and that each round has laps data for each driver who drove during that round

    """
         
    #if erorr is through catch the time 
    time_to_try_again_datetime = dt.datetime.now() +dt.timedelta(hours=1)
    hour, mins = time_to_try_again_datetime.time().hour, time_to_try_again_datetime.time().minute

    #see if table exists if not replace then exists then append use this as last argument in update_table
    if_exists = append_or_replace(engine,schema, table)
    
    
    #get most recent date
    date_check_max = dt.datetime.today()-dt.timedelta(days=1)
    Year= date_check_max.year

    
    try:
        if year == None:
            #if year left empty it means you are updating with latest data, so assign year var to today's date's year
            year = Year
            #latest round is the highest round in race table of db
            latest_round = int(pd.read_sql(f'select max(round) as latest from {schema}.race where season = {year}', engine).values[0])
            #produces the laps url for each driver in the round - it only returns the urls of drivers not in db table already
            #laps data is not rewritten or changed so doesn't need to have a look back window
            driver_urls, drivers = get_laps_url(engine, schema, table, year,latest_round)
            #if len driver_urls is 0 then that means all drivers were updated for latest round avalialbe in race table
            if len(driver_urls) == 0:
                print(f'Season {year} round{latest_round} is already in table {table} no need to update')
            else:
                #pull from api to create laps df
                #get_laps_round_update makes df by extracting all driver's laps from api before merging with pits
                laps = get_laps_round_update(driver_urls)
                #pulls from api to get pit stops
                pits = get_pitstops(year,latest_round)
                #left joins the laps data to pit stops for the same round and season
                laps_pits = merge_laps_pits(laps, pits)
                update_table(laps_pits, engine, schema, table,if_exists)
                print(f'Table {table} has been updated with season {year} round {latest_round} for the following drivers: {" ,".join(i for i in drivers)}')
                
            
        
        else:
            #intialize empty df to hold data
            rounds = sorted(get_data(engine, schema, 'race', f'season = {year}')['round'].unique(),reverse =True)
            
            #for each round loop through
            for r in rounds:
                #produces the laps url for each driver in the round
                driver_urls, drivers = get_laps_url(engine, schema, table, year,r)
                if len(driver_urls) == 0:
                    print(f'Season {year} round{r} is already in table {table} no need to update')
                #if the urls are longer than zero it has new data to be added for the season and round specified     
                else:
                    #pull pits data first
                    pits = get_pitstops(year,r)
                    #loop through each driver in the driver urls output then joins it to the pits df, so you update the data base after each driver
                    #since there is rate limit this is the most effective way to run it before it times out and you loose the data
                    for d_url, driver in zip(driver_urls,drivers):
                        #pull laps data from api for driver using driver url
                        laps = get_laps_single(d_url) 
                        #left joins the laps data to pit stops for the same round and season, if no laps data was pulled may be bc driver didn't finish race
                        #merge just returns the column names and empty df as if it had it
                        laps_pits = merge_laps_pits(laps, pits)
                        #update the individual driver to the database
                        update_table(laps_pits, engine, schema, table, if_exists)
                        #print message of update
                        print(f'Table {table} has been updated with season {year} round {r} with {len(laps)} laps for driver {driver}')
                       
    except KeyError:
       print(f'Jolpica Api has reached it maximum rate limit of 500 runs per hour please try again after {hour}:{mins}')

        
   
