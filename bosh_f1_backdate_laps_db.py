""" 
script back dates the laps data for seasons specified or the latest rounds
only works if result for round and race already in database because must pull by driverid
"""
import requests
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine, text, types
import psycopg2
import datetime as dt
from dotenv import load_dotenv
import os
import openpyxl

from bosh_f1_race_results import db_races_update
from bosh_f1_sprint_results import db_sprint_update
from bosh_f1_season_schedule import get_season_schedule , db_seasons_update
from bosh_f1_get_laps_data import get_laps_data
from utils import get_engine, latest_laps_update_excel, backdate_laps_data_excel



# get the db login details
load_dotenv()
#load engine with db name
engine = get_engine('bosh_f1')


""" in order to update the laps data, you must already have the race results for the round 
and season already in the database. this is because it uses the driver id to pull the
data from the api
"""

"""
BACKDATING LAPS DATA GREATER THAN LAST 30 DAYS

to back date laps data for rounds in season that exceed the last 30 days use the below
code copy and un-comment

laps data is only updated once and is contigent on the race results data for the season and
round specified already being stored in the database because the api endpoint requires driver id

thus laps data should be updated last, after 1) season schedule and 2) race results

lap results data is not likely to change since it is what happened during the race. 
the script works by finding matching driver ids from the specified season and checking round by 
round which drivers are missing data.

the api times out after a while usually after 500 hits. 
you must wait about an hour before you can run it again, 
the script prints to console the time you can try again when it hits a timeout error
you can rerun the same code below but it might be faster to keep track of the season in which the error timed out
the script also prints to console which season, round, driver was updated to the laps table for each update
the function only requires the missing season to update the laps data
if the season entered has all rounds present in database for all drivers it just prints a message confirming
that the season entered is already in database"""




""""
 Args:
      engine: 
        sqlalchemy engine to alter/update database
      schema (str):
        database schema name 
      col (str):
       name of column in table you want to partition out data by to store in tabs in excel usually season 
      lap_table (str): 
       name of table in database that stores the laps data 

       
       
"""

#CODE STARTS HERE
start = 2001
end = 2025
schema = 'f1_bosh'
lap_table_name = 'lap'
col = 'season'

#backdates all laps data for time specified season
for i in sorted(np.arange(start,end),reverse=True):
   get_laps_data(engine,  schema, lap_table_name, i)


# once you are satisfied with the laps data back date you can use 
# the below function to update the excel to repo to populate the tableau dash
"""""below function backdate_laps_data_excel(engine,schema:str,col:str,lap_table:str):
updates laps data by season to xlxs file in this repo titled "laps_data_for_tableau.xlsx" 
to populate the tableau dash 

below are its args:

    Args:
      engine: 
        sqlalchemy engine to alter/update database
      schema (str):
        database schema name 
      col (str):
       name of column in table you want to partition out data by to store in tabs in excel usually season 
      lap_table (str): 
       name of table in database that stores the laps data 

    Returns:
      xlxs of laps data, position for each driver for each race during a season used to populate tableau dash

    

since the laps data is row heavy (each season has a permutation by the number of rounds, drivers and laps)
the col argument in the function is used to partition out the data from the database 
to be stored as sheets the default would by to store it by season
the col has been left in incase you want to rename the columns from what they were ingested as
"""""

#when you are satisfied with the back log use this to update the excel that populates the tableau
#by uncommenting the code below
#backdate_laps_data_excel(engine, schema, col, lap_table_name)

