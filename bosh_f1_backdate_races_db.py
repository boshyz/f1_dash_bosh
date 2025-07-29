# back dates the race and sprint results for each round in a season for given seasons

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
from utils import get_engine, backdate_points_data_excel, latest_points_update_excel



# get the db login details
load_dotenv()
#load engine with db name
engine = get_engine('bosh_f1')

""""BACKDATING RACE DATA GREATER THAN LAST 30 DAYS

to back date race data for rounds in season that exceed the last 30 days use the below
code copy and un comment

race data is only updated once for dates that are greater than last 30 days
for races results that are less than last 30 days than the current date,  
those are deleted and updated into database again till the race start date exceeds the
30 day look back window races data can only be updated if the season schedule for 
the season of interest is in the database


the api times out after a while usually after 500 hits. 
you must wait about an hour before you can run it again, 
the scritp prints to console the time you can try again when it hits a timeout error
you can rerun the same code below but it might be faster to keep track of the season 
in which the error timed out the script also prints to console which season, and round was 
updated to the race table for each update.
the function only requires the missing season to update the races data
for past seasons i.e not current season, where all rounds are present in database 
the script will print a confirmation message to the console stating that all rounds are updated 
the entered season of interest
for current seasons script only print message 
if rounds are less than current the season entered is already in database


once you are satisfied with the laps data back date you can use 
the below function to update the excel to repo to populate the tableau dash"""

""""
gets the latest race data if set to year
uses engine, schema_name, table_name, season
schema_name, table_name need to entered as string
if year is left empty, function will check if the latest race round 
for the current season is in the db, if latest round of latest season is missing it will update it.
if year is entered as a 4 digit int, it will update all missing rounds for the whole year

db_race_update updates data base with race results and db_sprint_update updates db with sprint 
results
 """


#CODE STARTS HERE
#change the start and end args to be date ranges you want stored in the database that are inclusive
#enter the database schema, race and sprint results table as strings 

start = 2001
end = 2025
schema ='f1_dash'
race_table = 'race'
sprint_table = 'sprint'

#column name to partition results for points by default is set to sesaon unless you named otherwise
col = 'season'

#back date race results from 2001 till 2025:
for i in np.arange(start,end+1):
    db_races_update(engine, schema, race_table, i)



#back date sprint results from 2001 till 2025:
for i in np.arange(start,end+1):
    db_sprint_update(engine, schema, sprint_table, i)


#if you make a mistake drop the table you created using the query below change where appropriate
#conn = engine.connect()
#conn.execute(text("DROP TABLE f1_dash.race"))

#after you are satisfied with back date race and sprint results in the database 
#use the below function to create an excel file to populate tableau

"""""below function backdate_points_data_excel produces an excel file in this repo
titled "points_data_for_tableau.xlsx"

below are its args:

backdate_points_data_excel(engine,schema:str,col:str,race_table:str, sprint_table:str):

updates points by combining race and sprint data by season to xlxs for tableau dash

    Args:
      engine: 
        sqlalchemy engine to alter/update database
      schema (str):
        database schema name
      col (str):
       name of column in table you want to partition out data by to store in tabs in excel usually season 
      race_table (str):
        str name of the table in database that stores race results data 
      sprint_table (str):
        str name of the table in database that stores sprint results data  

    Returns:
      xlxs file with points data for sesaon and round with sprint and race data together

since the race results data is row heavy the col argument in the function
is used to partition out the data from the database to be stored as sheets
the default would by to store it by season
the col has been left in incase you want to rename the columns from what they were ingested as
"""""

#backdate_points_data_excel(engine, schema, col, race_table, sprint_table)
