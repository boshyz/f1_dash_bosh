""""
Please run this first if you are starting database data from scratch!!!!!

The below script is used to back date season schedules into the database.
season schedules serve as the corner stone to backdate/update data, as 
they are the reference point to understand how many rounds are in a season
and based on the current date, if the round has commenced or not. 
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

from bosh_f1_race_results import get_race_url, get_fin_race_results, db_races_update
from utils import get_engine, backdate_all, last_30_day_update_excel
from bosh_f1_season_schedule import  db_seasons_update, backdate_seasons_excel


# get the db login details
load_dotenv()

#enter the name of your postgres database into the get_engine function to set up connection
engine = get_engine('bosh_f1')

""""The season schedule acts as the foundation for extracting data from the api
It is used to sense check what rounds are missing from the database for race results
and determines what the lookback window and range of results are to power the dashboard
You can't complete race results updates without a race schedule for the season of interest
make sure you run this script first 

the function db_seasons_update works by using the function db_update_check.
db_update_check will see if the name of the table entered exists in the database.
if the table name does not exist it will insert the data into the database 
with a create table statement (sqlalchemy - replace)
please remember your naming convention for the table name created to store the season schedule data
the ones set in my database are singular lap, sprint, race, season """


#CODE STARTS HERE!!!
    
#db_seasons_update updates a table name season in the database which has been used to store 
#season schedules.
# db_seasons_update(engine, schema_name, season_schedule_table_name, start_year, end_year)
#enter desired date range into start and end
# end is inclusive so e.g. if it is set to 2025 it will also pull data for the year 2025
start = 2001
end = 2025
schema = 'f1_dash'
season_table_name = 'season'

db_seasons_update(engine,schema, season_table_name,start,end)

#backdate_seasons_excel retrieves season schedules from database and stores into an excel file
#  
"""""below function backdate_seasons_excel produces an excel file in this repo
titled season_schedule_for_tableau.xlsx
below are its args, the schema and season_schedule_table_name need to entered as string arguments
backdate_seasons_excel(engine, schema, season_table_name)
since the season schedule is not row heavy all the season schedules are stored in one sheet 
called season
"""""
backdate_seasons_excel(engine, schema, season_table_name)
