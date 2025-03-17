# back dates the lastest seasons, rounds for race & sprint results plus lap data 
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

from bosh_f1_race_results import db_races_update, get_race_url
from bosh_f1_sprint_results import db_sprint_update
from bosh_f1_season_schedule import db_seasons_update
from bosh_f1_get_laps_data import get_laps_data
from utils import get_engine, latest_laps_update_excel,latest_points_update_excel, backdate_points_data_excel,backdate_laps_data_excel
#set up engine enter the database name in as str change accordingly
#use.env to set up your own URI crendentials
engine = get_engine('bosh_f1')


"""BACKDATING POINTS AND LAPS DATA FOR LAST 30 DAYS

to back points data for race and sprints if applicable for round
exports to taleau files to populate tableau
then updates laps data for round
all data is updated filtered for deleted and retrieved if it is within the 30 days of today's date


 """


#CODE STARTS HERE change args!!!


schema = 'f1_dash'
season_table = 'season'
race_table = 'race'
sprint_table = 'sprint'
laps_table = 'lap'
col = 'season'
year = 2025

# only run this if the latest schedule is not in database uncomment below line 38
#db_seasons_update(engine,schema, season_table, year)


# only run this if the latest schedule is not in database uncomment below line 38
#db_seasons_update(engine,schema, season_table, year)

#update race data to database
db_races_update(engine, schema, race_table)
#update sprint data
db_sprint_update(engine, schema, sprint_table)
#update latest points data to excel for tableau
latest_points_update_excel(engine,schema,col,race_table, sprint_table)
#update laps data to database
get_laps_data(engine,  schema, laps_table)
#update laps data to excel for tableau
latest_laps_update_excel(engine,schema,col,laps_table)
