# back dates the races for the seasons 
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

#gets the latest race data if set to year
#uses engine, schema name, table name, then year
#if year left empty will check if latest race round in db else will update date it
#if year not emtpy will update for whole year
#db_race_update(engine, 'f1_dash', 'sprint', 2024)


""""back date race 2021:
for i in np.arange(2001,2025):
    db_races_update(engine, 'f1_dash', 'race', i)"""


#gets the latest sprint data if set to year
#uses engine, schema name, table name, then year
#if year left empty will check if latest sprint round in db else will update date it
#if year not emtpy will update for whole year
#db_sprint_update(engine, 'f1_dash', 'sprint', 2024)

""""#back date sprint 2021:
for i in np.arange(2001,2025):
    db_sprint_update(engine, 'f1_dash', 'sprint', i)"""


#if you make a mistake drop the table you created using the query below change where appropriate
#conn = engine.connect()
#conn.execute(text("DROP TABLE f1_dash.race"))

#use to back date races
backdate_points_data_excel(engine,'season','f1_dash', 'race')