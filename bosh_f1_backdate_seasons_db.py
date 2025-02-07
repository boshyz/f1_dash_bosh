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
#hi = get_race_url()
#hi = get_fin_results(get_race_url())

# get the db login details
load_dotenv()

engine = get_engine('bosh_f1')

    
# update the schedule into f1_dash.seasons table for 2001 to 2024
#update this first into the db so you can use it for race url 
db_seasons_update(engine,'f1_dash', 'seasons')

#print(pd.read_sql('select distinct seasons from f1_dash.seasons', engine))
#oldest season in table is 2001
#ackdate_seasons_excel back date seasons table if latest avaiable and updates excel 
#backdate_seasons_excel(engine, 'f1_dash', 'season', 'season')