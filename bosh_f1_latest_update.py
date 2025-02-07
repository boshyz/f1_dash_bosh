# bakc dates the races for the seasons 
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
from utils import get_engine, latest_laps_update_excel,backdate_laps_data_excel
#set up engine enter the database name in as str change accordingly
#use.env to set up your own URI crendentials
engine = get_engine('bosh_f1')

#update race data to database
#db_races_update(engine, 'f1_dash', 'race')
#update laps data to excel for tableau
#last_30_day_update_excel(engine, 'f1_dash', 'race')
#update laps data to database
#get_laps_data(engine, 'f1_dash', 'lap')
#update laps data to excel for tableau
#last_30_day_update_excel(engine, 'f1_dash', 'lap')
#update sprint data
#db_sprint_update(engine, 'f1_dash', 'sprint')
#update sprint data to excel for tableau
# update scea scheudle
#db_seasons_update(engine,'f1_dash', 'season',2025)
#get_laps_data(engine, 'f1_dash', 'lap', 2002  )
#backdate_laps_data_excel(engine,'f1_dash','season','lap')