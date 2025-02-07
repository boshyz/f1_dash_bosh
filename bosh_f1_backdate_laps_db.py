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
from bosh_f1_get_laps_data import get_laps_data, get_laps_url
from utils import get_engine, latest_laps_update_excel, backdate_laps_data_excel



# get the db login details
load_dotenv()
#load engine with db name
engine = get_engine('bosh_f1')


#laps, drivers = get_laps_url(engine, 'f1_dash', 'lap', 2024,24)
#print(laps, drivers)
#last_30_day_update_excel(engine,'f1_dash', 'lap')
#

#for i in sorted(np.arange(2001,2008),reverse=True):
get_laps_data(engine, 'f1_dash', 'lap', 2001 )
    

