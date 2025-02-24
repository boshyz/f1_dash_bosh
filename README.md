# f1_dash_bosh
This repo contains scripts created for extracting, transforming, storing data from the Jolpica-F1 Api to populate my F1 Tableau Public Dashboard. 
It is not intended for distribution and serves only an example. 
Link to dashboard found here

I have calibrated the database to be comprised of 4 tables: season, race, sprint, lap to store the data.
You can change these in the code when you make a copy. 

The code is not meant to be deployed but serve as guidance.


### step 1: create your own database
To start, create a postgres database and take note of your username and password. 
Edit the .env file to update the uri.

### step 2: create season schedules to database
use the bosh_f1_backdate_seasons_db.py file to create the season schedules in your database. here you determine what the look back window of your data is. 


### step 3: create points/results back date
use the bosh_f1_backdate_races_db.py file to create the race and sprint results for all rounds in the seasons you have stored season schedules for in your database.


### step 4: create laps back date
use the bosh_f1_backdate_laps_db.py file to create the lap by lap data for all drivers for all rounds in the seasons you have stored season schedules for in your database.


### if updating with latest data 
use the bosh_f1_latest_update.py file to update with the latest data for the last 30 days for the latest season provide you have updated db with latest season schedule
