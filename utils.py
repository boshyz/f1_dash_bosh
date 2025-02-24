"""functions to help update in database and format help format api endpoints"""

import requests
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import types, create_engine, text
import psycopg2
import datetime as dt
from googleapiclient.discovery import build
from dotenv import load_dotenv
import os
import openpyxl

def get_engine(db_name):
    """create engine for database set as global variable engine and global variable this year

    Args:
      db_name (str):
       name of database 

    Returns:
      sqlalchmy engine object set to database and schema so can be used to alter/update data retrieved

    """
    global engine
    BOSHF1_POSTGRES_URI = os.getenv('BOSHF1_POSTGRES_URI')
    password = BOSHF1_POSTGRES_URI+db_name
    # return engine for sqlclchemy
    engine = create_engine(password,isolation_level="AUTOCOMMIT")
    return engine


#bosh_f1_season_schedule
def create_date_field(df:pd.DataFrame, date_field, format ='%Y-%m-%d'):
    """takes in dataframe and list of columns to turn into datetime

    Args:
      df (pd.DataFrame):
        the data stored as a pandas dataframe  
      date_field (str/list):
        string or list of strings of columns names in dataframe to be converted to datetime 
      format (str:  (Default value = '%Y-%m-%d'):
        date format to make column datetime

    Returns:
      pandas dataframe with the columns entered into date_field converted to a datetime object

    """
    #date_fields = check_date_cols(fields, table)
    if df.empty != True:
        try:
            if type(date_field) == str:
                df[date_field] = pd.to_datetime(df[date_field], format=format)
                return df
            else:
                for i in date_field:
                    df[i] = pd.to_datetime(df[i], format=format)
                #print(df.dtypes)
                return df
        except KeyError as e:
            print(e)
    else:
        print("Empty df")

def clean_lap_position(x):
    """cleans a value and if value is "None" returns np.nan ""
    
    sometimes api pulls in column position from the laps endpoint as "None" for drivers
    who did not finish the race but had completed more than 1 lap:
    see this example:
    https://api.jolpi.ca/ergast/f1/2018/18/drivers/alonso/laps/?limit=100&offset=0
    This causes the column to ingested in as a str/object data type
    so this cleans

    Args:
      x: 
       column value for the lap position in the laps df

    Returns:
       np.nan if value is empty

    """
    if x == "None":
        return np.nan
    else:
        return x

def convert_df_types(df:pd.DataFrame,col, kind:str):
    """converts column(s0) in dataframe to specified kind of data type if the dataframe is not empty

    Args:
      df (pd.DataFrame):
        data stored as pandas dataframe  
      col (str): 
        name of the column in dataframe you want to change 
      kind (str):
        the data type you want to convert the column in col to  

    Returns:
       dataframe of the columns listed in col converted to the data type set in kind

    """
    if df.empty == True:
        return df
    else:
        if type(col) == str:
            new_types = {col:kind}
            df = df.astype( new_types)
            return df
        else:
            new_types = {i:kind for i in col}
            df = df.astype(new_types)
            return df

def get_df_sql_types(df):
    """takes df and filters for data types then assigns correct sqlalchmy data type
    to each feild returned as a dictionary
    for some reason pandas from_sql does not conserve date data type when extracting 
    so we have to find all data fileds that date in name and make them datetime so
    it can be ingested correctly into the database

    Args:
      df:
       data stored as panadas dataframe 

    Returns:
      dictionary of column names assigned to correct sqlaclhemy data types for given dataframe

    """
    df = create_date_field(df, df.filter(like = 'date').columns.to_list(), format ='%Y-%m-%d')
    
    float_cols = df.select_dtypes(include='float').columns.to_list()
    int_cols = df.select_dtypes(include='int').columns.to_list()
    date_cols = df.select_dtypes(include='datetime').columns.to_list()
    url_cols = df.filter(like = 'url', axis =1 ).columns.to_list()
    #get non generic str cols
    non_str_cols = float_cols+int_cols+url_cols+date_cols
    str_cols = df.loc[:,~df.columns.isin(non_str_cols)].columns.to_list()
    float_dict = {col: types.FLOAT() for col in float_cols}
    int_cols_dict = {col: types.INTEGER() for col in int_cols}
    date_cols_dict = {col:  types.DATE() for col in date_cols}
    url_dict = {col: types.VARCHAR(length=300) for col in url_cols}
    # all other strings get varchar length 50
    str_dict = {col: types.VARCHAR(length=50) for col in str_cols}
    # combine all dicts together
    data_types = float_dict | int_cols_dict | url_dict | date_cols_dict | str_dict
    return data_types
        
        
def update_table(df, engine, schema, table, if_exists='append'):
    """write to updated date to db table

    Args:
      df: 
       pandas dataframe of data
      engine: 
       sqlalchmey engine to alter/update database
      schema (str):
        name of database schema you want to update 
      table: 
        name of table in database you want to update
      if_exists:  (Default value = 'append')
       if newly created table i.e not in database can set optional if_exists to replace
       default is append to existing table 

    Returns:
      message that data has been updated to table for season and round and number of rows updated

    """
    season = df.season.unique()[0]
    round = df['round'].unique()[0]
    data_types = get_df_sql_types(df)
    df.to_sql(name = table,
                con = engine,
                schema = schema,
                if_exists = if_exists, 
                index = False, 
                dtype= data_types,
                method = 'multi',)
    row_count = len(df)
    print(f"Updated {row_count} rows for round:{round} season:{season} into {table}")


    
def get_tables(engine, schema) -> pd.DataFrame:
    """returns the names of tables in db as a df

    Args:
      engine: 
       sqlalchmey engine to alter/update database
      schema (str): 
       name of schema in database

    Returns:
      pandas dataframe that lists out the table names in the postgres database

    """
    query = f"select table_name from information_schema.tables where table_schema = '{schema}' "
    conn =  engine.connect()
    tables = conn.execute(text(query))
    available_tables = pd.DataFrame(tables.fetchall())
    return available_tables 



def return_table_names(engine,schema):
    """returns empty list if schema has no tables else names of tables in a list

    Args:
      engine:
       sqlalchmey engine to alter/update database 
      schema (str):
        name of database schema 

    Returns:
     list of table names in database and schema specified to be used in message
     

    """
    available_tables = get_tables(engine,schema)
    if len(available_tables) == 0:
        return []
        
    else:
        table_names = available_tables.iloc[:,0].to_list()
        return table_names

def append_or_replace(engine, schema, table):
    """
    returns the right if_exists argument for a given table name in database given its schema

    if the table name is not currently in schema it will return replace else append 

    Args:
      engine: 
       sqlalchmey engine to alter/update database
      schema (str):
       name of database schema 
      table (str):
       name of databse table  

    Returns:
      list containing append or replace depending on if name is already in database and schema

    """
    if_exists = ['append' if table in return_table_names(engine,schema) else 'replace'][0]
    return if_exists    

def retrieve_data_query(schema:str,table:str,*cols):
    """returns sql query to retrieve results from database based on params entered


    Args:
      schema (str):  schema name for database
      table (str): table name in database
      *cols (str) :inputted like where filter conditions in table e.g season = 2024

    Returns:
      : formated sql query str
      e.g Select * from table where col = input and col2 = input

    """
    if len(cols) ==1:
        query = f'SELECT * FROM {schema}.{table} WHERE {cols[0]}'
        return query
    elif len(cols) >1:
        rest_cols = [" AND "+ col for col in cols[1:]]
        base_query = f'SELECT * FROM {schema}.{table} WHERE {cols[0]}' 
        for i in rest_cols:
            query = base_query + i
        return query
    else:
        query = f'SELECT * FROM {schema}.{table}'
        return query 
    
def sense_check_query(schema:str,table:str,*cols):
    """returns sql query to sense check the number of rows in a table based on params entered
    
    Args:
    schema (str):  schema name for database
      table (str): table name in database
      *cols (str) :inputted like where filter conditions in table e.g season = 2024

    Returns:
      : formated sql query as a str that sense checks the number of rows for a table in the database
      based on params entered
      e.g SELECT COUNT(*) AS rows FROM table WHERE  col = input AND col2 = input

    """
    if len(cols) ==1:
        query = f'SELECT COUNT(*) AS rows FROM {schema}.{table} WHERE {cols[0]}'
        return query
    elif len(cols) >1:
        rest_cols = [" AND "+ col for col in cols[1:]]
        base_query = f'SELECT COUNT(*) AS rows FROM {schema}.{table} WHERE {cols[0]}' 
        for i in rest_cols:
            query = base_query + i
        return query
    else:
        query = f'SELECT COUNT(*) AS rows FROM {schema}.{table}'
        return query 
    

def get_delete_query(check_query):
    """takes sense check query which comes in as
    SELECT COUNT(*) FROM table etc
    and formats it to a delete query for the same condition

    Args:
      check_query (str):
        str of sql query reads  f'SELECT COUNT(*) AS rows FROM {schema}.{table}'
       

    Returns:
       str of delete query where it creates a delete query from the same table

    """
    delete_query = check_query.replace("SELECT COUNT(*) AS rows", "DELETE")
    return delete_query

#utils
def delete_from_db(engine, delete_query):
    """deletes data from speciefied table

    Args:
      engine: 
       sqlalchemy engine to update/alter database
      delete_query (str): 
        str delete query to delete from table

    Returns:
      message about the table being deleted

    """
    with engine.connect() as conn:
        #need to wrap in sqlaclemy text to make it executionable in vs code works fine as in jupiternotebook
        conn.execute(text(delete_query))
        #print what was deleted
        delete_message = delete_query.replace("DELETE","DELETED").replace(" WHERE","").replace(" =","")
        print(delete_message)
    
def row_count(engine, check_query):
    """returns the number of rows of data in db for a check query

    Args:
      engine: 
       sqlalchemy engine to update/alter database
      check_query (str): 
       str to understand row count for table and selected params 

    Returns:
      the number of rows that the query has

    """
    rows = pd.read_sql(check_query, engine).rows[0]
    return rows


def get_data(engine, schema:str,table:str,*cols):
    """returns a df from database based on filtered conditions with date cols formated as date fields
    
    takes schema, table, and filtered column params for a sql query and returns df of query
    when extracting from db using pd.read_sql the date data field types do not persist
    the create_date_field func is used to convert them to datetime so the correct filtering can be done in python
    
    used when sense checking and updating data
    
    schema (str) : schema name
    table  (str) :  table name
    cols (str):  inputted like where filter conditions in table e.g season = 2024

    Args:
      engine: 
      schema:str: 
      table:str: 
      *cols: 

    Returns:

    """
    retrieve_data = retrieve_data_query(schema, table, *cols)
    df = pd.read_sql(retrieve_data,engine)
    date_cols = df.filter(like = 'date').columns.to_list()
    df = create_date_field(df, date_cols)
    return df



def get_rounds_date_for_season(engine, schema:str,table:str,year:int):
    """returns the rounds and dates for a season from the season schedule based on race or sprint

    Args:
      engine: 
      schema:str: 
      table:str: 
      year:int: 

    Returns:

    """

    #last_30_days_check = str(dt.datetime.today().date() - dt.timedelta(days = 30))
    #year , month, day = last_30_days_check.year, last_30_days_check.month, last_30_days_check.day
    #
    #race_query = f"SELECT DISTINCT round FROM {schema}.season WHERE season = {year} AND date <= '{last_30_days_check}' " 
    #sprint_query = f"SELECT DISTINCT round FROM {schema}.season WHERE season = {year} AND sprint_date IS NOT NULL AND sprint_date <= '{last_30_days_check}' "
    race_query = f"SELECT round,date, qualifying_date FROM {schema}.season WHERE season = {year} " 
    sprint_query = f"SELECT round,sprint_date  FROM {schema}.season WHERE season = {year} AND sprint_date IS NOT NULL "
    try:
        if table == 'sprint':
            rounds = pd.read_sql(sprint_query,engine)
            rounds = create_date_field(rounds, 'sprint_date')
            return rounds
        elif table == 'race' or table == 'races' :
            rounds = pd.read_sql(race_query,engine)
            rounds = create_date_field(rounds, 'date')
            return rounds
        #elif table == 'lap'
    except:
        psycopg2.errors.UndefinedTable
    print(f"Table:{table} not in database please try again using race for race results or sprint for sprint results")


def get_rounds_in_table_db(engine, schema:str,table:str,year:int):
    """returns what rounds are in the table in the db so far

    Args:
      engine: 
      schema:str: 
      table:str: 
      year:int: 

    Returns:

    """
    #query to get
    query = f"SELECT DISTINCT round FROM {schema}.{table} WHERE season = {year}"
    try:
        rounds = pd.read_sql(query,engine)['round'].to_list()
        return rounds
    except:
        psycopg2.errors.UndefinedTable
        print(f'Table {table} does not exist in database')
  


def get_rounds_in_table_db(engine, schema:str,table:str,year:int):
    """returns what rounds are in the table in the db so far

    Args:
      engine: 
      schema:str: 
      table:str: 
      year:int: 

    Returns:

    """
    #query to get
    query = f"SELECT DISTINCT round FROM {schema}.{table} WHERE season = {year}"
    try:
        rounds = pd.read_sql(query,engine)['round'].to_list()
        return rounds
    except:
        psycopg2.errors.UndefinedTable
        print(f'Table {table} does not exist in database')
  
def get_missing_rounds(engine,schema, table, year):
    """returns the unique number of rounds for a season that have a date, or sprint_date equal to
    less than 30days from today for a given year.
    the db should have the rounds
    
    if data is missing or table does not exist it will return an int
    returns 200 if table not in database
    returns 100 if table present but is empty
    else returns both the missing_rounds the rounds that exceeed last 30 day look back window was lists

    Args:
      engine: 
      schema: 
      table: 
      year: 

    Returns:

    """
    
    last_30_days_check = dt.datetime.today() - dt.timedelta(days = 30)

    #check if table in database
    #if table not in database it will return 200
    tables = return_table_names(engine,schema)
    if table not in tables:
        print(f"Table {table} does not exist in database")
        return 200,200
        

    else:
        #if table is in database see if data for season is in table  it will return 100
        rounds_in_season_db = get_rounds_in_table_db(engine, schema,table,year)
        #if season not in db return 0
        if len(rounds_in_season_db) == 0:
            print(f"No data for season {year} exists in table {table}")
            return 100,100
        else:
            if table == 'sprint':
                #return number of rounds who's sprint dates occur on or before today's date minus 30 days
                sch_rounds_season = get_rounds_date_for_season(engine, schema, table, year)
               
                rounds_for_missing = sch_rounds_season.loc[sch_rounds_season.sprint_date <= last_30_days_check, 'round'].to_list()
                #return rounds who's sprint dates occur after today's date minus 30 days
                last_30_days_del = sch_rounds_season.loc[sch_rounds_season.sprint_date >= last_30_days_check, 'round'].to_list()
                missing_rounds = [i for i in rounds_for_missing if i not in rounds_in_season_db]
                print(f"Table {table} is missing the following rounds: {missing_rounds} for season {year}")
                print(f"Table {table} should delete the following rounds that exceed last 30 day lookback: {last_30_days_del} for season {year}")
                return missing_rounds, last_30_days_del
            elif table == 'race' or table == 'races':
                #return number of rounds who's date occur on or before today's date minus 30 days
                sch_rounds_season = get_rounds_date_for_season(engine, schema, table, year)

                rounds_for_missing = sch_rounds_season.loc[sch_rounds_season.date <= last_30_days_check, 'round'].to_list()
                last_30_days_del = sch_rounds_season.loc[sch_rounds_season.date >= last_30_days_check, 'round'].to_list()
                missing_rounds = [i for i in rounds_for_missing if i not in rounds_in_season_db]
                print(f"Table {table} is missing the following rounds: {missing_rounds} for season {year}")
                print(f"Table {table} should delete the following rounds that exceed last 30 day lookback: {last_30_days_del} for season {year}")
                return missing_rounds, last_30_days_del
                          
#utils
def check_db_for_season_round(df,engine, schema,table):
    """checks if the data from api for the round and season exists in db
    if data for round and season already exists in db deletes it from db
    and appends new df to db

    used to sense check missing points results data, or laps data

    Args:
     df: 
       data in pandas dataframe format
      engine: 
        sqlalchemy engine to alter/update database
      schema (str):
        database schema name 
      table (str):
        database table name 

    Returns:
    none,  message about data update

    """
    #takes the df made from the race url api and gets the year and round to see if its in the table 
    #df will only contain one round for a season
    year, round = df.season.unique()[0], df['round'].unique()[0]
    check_query = sense_check_query(schema,table,f"season = {year}", f"round = {round}")
    check_result = row_count(engine,check_query)
    # if the table exists and data is populated delete the data first 
    if check_result >0:
        #deletes all data from db that matches the season and round in df
        delete_query = get_delete_query(check_query)
        delete_from_db(engine, delete_query)
        #replace with latest entry
        update_table(df, engine,schema, table)
    # if the table exists but there is not data for that season add the data       
    elif check_result == 0:
        update_table(df, engine, schema, table)



#utils
def check_db_for_season(df,engine, schema,table):
    """checks if the data for the season exists in db
    if data for season already exists in the table specified the function deletes it from db
    and appends new df to db

    used to sense check missing season schedule

    Args:
      df: 
       data in pandas dataframe format
      engine: 
        sqlalchemy engine to alter/update database
      schema (str):
        database schema name 
      table (str):
        database table name 

    Returns:
       None, message about update

    """
    year = df.season.unique()[0]
    check_query = sense_check_query(schema,table,f"season = {year}")
    check_result = row_count(engine,check_query)
    # if the table exists and data is populated delete the data first 
    if check_result >1:
        #deletes all data from db that matches the season in df
        delete_query = get_delete_query(check_query)
        delete_from_db(engine,delete_query)
        #replace with results in df
        update_table(df, engine, schema, table)
    # if the table exists but there is not data for that season add the data   
    elif check_result == 0:
        update_table(df, engine, schema, table)


#utils
def db_update_check(df,engine,schema:str, table:str):
    """checks if table exists if not exists creates table, if empty for values then appeneds df

    Args:
      df: 
       data in pandas dataframe format
      engine: 
        sqlalchemy engine to alter/update database
      schema (str):
        database schema name 
      table (str):
        database table name  

    Returns:
     None, message about update

    """
    tables = return_table_names(engine, schema)
    # if there are no tables in the schema create a new table
    if len(tables) == 0:
        update_table(df,engine,schema,table, 'replace')
    #if table does not exist in schema create the table
    elif table not in tables:
        update_table(df,engine,schema,table, 'replace')
    #create delete then append from table if table exists
    else:
        check_query = sense_check_query(schema, table)
        if row_count(engine,check_query)>= 0 and table == 'season':
            check_db_for_season(df,engine,schema,table)    
        #if table is season then use season only to sense check    
        elif row_count(engine, check_query)>= 0:
            check_db_for_season_round(df,engine,schema,table)

#db_points_update(engine, 'f1_dash', 'race') 
def backdate_points_data_excel(engine,schema:str,col:str,race_table:str, sprint_table:str):
    """updates points by combining race and sprint data by season to xlxs for tableau dash

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

    """
    excel = "points_data_for_tableau.xlsx"
    col_values = sorted(pd.read_sql(f"select distinct {col} from {schema}.season ",engine)['season'].to_list(),reverse=True)
    with pd.ExcelWriter(excel) as writer:
        for col_val in col_values:
            race = pd.read_sql(f'SELECT * FROM {schema}.{race_table} where {col} = {col_val }', engine)
            sprint = pd.read_sql(f'SELECT * FROM {schema}.{sprint_table} where {col} = {col_val }', engine)
            race_sprint = pd.concat([race, sprint])
            sheet_name = f'race_sprint_{col_val}'
            race_sprint.to_excel(writer,sheet_name=sheet_name, index = False)
            print(f"Season {col_val} successfully upated to {excel} with {len(race_sprint)} rows")


def latest_points_update_excel(engine,schema:str,col:str,race_table:str, sprint_table:str):
    """updates points by combining race and sprint data for latest season to xlxs for tableau dash
    if the latest round to occur was less than 30 days ago

    Args:
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

    """
    excel = "points_data_for_tableau.xlsx"
    date_check_max = dt.datetime.today()-dt.timedelta(days=30)
    Year= date_check_max.year
    date_check = str(date_check_max.date())
    #sql alchemy doesn't like "" must use '' for quotes
    last_30_day_query = f"SELECT round FROM {schema}.season where {col} = {Year} and date >= TO_DATE('{date_check}', 'YYYY-MM-DD')"
    #get rounds that fit
    rounds = pd.read_sql(last_30_day_query, engine)
    if rounds.empty ==True:
        print(f"There are no races whose start date exceeds the 30 day lookback window from today, no updates made")
    else:
        #since each year is stored as a sheet you need to delete and replace the entire year
        sheet_name = f'race_sprint_{Year}'
        race = pd.read_sql(f"SELECT * FROM {schema}.{race_table} where season = {Year}",engine)
        sprint = pd.read_sql(f"SELECT * FROM {schema}.{sprint_table} where season = {Year}",engine)
        race_sprint = pd.concat([race, sprint])
        with pd.ExcelWriter(excel) as writer:
            # Now here add your new sheets
            race_sprint.to_excel(writer,sheet_name=sheet_name, index = False)
            print(f"{Year} successfully upated to {excel} with {len(race_sprint)} rows")


#db_points_update(engine, 'f1_dash', 'race') 
def backdate_laps_data_excel(engine,schema:str,col:str,lap_table:str):
    """updates laps data by season to xlxs for tableau dash

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

    """
    excel = "laps_data_for_tableau.xlsx"
    col_values = sorted(pd.read_sql(f"select distinct {col} from {schema}.{lap_table} ",engine)[f'{col}'].to_list(),reverse=True)
    with pd.ExcelWriter(excel) as writer:
        for col_val in col_values:
            laps = pd.read_sql(f'SELECT * FROM {schema}.{lap_table} where {col} = {col_val }', engine)
            sheet_name = f'laps_{col_val}'
            laps.to_excel(writer,sheet_name=sheet_name, index = False)
            print(f"Season {col_val} successfully upated to {excel} with {len(laps)} rows")

def latest_laps_update_excel(engine,schema:str,col:str,lap_table:str):
    """updates laps for latest season to xlxs for tableau dash
    if the latest round to occur was less than 30 days ago

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

    """
    excel = "laps_data_for_tableau.xlsx"    
    date_check_max = dt.datetime.today()-dt.timedelta(days=30)
    Year= date_check_max.year
    date_check = str(date_check_max.date())
    #sql alchemy doesn't like "" must use '' for quotes
    last_30_day_query = f"SELECT round FROM {schema}.season where {col} = {Year} and date >= TO_DATE('{date_check}', 'YYYY-MM-DD')"
    #get rounds that fit
    rounds = pd.read_sql(last_30_day_query, engine)
    if rounds.empty ==True:
        print(f"There are no races whose start date exceeds the 30 day lookback window from today, no updates made")
    else:
        #since each year is stored as a sheet you need to delete and replace the entire year
        with pd.ExcelWriter(excel) as writer:
            laps = pd.read_sql(f'SELECT * FROM {schema}.{lap_table} where {col} = {Year }', engine)
            sheet_name = f'laps_{Year}'
            laps.to_excel(writer,sheet_name=sheet_name, index = False)
            print(f"Season {Year} successfully upated to {excel} with {len(laps)} rows")

