# LIBRARIES
import pandas as pd
from sqlalchemy import create_engine


#----  DATABASE FUNCTIONS  ---------------------------------------------------------------------------------------------


# Function that takes the path in which the database you want to work with is located

def get_path():
    path = "initial_database_from_json.db"           # Write specific path
    return path


# Function that reads the file where the data is saved and create a dataframe

def fromSQLtoDF(list_name):

    route = get_path()                                      # Take the database route
    engine = create_engine(f'sqlite:///{route}')            # Create the Engine
    query = "SELECT * FROM '{}' ".format(list_name)         # Set the query
    df = pd.read_sql_query(query, engine)                   # Create the DataFrame from the SQL
    engine.dispose()                                        # Close and release the Engine

    return df


# Function that creates a Database from the dataframe introduced

def fromDFtoSQL(df, list_name, existence):

    # Give the option to choose whether you want to add to an existing list or replace it
    if existence == 0:
        if_exists = 'replace'
    elif existence == 1:
        if_exists = 'append'

    route = get_path()                                                      # Take the database route
    engine = create_engine(f'sqlite:///{route}')                            # Create the Engine
    df.to_sql(list_name, con=engine, if_exists='replace', index=False)      # Create the SQL from the DataFrame
    engine.dispose()                                                        # Close and release the Engine


