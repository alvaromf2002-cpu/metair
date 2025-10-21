# LIBRARIES
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.cluster import DBSCAN

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

# Import and transform data

def process_data(name):
    # df_origin RAW data
    # df0        Clasified data
    # df1       Valid data
    # df2       Treated data


    df_origin = pd.read_json(name)

    df0 = pd.DataFrame()

    df0["hex"] = df_origin["aircraft"].apply(lambda x: x.get("hex"))
    df0["lat"] = df_origin["aircraft"].apply(lambda x: x.get("lat"))
    df0["lon"] = df_origin["aircraft"].apply(lambda x: x.get("lon"))
    df0["alt_baro"] = df_origin["aircraft"].apply(lambda x: x.get("alt_baro"))
    df0["gs"] = df_origin["aircraft"].apply(lambda x: x.get("gs"))
    df0["tas"] = df_origin["aircraft"].apply(lambda x: x.get("tas"))
    df0["track"] = df_origin["aircraft"].apply(lambda x: x.get("track"))
    df0["mag_heading"] = df_origin["aircraft"].apply(lambda x: x.get("mag_heading"))
    df0["true_heading"] = df_origin["aircraft"].apply(lambda x: x.get("true_heading"))

    df1 = df0.dropna(subset=[col for col in df0.columns if col != "true_heading"])

    df2 = df1[["hex", "lat", "lon", "alt_baro"]].copy()

    df2["alt_baro"] = pd.to_numeric(df2["alt_baro"], errors="coerce")
    df2 = df2.dropna(subset=["alt_baro"])
    df2 = df2[df2["alt_baro"] > 0]

    df2["Wx"] = df1["gs"]*np.sin(np.radians(df1["track"])) - df1["tas"]*np.sin(np.radians(df1["true_heading"]))
    df2["Wy"] = df1["gs"]*np.cos(np.radians(df1["track"])) - df1["tas"]*np.cos(np.radians(df1["true_heading"]))
    df2["W"] =  ( df2["Wx"]**2 + df2["Wy"]**2 )**0.5
    df2["Wind_to"] = (np.degrees(np.arctan2(df2["Wx"], df2["Wy"])) + 360) % 360
    df2["Wind_from"] = (np.degrees(np.arctan2(df2["Wx"], df2["Wy"])) + 180) % 360

    return df2, df0


    # Filter Data 
    # First outliers by log-normal
    # Second cluster outliers 
    # Tune Cutoff values
def filter_data(df):
    # df Input 
    # df1 First filter
    # df2 Second filter
    
    # First outliers by log-normal
    # Sigma log-normal
    sg1 = 2.5

    log_data = np.log(df["W"])

    mu, sigma = log_data.mean(), log_data.std()
    upper = mu + sg1*sigma
    high_thresh = np.exp(upper)

    df1 = df[df["W"] <= high_thresh]

    # Second cluster outliers 

    # Improve geometrical distance (lon,lat != x,y)
    # DBSCAN parameters
    eps = .5      # radius to consider neighbors
    min_samples = 3  # minimum points to be considered dense

    db = DBSCAN(eps=eps, min_samples=min_samples)
    df2 = df1.copy()
    df2['cluster'] = db.fit_predict(df1[["lon", "lat"]])

    df2 = df2[df2['cluster'] >= 0]
    

    # Sigma normal
    sg2 = 1

    df_aux = []
    for cluster_id, group in df2.groupby("cluster"):
        W = group["W"]
        Wx = group["Wx"]
        Wy = group["Wy"]

        # if len(W) < 5:  # skip tiny clusters
        #     # keep small clusters as they are
        #     df_aux.append(group)
        #     continue
        mu, s = W.mean(), W.std()
        mux, sx = Wx.mean(), Wx.std()
        muy, sy = Wy.mean(), Wy.std()

        low, high = mu - sg2*s, mu + sg2*s
        lowx, highx = mux - sg2*sx, mux + sg2*sx
        lowy, highy = muy - sg2*sy, muy + sg2*sy

        group_filtered = group[(group["W"] >= low) & (group["W"] <= high) &
                                (group["Wx"] >= lowx) & (group["Wx"] <= highx) &
                                  (group["Wy"] >= lowy) & (group["Wy"] <= highy)]
        df_aux.append(group_filtered)

    df_clean = pd.concat(df_aux, ignore_index=True)


    return df_clean, df1



def data_treatment(name):
    
    df2, df = process_data(name)
    df4, df3 = filter_data(df2)

    return df4, df



