# LIBRARIES
import pandas as pd
import numpy as np
import json

from sklearn.cluster import DBSCAN
from tinydb import TinyDB

from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#----  DATABASE FUNCTIONS  ---------------------------------------------------------------------------------------------


# Function that takes the path in which the database you want to work with is located

def get_path():
    path = "initial_database_from_json.db"                  # Write specific path
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



# Function that imports and transforms data

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


# Function that filters data by removing log-normal and cluster-based outliers with tuned cutoff values

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
    # df2 = df1.copy()

    df1l = df1[(df1["alt_baro"] < 10000)]
    df1m = df1[(df1["alt_baro"] < 30000) & (df1["alt_baro"] >= 10000)]
    df1h = df1[(df1["alt_baro"] >= 30000)]

    df2l = df1l.copy()
    df2m = df1l.copy()
    df2h = df1h.copy()
    df2l['cluster'] = db.fit_predict(df2l[["lon", "lat"]])
    df2m['cluster'] = db.fit_predict(df2m[["lon", "lat"]])
    df2h['cluster'] = db.fit_predict(df2h[["lon", "lat"]])

    # df2['cluster'] = db.fit_predict(df1[["lon", "lat"]])

    df2l = df2l[df2l['cluster'] >= 0]
    df2m = df2m[df2m['cluster'] >= 0]
    df2h = df2h[df2h['cluster'] >= 0]
    

    # Sigma normal
    sg2 = 1

    df_aux = []
    for df_name, df in [("low", df2l), ("mid", df2m), ("high", df2h)]:
        for cluster_id, group in df.groupby("cluster"):
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


# Processes and filters data for a given name

def data_treatment(name):
    
    df2, df = process_data(name)
    df4, df3 = filter_data(df2)

    return df4, df


#----  ETL FUNCTIONS  ---------------------------------------------------------------------------------------------

# SQLAlchemy base
Base = declarative_base()

# AircraftMetrics table definition
class AircraftMetrics(Base):
    __tablename__ = 'aircraft_metrics'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False) 
    timestamp_utc = Column(String, nullable=False)
    altitude_m = Column(Float, nullable=False)
    gs_kmh = Column(Float, nullable=False)
    tas_kmh = Column(Float, nullable=False)
    heading_diff_deg = Column(Float, nullable=False)

# Load JSON file
def load_json(path):
    """Load a JSON file and return its content."""
    with open(path, 'r') as f:
        return json.load(f)

# Write raw records to NoSQL TinyDB
def write_nosql(records, db_path="nosql_raw.json"):
    """Write records to a TinyDB JSON database."""
    db = TinyDB(db_path)
    db.truncate()
    db.insert_multiple(records)

# Transform raw records into structured format

def transform_records(records):
    """Transform raw JSON records into structured dictionary for SQL/NoSQL."""
    total = len(records)
    used = 0
    transformed = []
    
    for r in records:
        try:
            # Extract raw fields
            name = r.get("hex")
            timestamp_utc = r.get("now")
            altitude_m = r.get("alt_baro")
            gs_kmh = r.get("gs")
            tas_kmh = r.get("tas")
            mag_heading = r.get("mag_heading")
            true_heading = r.get("true_heading")

            # Skip incomplete records
            if None in [name, timestamp_utc, altitude_m, gs_kmh, tas_kmh, mag_heading, true_heading]:
                continue

            # Calculate heading difference
            heading_diff_deg = true_heading - mag_heading

            # Append transformed record
            transformed.append({
                "name": name,
                "timestamp_utc": timestamp_utc,
                "altitude_m": altitude_m,
                "gs_kmh": gs_kmh,
                "tas_kmh": tas_kmh,
                "heading_diff_deg": heading_diff_deg
            })
            used += 1
        except Exception:
            continue

    return transformed, total, used

# Write transformed records to SQL database

def write_sql(transformed_records, db_path="etl_output.db"):
    """Write transformed records to an SQLite database using SQLAlchemy."""
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.bulk_insert_mappings(AircraftMetrics, transformed_records)
    session.commit()
    session.close()

# Full ETL pipeline

def run_pipeline(path_json):
    """Run the full ETL pipeline: load JSON, write NoSQL, transform, and write SQL."""
    records = load_json(path_json)
    write_nosql(records)
    transformed, total, used = transform_records(records)
    write_sql(transformed)
    return {
        "total": total,
        "used": used,
        "db_path": "etl_output.db",
        "nosql_path": "nosql_raw.json"
    }
