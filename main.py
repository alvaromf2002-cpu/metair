from functions import *
import pandas as pd
import numpy as np
import json
import os
from sklearn.cluster import DBSCAN
from tinydb import TinyDB
from sqlalchemy import create_engine

# Select the aircraft data file:

# 000000Z.json
#     ...
# 230000Z.json

filepath = "data/000000Z.json"
print("filepath exists:", os.path.exists(filepath))

# pipeline (Extract, Transform, Load)
try:
    df4, df = data_treatment(filepath)
except Exception as e:
    print("Error in data_treatment:", e)

fromDFtoSQL(df, "Raw_SQL", existence=0)             # existence = 0 as database does not exist yet
fromDFtoSQL(df4, "Wind_SQL", existence=0)            # existence = 0 as database does not exist yet