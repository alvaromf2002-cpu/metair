import json
from tinydb import TinyDB
from sqlalchemy import create_engine, Column, String, Float, Integer, MetaData, Table
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
from dateutil import tz

# SQLAlchemy base
Base = declarative_base()

class AircraftMetrics(Base):
    #__tablename__ = 'aircraft_metrics'
    #id = Column(Integer, primary_key=True, autoincrement=True)
    # = Column(String, nullable=False)
    #timestamp_utc = Column(String, nullable=False)
    #altitude_m = Column(Float, nullable=False)
    #gs_kmh = Column(Float, nullable=False)
    #tas_kmh = Column(Float, nullable=False)
    #heading_diff_deg = Column(Float, nullable=False)

def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)

def write_nosql(records, db_path="nosql_raw.json"):
    db = TinyDB(db_path)
    db.truncate()
    db.insert_multiple(records)

def transform_records(records):
    total = len(records)
    used = 0
    transformed = []
    for r in records:
        try:
            now = r.get("now")
            hex_id = r.get("hex")
            alt_baro = r.get("alt_baro")
            gs = r.get("gs")
            tas = r.get("tas")
            track = r.get("track")
            mag_heading = r.get("mag_heading")
            true_heading = r.get("true_heading")

            # Validación
            if None in [now, hex_id, alt_baro, gs, tas, track, mag_heading, true_heading]:
                continue

            #============================== Transformación de datos =================================#
 











 

            transformed.append({
                "hex": hex_id,
                "timestamp_utc": timestamp_utc,
                "altitude_m": altitude_m,
                "gs_kmh": gs_kmh,
                "tas_kmh": tas_kmh,
                "heading_diff_deg": diff
            })
            used += 1
        except Exception:
            continue

            #========================================================================================#

    return transformed, total, used

def write_sql(transformed_records, db_path="etl_output.db"):
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.bulk_insert_mappings(AircraftMetrics, transformed_records)
    session.commit()
    session.close()

def run_pipeline(path_json):
    records = load_json(path_json)
    write_nosql(records)
    transformed, total, used = transform_records(records)
    write_sql(transformed)
    return {"total": total, "used": used, "db_path": "etl_output.db", "nosql_path": "nosql_raw.json"}
