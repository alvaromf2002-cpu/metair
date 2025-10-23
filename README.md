# MétAir

This repository contains the deliverable of the **MétAir** project, developed as part of the **Data Engineering** module of the *Data & Decision Sciences Major* at **ISAE-SUPAERO**.  
The project aims to design an **ETL (Extract, Transform, Load)** pipeline capable of extracting real-time flight data from the **ADS-B Exchange API**, processing it to estimate high-altitude wind conditions, and storing the results in a structured database.


## Repository Structure

```
metair/
│
├── __pycache__/                      # Compiled Python cache files
├── data/                             # Raw JSON flight data from OpenSky
├── images/                           # Generated visuals (maps, plots, animations)
│   ├── output.png
│   └── wind_animation.gif
│
├── functions.py                      # Core ETL functions (extract, transform, load)
├── initial_database_from_json.db     # SQLite database generated from processed data
├── json_load.ipynb                   # Notebook for exploratory analysis
├── main.py                           # Main script that runs the ETL pipeline
├── ui_app.py                         # Graphical interface for data visualization
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```


## Project Overview

The **MétAir** pipeline follows a classic ETL structure:

### 1. Extraction  
Reads hourly JSON files obtained from the ADS-B Exchange API. These files contain ADS-B data transmitted by aircraft, including position, velocity, altitude, and heading.

### 2. Transformation  
Cleans the data, computes wind components (\( W_x, W_y, W \)) based on true airspeed, ground speed, and heading, and applies filtering using log-normal models and spatial clustering (DBSCAN).

### 3. Load  
Stores the processed data in a **SQLite** database using **SQLAlchemy**.  
Two key functions are used:

- `fromDFtoSQL(df, table_name, existence)` — writes or updates a SQL table from a DataFrame.  
- `fromSQLtoDF(table_name)` — reads an existing SQL table into a DataFrame for analysis and visualization.

---

## How to Run the Project

### 1. Clone the repository

```bash
git clone https://github.com/alvaromf2002-cpu/metair.git
cd metair
```

### 2. Create a virtual environment and install dependencies

```bash
python3 -m venv venv
source venv/bin/activate      # Linux / macOS
venv\Scripts\activate         # Windows
pip install -r requirements.txt
```

### 3. Run the ETL pipeline

```bash
python main.py
```

The script will process all JSON files in the `data/` directory, clean and transform the data, and generate a SQLite database containing the results.

### 4. Visualize the results

- Static maps are available in the `images/` folder (`output.png`).
- A temporal wind animation is provided as `wind_animation.gif`.
- The graphical interface (`ui_app.py`) can be used to explore the processed data interactively.

---

## Results

- Estimation of 3D wind vectors at different altitudes based on ADS-B flight data.  
- Geographic visualizations of flight paths, color-coded by time of day.  
- Histograms showing wind speed distributions after filtering.  
- A structured SQLite database containing all processed and validated flight data.



## Authors

- Ismael El Khattabi Vílchez  
- Ángel García Urbán  
- Alexandre Jesús de Oliveira  
- Álvaro Martínez Felipe  


## License

This project is released under the **MIT License** (or equivalent).  

Special thanks to the open-source community for the tools used:  
**pandas**, **numpy**, **matplotlib**, **cartopy**, **SQLAlchemy**, **scikit-learn**, and others.
