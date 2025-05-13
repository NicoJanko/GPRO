import requests
import psycopg2
import time

from utils import load_credentials

creds = load_credentials()

API_KEY = creds['api_key']
LANG = 'gb'
BASE_URL = f"https://gpro.net/{LANG}/backend/api/v2/"

HEADER = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

DB_URL = creds['database_url']


conn = psycopg2.connect(DB_URL)
cur = conn.cursor()


create_track_table_query = """
CREATE TABLE IF NOT EXISTS tracks_info (
    id SERIAL PRIMARY KEY,
    track_name TEXT NOT NULL,
    race_distance_km DECIMAL,
    nb_laps INTEGER,
    laps_distance_km DECIMAL,
    pit_stop_time_sec DECIMAL,
    nb_turns INTEGER,
    power INTEGER,
    accel INTEGER,
    handl INTEGER,
    fuel_consumption TEXT,
    tyre_wear TEXT,
    downforce TEXT,
    grip_level TEXT,
    susp_rigid TEXT,
    overtaking TEXT
);
"""

cur.execute(create_track_table_query)
conn.commit()


insert_track_info_query = """
INSERT INTO tracks_info (
    id,
    track_name,
    race_distance_km,
    nb_laps,
    laps_distance_km,
    pit_stop_time_sec,
    nb_turns,
    power,
    accel,
    handl,
    fuel_consumption,
    tyre_wear,
    downforce,
    grip_level,
    susp_rigid,
    overtaking
) VALUES (%s,%s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""

for id in range(0,80):
    PARAMS = {
    "id":id
}

    response = requests.get(BASE_URL+'TrackProfile', headers=HEADER, params=PARAMS)
    
    if response.status_code != 200:
        print(f"Status 401 for id : {id}")
    else:
        track_info =response.json()
        cur.execute(insert_track_info_query,(
                    id,
                    track_info.get('trackName'),
                    track_info.get('raceDistance'),
                    track_info.get('laps'),
                    track_info.get('lapDistance'),
                    track_info.get('timeInOutPits'),
                    track_info.get('nbTurns'),
                    track_info.get('power'),
                    track_info.get('accel'),
                    track_info.get('handl'),
                    track_info.get('fuelConsumption'),
                    track_info.get('tyreWear'),
                    track_info.get('downforce'),
                    track_info.get('gripLevel'),
                    track_info.get('suspRigidity'),
                    track_info.get('overtaking')                    
                    ))
        
        conn.commit()

        print(f"{track_info.get('trackName')}:{id} done")
        time.sleep(5)


cur.close()
conn.close()