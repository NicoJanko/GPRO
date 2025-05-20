import psycopg2
import decimal
import requests
import xgboost as xgb
import pandas as pd
from utils import load_credentials

creds = load_credentials()

LANG = 'gb'
API_KEY = creds['api_key']
BASE_URL = f"https://gpro.net/{LANG}/backend/api/v2/"

HEADER = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

DB_URL = creds['database_url']

PARAMS = {}

response = requests.get(BASE_URL+'DriProfile', headers=HEADER, params=PARAMS)
driver_data = response.json()

response = requests.get(BASE_URL+'Practice', headers=HEADER, params=PARAMS)
track_data = response.json()
track_id = track_data['trackId']

select_track_info_query = f"""
SELECT
    ti.track_name,
    ti.race_distance_km,
    ti.nb_laps,
    ti.laps_distance_km,
    ti.pit_stop_time_sec,
    ti.nb_turns,
    ti.power,
    ti.accel,
    ti.handl,
    ti.fuel_consumption,
    ti.tyre_wear,
    ti.downforce,
    ti.grip_level,
    ti.susp_rigid,
    ti.overtaking
FROM 
    tracks_info ti
WHERE
     ti.id = {track_id};

"""
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
cur.execute(select_track_info_query)
results = cur.fetchall()
column_names = [desc[0] for desc in cur.description]

converted_dicts = [
    {
        key: float(val) if isinstance(val, decimal.Decimal) else val
        for key, val in zip(column_names, row)
    }
    for row in results
]

cur.close()
conn.close()

converted_dicts[0]["car_pwr"] = track_data["carPower"]
converted_dicts[0]["car_hdl"] = track_data["carHandl"]
converted_dicts[0]["car_acl"] = track_data["carAccel"]
converted_dicts[0]["driver_oal"] = driver_data["overall"]
converted_dicts[0]["driver_con"] = driver_data["concentration"]
converted_dicts[0]["driver_tal"] = driver_data["talent"]
converted_dicts[0]["driver_agr"] = driver_data["aggressiveness"]
converted_dicts[0]["driver_exp"] = driver_data["experience"]
converted_dicts[0]["driver_tei"] = driver_data["techInsight"]
converted_dicts[0]["driver_wei"] = driver_data["weight"]

df = pd.DataFrame(converted_dicts)
track_name = df['track_name']
df = df.drop(['track_name'],axis=1)
for col in df.select_dtypes(include=['object','string']).columns:
    df[col] = df[col].astype('category')

ddata = xgb.DMatrix(df, enable_categorical=True)

setups = ['fwg','rwg','eng','brk','ger','sus']
setups_results = {}

for setup in setups:
    print(f'Predicting {setup} for {track_name}')
    model = xgb.Booster({'nthread': 4})
    model.load_model(f'{setup}_model.json')
    setups_results[setup] = model.predict(ddata, iteration_range = (0, model.best_iteration+1))

print(setups_results)