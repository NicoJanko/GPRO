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
converted_dicts[0]["setup_fwg"] = track_data["setFWing"]
converted_dicts[0]["setup_rwg"] = track_data["setRWing"]
converted_dicts[0]["setup_eng"] = track_data["setEngine"]
converted_dicts[0]["setup_brk"] = track_data["setBrakes"]
converted_dicts[0]["setup_ger"] = track_data["carHandl"]
converted_dicts[0]["setup_sus"] = track_data["setGear"]
converted_dicts[0]["driver_oal"] = driver_data["overall"]
converted_dicts[0]["driver_con"] = driver_data["concentration"]
converted_dicts[0]["driver_tal"] = driver_data["talent"]
converted_dicts[0]["driver_agr"] = driver_data["aggressiveness"]
converted_dicts[0]["driver_exp"] = driver_data["experience"]
converted_dicts[0]["driver_tei"] = driver_data["techInsight"]
converted_dicts[0]["driver_wei"] = driver_data["weight"]

df = pd.DataFrame(converted_dicts)
eso_df = df.copy()
sof_df = df.copy()
med_df = df.copy()
har_df = df.copy()

eso_df['dry_tyre'] = 'Extra Soft'
sof_df['dry_tyre'] = 'Soft'
med_df['dry_tyre'] = 'Medium'
har_df['dry_tyre'] = 'Hard'

tyre_categories = pd.CategoricalDtype(categories=["Extra Soft", "Soft", "Medium", "Hard"], ordered=False)

for df_variant in [eso_df, sof_df, med_df, har_df]:
    df_variant['dry_tyre'] = df_variant['dry_tyre'].astype(tyre_categories)

for col in eso_df.select_dtypes(include=['object','string']).columns:
    eso_df[col] = eso_df[col].astype('category')

for col in sof_df.select_dtypes(include=['object','string']).columns:
    sof_df[col] = sof_df[col].astype('category')

for col in med_df.select_dtypes(include=['object','string']).columns:
    med_df[col] = med_df[col].astype('category')

for col in har_df.select_dtypes(include=['object','string']).columns:
    har_df[col] = har_df[col].astype('category')

print(eso_df['dry_tyre'].dtype, eso_df['dry_tyre'].cat.categories)
print(sof_df['dry_tyre'].dtype, sof_df['dry_tyre'].cat.categories)
print(med_df['dry_tyre'].dtype, med_df['dry_tyre'].cat.categories)
print(har_df['dry_tyre'].dtype, har_df['dry_tyre'].cat.categories)
tyre_dmat = {}
tyre_dmat['Extra_Soft'] = xgb.DMatrix(eso_df, enable_categorical=True)
tyre_dmat['Soft'] = xgb.DMatrix(sof_df, enable_categorical=True)
tyre_dmat['Medium'] = xgb.DMatrix(med_df, enable_categorical=True)
tyre_dmat['Hard'] = xgb.DMatrix(har_df, enable_categorical=True)

tyre_model = xgb.Booster({'nthread': 4})
tyre_model.load_model(f'tyre_model.json')
fuel_model = xgb.Booster({'nthread': 4})
fuel_model.load_model(f'fuel_model.json')
full_results = {}

for tyre in tyre_dmat.keys():
    results = {}
    results['Fuel'] = fuel_model.predict(tyre_dmat[tyre],iteration_range = (0, fuel_model.best_iteration+1))
    results['Tyre_Deg'] = tyre_model.predict(tyre_dmat[tyre],iteration_range = (0, tyre_model.best_iteration+1))
    full_results[tyre] = results

for tyre,results in full_results.items():
    print(f"""
{tyre} : {results}
""") 