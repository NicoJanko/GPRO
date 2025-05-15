import requests
import psycopg2
import time
from utils import load_credentials,average_consumption

creds = load_credentials()

LANG = 'gb'
API_KEY = creds['api_key']
BASE_URL = f"https://gpro.net/{LANG}/backend/api/v2/"

HEADER = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

DB_URL = creds['database_url']

PARAMS = {

}

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

create_race_analysis_table_query = """
CREATE TABLE IF NOT EXISTS race_analysis (
    id SERIAL PRIMARY KEY,
    race_id TEXT NOT NULL,
    track_id INTEGER NOT NULL,
    car_pwr INTEGER NOT NULL,
    car_hdl INTEGER NOT NULL,
    car_acl INTEGER NOT NULL,
    setup_fwg INTEGER NOT NULL,
    setup_rwg INTEGER NOT NULL,
    setup_eng INTEGER NOT NULL,
    setup_brk INTEGER NOT NULL,
    setup_ger INTEGER NOT NULL,
    setup_sus INTEGER NOT NULL,
    driver_oal INTEGER NOT NULL,
    driver_con INTEGER NOT NULL,
    driver_tal INTEGER NOT NULL,
    driver_agr INTEGER NOT NULL,
    driver_exp INTEGER NOT NULL,
    driver_tei INTEGER NOT NULL,
    driver_sta INTEGER NOT NULL,
    driver_cha INTEGER NOT NULL,
    driver_mot INTEGER NOT NULL,
    driver_REP INTEGER NOT NULL,
    driver_wei INTEGER NOT NULL,
    start_fuel_L INTEGER NOT NULL,
    start_tyre_type TEXT NOT NULL,
    pit_1_lap INTEGER,
    tyre_cond_pit_1_perc DECIMAL,
    fuel_left_pit_1_L DECIMAL,
    pit_1_tyre_type TEXT,
    pit_1_fuel_L INTEGER,
    pit_2_lap INTEGER,
    tyre_cond_pit_2_perc DECIMAL,
    fuel_left_pit_2_L DECIMAL,
    pit_2_tyre_type TEXT,
    pit_2_fuel_L INTEGER,
    pit_3_lap INTEGER,
    tyre_cond_pit_3_perc DECIMAL,
    fuel_left_pit_3_L DECIMAL,
    pit_3_tyre_type TEXT,
    pit_3_fuel_L INTEGER,
    pit_4_lap INTEGER,
    tyre_cond_pit_4_perc DECIMAL,
    fuel_left_pit_4_L DECIMAL,
    pit_4_tyre_type TEXT,
    pit_4_fuel_L INTEGER,
    pit_5_lap INTEGER,
    tyre_cond_pit_5_perc DECIMAL,
    fuel_left_pit_5_L DECIMAL,
    pit_5_tyre_type TEXT,
    pit_5_fuel_L INTEGER,
    final_tyre_cond_perc DECIMAL,
    final_fuel_L DECIMAL,
    avg_dry_tyre_deg_perc_per_km DECIMAL NOT NULL,
    avg_fuel_cons_L_per_km DECIMAL NOT NULL
);
"""

cur.execute(create_race_analysis_table_query)
conn.commit()

insert_race_analysis_query = """
INSERT INTO race_analysis (
    race_id,
    track_id,
    car_pwr,
    car_hdl,
    car_acl,
    setup_fwg,
    setup_rwg,
    setup_eng,
    setup_brk,
    setup_ger,
    setup_sus,
    driver_oal,
    driver_con,
    driver_tal,
    driver_agr,
    driver_exp,
    driver_tei,
    driver_sta,
    driver_cha,
    driver_mot,
    driver_rep,
    driver_wei,
    start_fuel_L,
    start_tyre_type,
    pit_1_lap,
    tyre_cond_pit_1_perc,
    fuel_left_pit_1_L,
    pit_1_tyre_type,
    pit_1_fuel_L,
    pit_2_lap,
    tyre_cond_pit_2_perc,
    fuel_left_pit_2_L,
    pit_2_tyre_type,
    pit_2_fuel_L,
    pit_3_lap,
    tyre_cond_pit_3_perc,
    fuel_left_pit_3_L,
    pit_3_tyre_type,
    pit_3_fuel_L,
    pit_4_lap,
    tyre_cond_pit_4_perc,
    fuel_left_pit_4_L,
    pit_4_tyre_type,
    pit_4_fuel_L,
    pit_5_lap,
    tyre_cond_pit_5_perc,
    fuel_left_pit_5_L,
    pit_5_tyre_type,
    pit_5_fuel_L,
    final_tyre_cond_perc,
    final_fuel_L,
    avg_dry_tyre_deg_perc_per_km,
    avg_fuel_cons_L_per_km
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s
);
"""

races = []

response = requests.get(BASE_URL+'RaceAnalysis',headers=HEADER, params=PARAMS)
if response.status_code != 200:
    print(f"Error in first query with status : {response.status_code}")
else : 
    last_race_analysis = response.json()
    for race in reversed(last_race_analysis['racesToSelect']):
        races.append(race['value'])

for race_id in races:
    PARAMS['SR'] = race_id
    
    response = requests.get(BASE_URL+'RaceAnalysis',headers=HEADER, params=PARAMS)
    if response.status_code != 200:
        print(f"Error in first query with status : {response.status_code}")
    else:
        race_analysis = response.json()
        race_distance_query = f"""
    SELECT
        ti.id,
        ti.race_distance_km
    FROM calendar c
    JOIN tracks_info ti
        ON ti.id = c.race_{str(race_id.split(',')[1])}_id
    WHERE c.season = {race_id.split(',')[0]}
"""
        cur.execute(race_distance_query)
        results = cur.fetchall()[0]
        track_id = int(results[0])
        race_distance = float(results[1])
        avg_tyre, avg_float = average_consumption(race_analysis,race_distance)
        pits = race_analysis.get("pits", [])
        pit_1 = pits[0] if len(pits) > 0 else {}
        pit_2 = pits[1] if len(pits) > 1 else {}
        pit_3 = pits[2] if len(pits) > 2 else {}
        pit_4 = pits[3] if len(pits) > 3 else {}
        pit_5 = pits[4] if len(pits) > 4 else {}



        cur.execute(insert_race_analysis_query,(
            race_id,
            track_id,
            race_analysis.get("carPower"),#car_pwr,
            race_analysis.get("carHandl"),#car_hdl,
            race_analysis.get("carAccel"),#car_acl,
            race_analysis.get("setupsUsed")[2].get("setFWing"),#setup_fwg,
            race_analysis.get("setupsUsed")[2].get("setRWing"),#setup_rwg,
            race_analysis.get("setupsUsed")[2].get("setEng"),#setup_eng,
            race_analysis.get("setupsUsed")[2].get("setBra"),#setup_brk,
            race_analysis.get("setupsUsed")[2].get("setGear"),#setup_ger,
            race_analysis.get("setupsUsed")[2].get("setSusp"),#setup_sus,
            race_analysis.get("driver")['OA'],#driver_oal,
            race_analysis.get("driver")['con'],#driver_con,
            race_analysis.get("driver")['tal'],#driver_tal,
            race_analysis.get("driver")['agr'],#driver_agr,
            race_analysis.get("driver")['exp'],#driver_exp,
            race_analysis.get("driver")['tei'],#driver_tei,
            race_analysis.get("driver")['sta'],#driver_sta,
            race_analysis.get("driver")['cha'],#driver_cha,
            race_analysis.get("driver")['mot'],#driver_mot,
            race_analysis.get("driver")['rep'],#driver_rep,
            race_analysis.get("driver")['wei'],#driver_wei,
            race_analysis.get("startFuel"),#start_fuel_L,
            race_analysis.get("setupsUsed")[2].get("setTyres"),#start_tyre_type,
            pit_1.get("lap",0),#pit_1_lap,
            pit_1.get("tyreCond",0),#tyre_cond_pit_1_per,
            pit_1.get("fuelLeft",0)*1.8,#fuel_left_pit_1_L,
            race_analysis.get("laps")[int(pit_1.get("lap",-1))+1]["tyres"],#pit_1_tyre_type,
            pit_1.get("refilledTo",0),#pit_1_fuel_L,
            pit_2.get("lap",0),#pit_2_lap,
            pit_2.get("tyreCond",0),#tyre_cond_pit_2_per,
            pit_2.get("fuelLeft",0)*1.8,#fuel_left_pit_1_L,
            race_analysis.get("laps")[int(pit_2.get("lap",-1))+1]["tyres"],#pit_1_tyre_type,
            pit_2.get("refilledTo",0),#pit_1_fuel_L,
            pit_3.get("lap",0),#pit_3_lap,
            pit_3.get("tyreCond",0),#tyre_cond_pit_3_per,
            pit_3.get("fuelLeft",0)*1.8,#fuel_left_pit_3_L,
            race_analysis.get("laps",0)[int(pit_3.get("lap",-1))+1]["tyres"],#pit_3_tyre_type,
            pit_3.get("refilledTo",0),#pit_3_fuel_L,
            pit_4.get("lap",0),#pit_4_lap,
            pit_4.get("tyreCond",0),#tyre_cond_pit_4_per,
            pit_4.get("fuelLeft",0)*1.8,#fuel_left_pit_4_L,
            race_analysis.get("laps",0)[int(pit_4.get("lap",-1))+1]["tyres"],#pit_4_tyre_type,
            pit_4.get("refilledTo",0),#pit_4_fuel_L,
            pit_5.get("lap",0),#pit_5_lap,
            pit_5.get("tyreCond",0),#tyre_cond_pit_5_per,
            pit_5.get("fuelLeft",0)*1.8,#fuel_left_pit_5_L,
            race_analysis.get("laps")[int(pit_5.get("lap",-1))+1]["tyres"],#pit_5_tyre_type,
            pit_5.get("refilledTo",0),#pit_5_fuel_L,
            race_analysis.get("finishTyres"),#final_tyre_cond_per,
            race_analysis.get("finishFuel"),#final_fuel_L,
            avg_tyre,    #avg_dry_tyre_deg_per/km,
            avg_float    #avg_fuel_cons_L/km
        ))
        conn.commit()

        print(f"{race_id} done")
        time.sleep(2)

cur.close()
conn.close()
