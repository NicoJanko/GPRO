import requests
import psycopg2
import time
from utils import load_credentials

creds = load_credentials()

DB_URL = creds['database_url']

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

select_consumption_learning_table_query = """
SELECT
    ti.race_distance_km,
    ti.nb_laps,
    ti.lap_distance_km,
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
    ti.overtaking,
    ra.car_pwr,
    ra.car_hdl,
    ra.car_acl,
    ra.setup_fwg,
    ra.setup_rwg,
    ra.setup_eng,
    ra.setup_brk,
    ra.setup_ger,
    ra.setup_sus,
    ra.driver_oal,
    ra.driver_con,
    ra.driver_tal,
    ra.driver_agr,
    ra.driver_exp,
    ra.driver_tei,
    ra.driver_wei,
    CASE 
        WHEN ra.start_tyre_type != 'Rain'
        THEN ra.start_tyre_type
        ELSE ra.pit_2_tyre_type
    END AS dry_tyre,
    ra.avg_dry_tyre_deg_perc_per_km,
    ra.avg_fuel_cons_l_per_km
FROM 
    race_analysis ra
JOIN
    tracks_info ti ON ti.id = ra.track_id;

"""