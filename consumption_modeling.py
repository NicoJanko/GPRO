
import psycopg2
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error
import numpy as np
import xgboost as xgb
import decimal
import pandas as pd
from utils import load_credentials
import optuna

creds = load_credentials()
DB_URL = creds['database_url']

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()
#took of race_distance & laps_distance

select_consumption_learning_table_query = """
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

cur.execute(select_consumption_learning_table_query)
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
df = pd.DataFrame(converted_dicts)
for col in df.select_dtypes(include=['object','string']).columns:
    df[col] = df[col].astype('category')

tyre_deg = df['avg_dry_tyre_deg_perc_per_km']
fuel_cons = df['avg_fuel_cons_l_per_km']
learn_df = df.drop(['avg_dry_tyre_deg_perc_per_km','avg_fuel_cons_l_per_km'],axis=1)

data_train, data_test, tyre_train, tyre_test, fuel_train, fuel_test = train_test_split(learn_df,tyre_deg, fuel_cons,test_size=0.25)

#dfull = xgb.DMatrix(learn_df)
dtrain_tyre = xgb.DMatrix(data_train,tyre_train, enable_categorical=True)
dtrain_fuel = xgb.DMatrix(data_train,fuel_train, enable_categorical=True)
dtest_tyre = xgb.DMatrix(data_test,tyre_test, enable_categorical=True)
dtest_fuel = xgb.DMatrix(data_test,fuel_test, enable_categorical=True)

def objective(trial, dtrain, dtest, y_test,num_boost=1000):
    param = {
        'objective':'reg:squarederror',
        'eval_metric':'rmse',
        'max_depth': trial.suggest_int('max_depth',1,10),
        'eta': trial.suggest_float('eta',0.01,0.3, log=True),
        'subsample':trial.suggest_float('subsample',0.5,1.0),
        'colsample_bytree':trial.suggest_float('colsample_bytree',0.5,1.0),
        'lambda':trial.suggest_float('lambda',1e-3,10.0,log=True),
        'alpha':trial.suggest_float('alpha',1e-3,10.0,log=True)
    }
    evallist = [(dtrain,'train'),(dtest,'eval')]
    model = xgb.train(
        param,
        dtrain,
        num_boost,
        evals = evallist,
        early_stopping_rounds = 50,
        verbose_eval=False
        
    )
    preds = model.predict(dtest, iteration_range = (0, model.best_iteration+1))
    rmse = root_mean_squared_error(y_test,preds)
    return rmse

study_tyre = optuna.create_study(direction='minimize')
study_tyre.optimize(lambda trial: objective(trial, dtrain_tyre, dtest_tyre,tyre_test), n_trials=50)

study_fuel = optuna.create_study(direction='minimize')
study_fuel.optimize(lambda trial: objective(trial, dtrain_fuel, dtest_fuel,fuel_test), n_trials=50)

print(f"""
Best Tyre value : {study_tyre.best_value}
Best Tyre Param : {study_tyre.best_params}

""")
print(f"""
Best Fuel value : {study_fuel.best_value}
Best Fuel Param : {study_fuel.best_params}

""")

tyre_model = xgb.train(
    {**study_tyre.best_params, 'objective': 'reg:squarederror', 'eval_metric': 'rmse'},
    dtrain_tyre,
    num_boost_round=1000,
    evals=[(dtrain_tyre, 'train'), (dtest_tyre, 'eval')],
    early_stopping_rounds=50
)

tyre_model.save_model("tyre_model.json")

fuel_model = xgb.train(
    {**study_fuel.best_params, 'objective': 'reg:squarederror', 'eval_metric': 'rmse'},
    dtrain_fuel,
    num_boost_round=1000,
    evals=[(dtrain_fuel, 'train'), (dtest_fuel, 'eval')],
    early_stopping_rounds=50
)

fuel_model.save_model("fuel_model.json")
