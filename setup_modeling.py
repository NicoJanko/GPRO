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

select_consumption_learning_table_query = """
SELECT
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
    ti.overtaking,
    ra.car_pwr,
    ra.car_hdl,
    ra.car_acl,
    ra.driver_oal,
    ra.driver_con,
    ra.driver_tal,
    ra.driver_agr,
    ra.driver_exp,
    ra.driver_tei,
    ra.driver_wei,
    ra.setup_fwg,
    ra.setup_rwg,
    ra.setup_eng,
    ra.setup_brk,
    ra.setup_ger,
    ra.setup_sus
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


fwg = df['setup_fwg']
rwg = df['setup_rwg']
eng = df['setup_eng']
brk = df['setup_brk']
ger = df['setup_ger']
sus = df['setup_sus']


learn_df = df.drop(['setup_fwg','setup_rwg','setup_eng','setup_brk','setup_ger','setup_sus'],axis=1)

data_train, data_test, fwg_train, fwg_test, rwg_train, rwg_test,eng_train, eng_test, brk_train, brk_test,ger_train, ger_test, sus_train, sus_test, = train_test_split(learn_df,fwg, rwg,eng,brk,ger,sus,test_size=0.25)

#dfull = xgb.DMatrix(learn_df)
dtrain_fwg = xgb.DMatrix(data_train,fwg_train, enable_categorical=True)
dtrain_rwg = xgb.DMatrix(data_train,rwg_train, enable_categorical=True)
dtrain_eng = xgb.DMatrix(data_train,eng_train, enable_categorical=True)
dtrain_brk = xgb.DMatrix(data_train,brk_train, enable_categorical=True)
dtrain_ger = xgb.DMatrix(data_train,ger_train, enable_categorical=True)
dtrain_sus = xgb.DMatrix(data_train,sus_train, enable_categorical=True)




dtest_fwg = xgb.DMatrix(data_test,fwg_test, enable_categorical=True)
dtest_rwg = xgb.DMatrix(data_test,rwg_test, enable_categorical=True)
dtest_eng = xgb.DMatrix(data_test,eng_test, enable_categorical=True)
dtest_brk = xgb.DMatrix(data_test,brk_test, enable_categorical=True)
dtest_ger = xgb.DMatrix(data_test,ger_test, enable_categorical=True)
dtest_sus = xgb.DMatrix(data_test,sus_test, enable_categorical=True)

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

study_fwg = optuna.create_study(direction='minimize')
study_fwg.optimize(lambda trial: objective(trial, dtrain_fwg, dtest_fwg,fwg_test), n_trials=50)

study_rwg = optuna.create_study(direction='minimize')
study_rwg.optimize(lambda trial: objective(trial, dtrain_rwg, dtest_rwg,rwg_test), n_trials=50)

study_eng = optuna.create_study(direction='minimize')
study_eng.optimize(lambda trial: objective(trial, dtrain_eng, dtest_eng,eng_test), n_trials=50)

study_brk = optuna.create_study(direction='minimize')
study_brk.optimize(lambda trial: objective(trial, dtrain_brk, dtest_brk,brk_test), n_trials=50)

study_ger = optuna.create_study(direction='minimize')
study_ger.optimize(lambda trial: objective(trial, dtrain_ger, dtest_ger,ger_test), n_trials=50)

study_sus = optuna.create_study(direction='minimize')
study_sus.optimize(lambda trial: objective(trial, dtrain_sus, dtest_sus,sus_test), n_trials=50)
print(f"""
Best Tyre value : {study_fwg.best_value}
Best Tyre Param : {study_fwg.best_params}

""")
print(f"""
Best Fuel value : {study_rwg.best_value}
Best Fuel Param : {study_rwg.best_params}

""")
print(f"""
Best Fuel value : {study_eng.best_value}
Best Fuel Param : {study_eng.best_params}

""")
print(f"""
Best Fuel value : {study_brk.best_value}
Best Fuel Param : {study_brk.best_params}

""")
print(f"""
Best Fuel value : {study_ger.best_value}
Best Fuel Param : {study_ger.best_params}

""")
print(f"""
Best Fuel value : {study_sus.best_value}
Best Fuel Param : {study_sus.best_params}

""")

fwg_model = xgb.train(
    {**study_fwg.best_params, 'objective': 'reg:squarederror', 'eval_metric': 'rmse'},
    dtrain_fwg,
    num_boost_round=1000,
    evals=[(dtrain_fwg, 'train'), (dtest_fwg, 'eval')],
    early_stopping_rounds=50
)

fwg_model.save_model("fwg_model.json")

rwg_model = xgb.train(
    {**study_rwg.best_params, 'objective': 'reg:squarederror', 'eval_metric': 'rmse'},
    dtrain_rwg,
    num_boost_round=1000,
    evals=[(dtrain_rwg, 'train'), (dtest_rwg, 'eval')],
    early_stopping_rounds=50
)

rwg_model.save_model("rwg_model.json")

eng_model = xgb.train(
    {**study_eng.best_params, 'objective': 'reg:squarederror', 'eval_metric': 'rmse'},
    dtrain_eng,
    num_boost_round=1000,
    evals=[(dtrain_eng, 'train'), (dtest_eng, 'eval')],
    early_stopping_rounds=50
)

eng_model.save_model("eng_model.json")

brk_model = xgb.train(
    {**study_brk.best_params, 'objective': 'reg:squarederror', 'eval_metric': 'rmse'},
    dtrain_brk,
    num_boost_round=1000,
    evals=[(dtrain_brk, 'train'), (dtest_brk, 'eval')],
    early_stopping_rounds=50
)

brk_model.save_model("brk_model.json")

ger_model = xgb.train(
    {**study_ger.best_params, 'objective': 'reg:squarederror', 'eval_metric': 'rmse'},
    dtrain_ger,
    num_boost_round=1000,
    evals=[(dtrain_ger, 'train'), (dtest_ger, 'eval')],
    early_stopping_rounds=50
)

ger_model.save_model("ger_model.json")

sus_model = xgb.train(
    {**study_sus.best_params, 'objective': 'reg:squarederror', 'eval_metric': 'rmse'},
    dtrain_sus,
    num_boost_round=1000,
    evals=[(dtrain_sus, 'train'), (dtest_sus, 'eval')],
    early_stopping_rounds=50
)

sus_model.save_model("sus_model.json")