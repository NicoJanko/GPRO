import json
from typing import Tuple

def load_credentials(filepath="cred.cred"):
    with open(filepath, "r") as f:
        creds = json.load(f)
    return creds

def average_consumption(race_analysis: json, race_lenght:float) -> Tuple[float,float]:
    number_pits = len(race_analysis['pits'])
    number_laps = race_analysis['laps'][-1]['idx']
    lap_distance = race_lenght/number_laps
    total_tyre = 0
    total_fuel = 0
    race_lenght_dry = race_lenght
    for n in range(number_pits):
        if race_analysis['laps'][race_analysis.get("pits")[n]['lap']]['tyres']!='Rain':
            total_tyre += (100-race_analysis.get("pits")[n].get("tyreCond"))
        else:
            race_lenght_dry += -race_analysis.get("pits")[n]['lap']*lap_distance
        if n ==0:
            total_fuel += (race_analysis.get("startFuel")-race_analysis.get("pits")[n].get("fuelLeft")*1.8)
        else:
            total_fuel += (race_analysis.get("pits")[n-1].get("refilledTo")-race_analysis.get("pits")[n].get("fuelLeft")*1.8)
    total_tyre += (100-race_analysis.get("finishTyres"))
    total_fuel += (race_analysis.get("pits")[-1].get("refilledTo")-race_analysis.get("finishFuel"))

    avg_tyre = round(total_tyre/race_lenght_dry,3)
    avg_fuel = round(total_fuel/race_lenght,3)

    return avg_tyre, avg_fuel