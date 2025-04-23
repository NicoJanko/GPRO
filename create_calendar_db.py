import requests
import psycopg2
import time

API_KEY = 'eyJ0eXAiOiJKV1QiLCAiYWxnIjoiSFMyNTYifQ.eyJpZCI6IDEwNzk2MzksImNyZWF0ZWQiOiJXZWQgQXByIDE2IDE2OjU3OjEwIFVUQyswMjAwIDIwMjUifQ.UVvMjXDI_ce7Hh6fUomJfhJ2JF16k6IywcZa2SpfiqI'
LANG = 'gb'
BASE_URL = f"https://gpro.net/{LANG}/backend/api/v2/"

HEADER = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json"
}

DB_URL = ""


conn = psycopg2.connect(DB_URL)
cur = conn.cursor()


create_calendar_query = """
CREATE TABLE IF NOT EXISTS calendar (
    id SERIAL PRIMARY KEY,
    season INTEGER UNIQUE NOT NULL,
    "group" TEXT NOT NULL,
    test_id INTEGER NOT NULL,
    race_1_id INTEGER NOT NULL,
    race_2_id INTEGER NOT NULL,
    race_3_id INTEGER NOT NULL,
    race_4_id INTEGER NOT NULL,
    race_5_id INTEGER NOT NULL,
    race_6_id INTEGER NOT NULL,
    race_7_id INTEGER NOT NULL,
    race_8_id INTEGER NOT NULL,
    race_9_id INTEGER NOT NULL,
    race_10_id INTEGER NOT NULL,
    race_11_id INTEGER NOT NULL,
    race_12_id INTEGER NOT NULL,
    race_13_id INTEGER NOT NULL,
    race_14_id INTEGER NOT NULL,
    race_15_id INTEGER NOT NULL,
    race_16_id INTEGER NOT NULL,
    race_17_id INTEGER NOT NULL
);
"""

cur.execute(create_calendar_query)
conn.commit()


insert_calendar_query = """
INSERT INTO calendar (
    season,
    "group",
    test_id,
    race_1_id,
    race_2_id,
    race_3_id,
    race_4_id,
    race_5_id,
    race_6_id,
    race_7_id,
    race_8_id,
    race_9_id,
    race_10_id,
    race_11_id,
    race_12_id,
    race_13_id,
    race_14_id,
    race_15_id,
    race_16_id,
    race_17_id 
) VALUES (%s,%s,%s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (season) DO NOTHING;
"""

response = requests.get(BASE_URL+'office', headers=HEADER)
if response.status_code != 200:
    print(f"Status 401 for season")
else:
    season = int(response.json().get('seasonNb'))

response = requests.get(BASE_URL+'Calendar', headers=HEADER)
    
if response.status_code != 200:
    print(f"Status 401 for calendar")
else:
    calendar = response.json()
    full_calendar = {}
    full_calendar['test'] = calendar['testTrackId']
    group = calendar['group']

    for event in calendar['events']:
        if event['eventType'] == 'R':
            full_calendar[event['idx']] = int(event['trackId'])

    cur.execute(insert_calendar_query,(
                    season,
                    group,
                    full_calendar.get('test'),
                    full_calendar.get('1'),
                    full_calendar.get('2'),
                    full_calendar.get('3'),
                    full_calendar.get('4'),
                    full_calendar.get('5'),
                    full_calendar.get('6'),
                    full_calendar.get('7'),
                    full_calendar.get('8'),
                    full_calendar.get('9'),
                    full_calendar.get('10'),
                    full_calendar.get('11'),
                    full_calendar.get('12'),
                    full_calendar.get('13'),
                    full_calendar.get('14'),
                    full_calendar.get('15'),
                    full_calendar.get('16'),
                    full_calendar.get('17'),                    
                    ))
        
    conn.commit()

print(f"calendar done")
time.sleep(1)


cur.close()
conn.close()