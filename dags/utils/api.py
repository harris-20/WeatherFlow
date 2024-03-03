import random
import requests
from typing import List

from airflow.models import Variable

from utils.database import MySQLHandler


def fetch_data() -> List[dict]:
    keys = [Variable.get("API_KEY1"), Variable.get("API_KEY2")]

    if not keys:
        raise EnvironmentError("No API keys found")
    
    api_key = random.choice(keys)

    cxn = MySQLHandler()
    cxn.connect()
    stations = cxn.execute("SELECT station_id, city, country_code FROM W_STATION WHERE enabled = 1;")
    cxn.close()

    to_return = []
    for _, row in stations.iterrows():
        station_id, city, country_code = row["station_id"], row["city"], row["country_code"]
        resp = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city},{country_code}&appid={api_key}&units=metric")
        
        resp = resp.json()

        to_return.append({"station_id": station_id, "temp": resp["main"]["temp"], "temp_min": resp["main"]["temp_min"], "temp_max": resp["main"]["temp_max"],
                "feels_like": resp["main"]["feels_like"], "wind_speed": resp["wind"]["speed"], "wind_direction": resp["wind"]["deg"],
                "pressure": resp["main"]["pressure"], "humidity": resp["main"]["humidity"], "visibility": resp["visibility"], 
                "cloudiness": resp["clouds"]["all"], "timestamp": resp["dt"], "sunrise_ts": resp["sys"]["sunrise"], 
                "sunset_ts": resp["sys"]["sunset"], "timezone": resp["timezone"]})
    return to_return
