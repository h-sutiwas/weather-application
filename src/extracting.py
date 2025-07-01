import requests
import json
import logging

import pathlib
import datetime

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

locator = Nominatim(user_agent="myGeocoder")
geocode = RateLimiter(locator.geocode, min_delay_seconds=0.1)

def config_credentials():
    with open("config/config.json", "r") as config_file:
        config = json.load(config_file)

    with open("credentials/OpenWeatherAPI.json", "r") as weather_api_file:
        WEATHER_API = json.load(weather_api_file)["my_FirstAPI"]
    
    return config, WEATHER_API

def get_coordinates(city, country):
    response = geocode(query={
        "city": city,
        "country": country
    })
    return  {
        "latitude": response.latitude,
        "longitude": response.longitude
        }

def extract_weather_data(row, WEATHER_API):
    url = f'http://api.openweathermap.org/data/2.5/weather?lat={row.latitude}&lon={row.longitude}&appid={WEATHER_API}'
    try:
        response = requests.get(url)
        response.raise_for_status()

        logging.info(f"Weather data for {row.city}: Extraction Confirmed")
        response_json = response.json()
        sunset_utc = datetime.datetime.fromtimestamp(response_json['sys']['sunset'])
        return {
            "temp": response_json["main"]["temp"] - 273.15,
            "description": response_json["weather"][0]["description"],
            "icon": response_json["weather"][0]["icon"],
            "sunset_utc": sunset_utc,
            "sunset_local": sunset_utc + datetime.timedelta(seconds=response_json["timezone"])
        }
    
    except requests.RequestException as e:
        logger.error(f"Error fetching data for {row.city}: {e}")
        return None

if __name__ == '__main__':
    json_config, WEATHER_API = config_credentials()
    df = pd.DataFrame(json_config['cities'], columns=['city', 'country'])

    df_coordinates = df.apply(lambda x: get_coordinates(x.city, x.country), axis=1)
    df = pd.concat([df, pd.json_normalize(df_coordinates)], axis=1)

    df_weather = df.apply(lambda x: extract_weather_data(x, WEATHER_API), axis=1)
    df = pd.concat([df, pd.json_normalize(df_weather)], axis=1)

    src_dir = pathlib.Path(__file__).parent
    data_dir = src_dir.parent / 'data'

    # data_dir.mkdir(exist_ok=True)

    df.to_parquet(data_dir / "api_data.parquet", index=False)