import json
import requests
import boto3
from datetime import datetime
from decimal import Decimal



API_URL = "http://api.weatherapi.com/v1/current.json"
API_KEY = "befd1e30711b490f954161502240105"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('weather_table')


def get_weather_data(city):
    params = {
        "q": city,
        "key": API_KEY
    }
    response = requests.get(API_URL, params=params)
    data = response.json()
    return data


def lambda_handler(event, context):
    # Get Data from Weather API
    cities = ["Charlotte", "Raleigh", "Chicago", "Atlanta", "Los Angeles", "New York", "Boston", "San Francisco", "San Diego", "Dallas"]
    for city in cities:
        data = get_weather_data(city)

        temp = data['current']['temp_f']
        wind_speed = data['current']['wind_mph']
        wind_dir = data['current']['wind_dir']
        pressure_in = data['current']['pressure_in']
        precip_in = data['current']['precip_in']
        humidity = data['current']['humidity']
        visibility = data['current']['vis_miles']
        uv = data['current']['uv']
        current_timestamp = datetime.utcnow().isoformat()

        print(city, temp, wind_speed, wind_dir, pressure_in, precip_in, humidity, visibility, uv)

        # Insert data into DynamoDB
        item = {
            'city': city,                                                                                                # Partition Key
            'time': str(current_timestamp),                                                                              # Sort Key
            'temp': temp,
            'wind_speed': wind_speed,
            'wind_dir': wind_dir,
            'pressure_in': pressure_in,
            'precip_in': precip_in,
            'humidity': humidity,
            'visibility': visibility,
            'uv': uv
        }
        item = json.loads(json.dumps(item), parse_float=Decimal)
        table.put_item(Item=item)