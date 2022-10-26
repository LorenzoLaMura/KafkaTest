import json
import datetime
import urllib.request
from kafka import KafkaProducer

API_KEY = '5c28e98285b61d63dc0069132031d887dc4afcaa'
url = f'https://api.jcdecaux.com/vls/v1/stations?apiKey={API_KEY}'
producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093'])
previous_stations = json.loads(urllib.request.urlopen(url).read().decode())

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    i, nb_stations = 0, 0
    for station in stations:
        if station['available_bikes'] == 0 and previous_stations[i]['available_bikes'] >= 1:
            station['empty'] = True
            producer.send('empty-stations', json.dumps(station).encode(), key=str(station['number']).encode())
            nb_stations += 1
        if station['available_bikes'] >= 1 and previous_stations[i]['available_bikes'] == 0:
            station['empty'] = False
            producer.send('empty-stations', json.dumps(station).encode(), key=str(station['number']).encode())
            nb_stations += 1
        i += 1
    previous_stations = stations
    print(f'{datetime.datetime.now()} Produced {nb_stations} station records')
