import json
from kafka import KafkaConsumer

stations = {}
consumer = KafkaConsumer('empty-stations', bootstrap_servers=['localhost:9092', 'localhost:9093'],
                         group_id='monitor-stations')

for message in consumer:
    station = json.loads(message.value.decode())
    if station['empty']:
        print(f"The Station of {station['contract_name']} {station['name']} is empty is this moment !")
