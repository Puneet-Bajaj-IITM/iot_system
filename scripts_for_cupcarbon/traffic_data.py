from paho.mqtt import client as mqtt_client
import json
import numpy as np
from datetime import datetime

current_datetime = datetime.now()

# Extract date and time components
date_str = current_datetime.strftime("%Y-%m-%d")
time_str = current_datetime.strftime("%H:%M:%S")

# Broker
broker = "broker.hivemq.com"
port = 1883

# Topic
topic = "traffic_data"

print("getid", flush=True)
id = input()
client_id = "***id****"+id

# Initialize MQTT client
client = mqtt_client.Client(client_id)


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!", flush=True)
        else:
            print("Failed to connect, return code", rc, flush=True)

    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def on_message(client, userdata, msg):
    message = msg.payload.decode()
    if message == 'get_data':
        print("getname", flush=True)
        name = input()
        print("getx", flush=True)
        x = input()
        print("gety", flush=True)
        y = input()
        pm2_5 = np.random.normal(22, 4)
        air_pollution = np.random.normal(50, 10)
        water_consumption = np.random.randint(100, 500)
        traffic_data = np.random.randint(0, 1000)
        temperature = np.random.normal(25, 5)
        values = {
            'pm2_5': pm2_5,
            'air_pollution': air_pollution,
            'water_consumption': water_consumption,
            'traffic_data': traffic_data,
            'temperature': temperature
        }

        data = {
            "id": id,
            "name": name,
            "x": x,
            "y": y, 
            "time": datetime.now().strftime("%H:%M:%S"),
            "date": datetime.now().strftime("%Y-%m-%d"),
            topic : values[topic]
        }
        client.publish(topic, json.dumps(data))



def subscribe(client: mqtt_client):
    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()
