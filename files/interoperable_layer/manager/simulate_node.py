from paho.mqtt import client as mqtt_client
import json
import numpy as np
import threading
import logging
from datetime import datetime

class SimulatedNode:
    def __init__(self, node_id, name, x, y, broker, port, topic):
        self.node_id = node_id
        self.name = name
        self.x = x
        self.y = y
        self.broker = broker
        self.port = port
        self.topic = topic
        self.client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1)
        self.client.on_message = self.on_message

    def connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            pass
        self.client.on_connect = on_connect
        self.client.connect(self.broker, self.port)
        return self.client

    def on_message(self, client, userdata, msg):
        message = msg.payload.decode()
        if message == 'get_data':
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
                "id": self.node_id,
                "name": self.name,
                "time": datetime.now().strftime("%H:%M:%S"),
                "date": datetime.now().strftime("%Y-%m-%d"),
                "x": self.x,
                "y": self.y,
                self.topic : values[self.topic]
            }
            self.client.publish(self.topic, json.dumps(data))

    def subscribe(self):
        self.client.subscribe(self.topic)

    def mqtt_subscriber(self):
        client = self.connect_mqtt()
        self.subscribe()
        client.loop_forever()

    def start_simulation(self):
        mqtt_thread = threading.Thread(target=self.mqtt_subscriber)
        mqtt_thread.daemon = True
        mqtt_thread.start()

    def stop_simulation(self):
        self.client.loop_stop()

if __name__ == '__main__':
    node = SimulatedNode("node1", "Node 1", 0, 0, "broker.hivemq.com", 1883, "iot")
    node.start_simulation()
