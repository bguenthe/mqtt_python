import time
from datetime import datetime

import paho.mqtt.client as mqtt
import psycopg2


class MqttLogger:
    def __init__(self):
        self.client = None

    def on_connect(self, client, userdata, flags, rc):
        print(self.get_time() + ": " + "Connected with result code " + str(rc))

        topics = [("#", 0), ]

        client.subscribe(topics)
        print(self.get_time() + ": " + "Subscribed to: " + str(topics))

    def on_message(self, client, userdata, msg):
        print(self.get_time() + ": " + msg.topic + " " + str(msg.payload))

        try:
            self.cur.execute(
                """INSERT INTO mqtt_logger(topic, logtime, payload) VALUES (%s, %s, %s)""",
                (msg.topic, datetime.now(), msg.payload.decode(), None))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()

    def mqtt_init(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect("192.168.178.35", 1883, 60)  # odroid

    def db_init(self):
        self.conn = psycopg2.connect(database='mqtt', user='postgres', password='postgres', host='192.168.178.35')
        self.cur = self.conn.cursor()

    def get_time(self):
        return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


if __name__ == "__main__":
    connected = False
    mqtt_logger = MqttLogger()

    while not connected:
        try:
            mqtt_logger.mqtt_init()
            mqtt_logger.db_init()
            print("Connected")
            connected = True
            mqtt_logger.client.loop_forever()
        except Exception as e:
            print(mqtt_logger.get_time() + ": " + e.__str__())
            connected = False
            print("/nNot Connected. Try Again in 5 Seconds")
            time.sleep(5)
