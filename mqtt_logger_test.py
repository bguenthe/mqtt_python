import time
from datetime import datetime
import paho.mqtt.client as mqtt


class MqttLogger:
    def __init__(self):
        self.client = None

    def mqtt_init(self):
        self.client = mqtt.Client()
        self.client.connect("192.168.178.32", 1883, 60)  # raspberry 4GB docker

    def get_time(self):
        return datetime.now().strftime("%d.%m.%Y %H:%M:%S")

if __name__ == "__main__":
    connected = False
    mqtt_logger = MqttLogger()

    while not connected:
        try:
            mqtt_logger.mqtt_init()
            connected = True
        except Exception as e:
            print(mqtt_logger.get_time() + ": " + e.__str__())
            connected = False
            time.sleep(5)

    while True:
        mqtt_logger.client.publish("raspberry/howareyou")
        time.sleep(60)