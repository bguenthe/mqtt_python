import json
import platform
import time
from datetime import datetime

import psutil
import paho.mqtt.client as mqtt

client = None


def on_connect(client, userdata, flags, rc):
    print(get_time() + ": " + "Connected with result code " + str(rc))

    topics = [("/raspberry/howareyou", 0), ]

    client.subscribe(topics)
    print(get_time() + ": " + "Subscribed to: " + str(topics))


# Einkommende Kommandos, die für den Arduino bestimmt sind
def on_message(client1, userdata, msg):
    global client
    print(get_time() + ": " + msg.topic + " " + str(msg.payload))
    message = msg.payload.decode()
    if msg.topic == "/raspberry/howareyou":
        comp = {}
        comp["cpu_count"] = str(psutil.cpu_count())
        comp["cpu_percent"] = str(psutil.cpu_percent())
        comp["os_name"] = str(platform.uname())
        client.publish("/raspberry/healthstatus", json.dumps(comp))


def mqtt_init():
    global client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    # client.connect("raspberrypi-lan.lyk3nnyrasmj0efc.myfritz.net", 1883, 60)
    client.connect("192.168.178.113", 1883, 60)  # proxmox


def get_time():
    return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


if __name__ == "__main__":
    connected = False
    while not connected:
        try:
            mqtt_init()
            connected = True
            client.loop_forever()
        except Exception as e:
            print(get_time() + ": " + e.__str__())
            connected = False
            time.sleep(5)
