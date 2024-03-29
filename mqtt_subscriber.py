import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    client.subscribe("testtopic/hello")
    client.subscribe("arduionotopic/counter")
    client.subscribe("arduionotopic/button")
    client.subscribe("arduionotopic/button_status")

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

#client.connect("lyk3nnyrasmj0efc.myfritz.net", 1883, 60)
client.connect("192.168.178.32", 1883, 60)

client.loop_forever()