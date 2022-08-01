import time
from datetime import datetime
import paho.mqtt.client as mqtt
import json
import psycopg2


class MqttLogger:
    clientcosts = "monthlycosts/clientcosts"
    sendmeservercosts = "monthlycosts/sendmeservercosts"
    servercosts = "monthlycosts/servercosts"

    clientincome = "monthlycosts/clientincome"
    sendmeserverincome = "monthlycosts/sendmeserverincome"
    serverincome = "monthlycosts/serverincome"

    def __init__(self):
        self.client = None

    def on_connect(self, client, userdata, flags, rc):
        print(self.get_time() + ": " + "Connected with result code " + str(rc))

        client.subscribe(self.clientcosts)
        client.subscribe(self.sendmeservercosts)
        client.subscribe(self.clientincome)
        client.subscribe(self.sendmeserverincome)

        print(self.get_time() + ": " + "Subscribed to monthlycosts")

    def on_message(self, client, userdata, msg):
        if msg.topic == self.clientcosts:
            self.saveMontlycosts(msg)
        if msg.topic == self.clientincome:
            self.saveMontlyIncome(msg)
        if msg.topic == self.sendmeservercosts:
            self.sendServerCosts(msg)
        if msg.topic == self.sendmeserverincome:
            self.sendServerIncome(msg)

    def saveMontlycosts(self, msg):
        try:
            print(self.get_time() + ": " + msg.topic + " " + str(msg.payload.decode()))
            self.cur.execute(
                """INSERT INTO mqtt_logger(topic, logtime, payload) VALUES (%s, %s, %s)""",
                (msg.topic, datetime.now(), msg.payload.decode()))
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()

    def saveMontlyIncome(self, msg):
        try:
            print(self.get_time() + ": " + msg.topic + " " + str(msg.payload.decode()))
            self.cur.execute(
                """INSERT INTO mqtt_logger(topic, logtime, payload) VALUES (%s, %s, %s)""",
                (msg.topic, datetime.now(), msg.payload.decode()))
            self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()

    def sendServerCosts(self, msg):
        try:
            print(self.get_time() + ": " + msg.topic + " " + str(msg.payload.decode()))
            # NUR meine Daten vom Hardwarehandy
            sql = """SELECT payload from mqtt_logger where topic = '""" + self.clientcosts + "'" \
                  + """ and payload::json->>'manmod' = 'samsung_SM-G955F'"""
            self.cur.execute(sql)
            results = self.cur.fetchall()
            print(len(results))
            for result in results:
                self.client.publish(self.servercosts, result[0])
                print(self.get_time() + ": " + msg.topic + " " + result[0])
        except Exception as e:
            print(e)
            self.conn.rollback()

    def sendServerIncome(self, msg):
        try:
            print(self.get_time() + ": " + msg.topic + " " + str(msg.payload.decode()))
            sql = """SELECT payload from mqtt_logger where topic = '""" + self.clientincome + "'"\
                  + """ and payload::json->>'manmod' = 'samsung_SM-G955F'"""
            self.cur.execute(sql)
            results = self.cur.fetchall()
            print(len(results))
            for result in results:
                self.client.publish(self.serverincome, result[0])
                print(self.get_time() + ": " + msg.topic + " " + result[0])
        except Exception as e:
            print(e)
            self.conn.rollback()

    def mqtt_init(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect("192.168.178.32", 1883, 60)  # raspi 4GB

    def db_init(self):
        self.conn = psycopg2.connect(database='mqtt', user='postgres', password='postgres', host='192.168.178.32')
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
            print("Not Connected. Try Again in 5 Seconds")
            time.sleep(5)
