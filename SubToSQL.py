import subscriber
import sqlite3
import json
import queue
import time

# MQTT Subscribe values
URL = "ec2-54-175-14-144.compute-1.amazonaws.com"
PORT = 8883
TOPIC = "test"
DEVICE_ID = "8052-IOT"
USERNAME = "saver"
PW = "3ed4rf5tg"

# SQL settings
SQL_FILE = 'pm_readings_db.sqlite'
global db
db = None
global cursor
cursor = None
payloads = queue.Queue()

# Init
global sub
sub = None


def pushData(data):
    payloads.put(data)


def read():
    print()
    while payloads.qsize() > 0:
        open_db()
        payload = json.loads(payloads.get())
        print(payload)
        date = payload['time']
        PM10 = payload['PM10']
        PM25 = payload['PM25']
        cursor.execute(
            '''INSERT INTO pm_reading(date, pm25, pm10) VALUES(?,?,?)''', (date, PM25, PM10))
        db.commit()
        print(cursor.execute('''SELECT * FROM pm_reading''').fetchall())


def open_db():
    global db, cursor
    db = sqlite3.connect(SQL_FILE)
    cursor = db.cursor()


def close_db():
    db.close()


def init_db():
    open_db()
    global db, cursor
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS pm_reading (id INTEGER PRIMARY KEY AUTOINCREMENT, date TIMESTAMP, pm25 INTEGER, pm10 INTEGER)''')
    db.commit()
    db.close()


if __name__ == "__main__":
    init_db()
    sub = subscriber.Subscriber(
        URL, PORT, DEVICE_ID, USERNAME, PW, pushData, TOPIC)
    sub.start()
    while(True):
        time.sleep(10)
        read()
