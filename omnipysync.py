import ssl
from threading import Thread
from enum import Enum
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from podsession import PodSession
import simplejson as json
import sqlite3
import datetime as dt
import time
import glob
import paho.mqtt.client as mqtt
import logging


class OmniPySync:

    def __init__(self):
        with open("settings.json", "r") as stream:
            self.settings = json.load(stream)
        self.logger = logging.getLogger("omnipy_sync")
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # fh = logging.FileHandler(DATA_PATH + OMNIPY_LOGFILE + LOGFILE_SUFFIX)
        # fh.setLevel(logging.DEBUG)
        # fh.setFormatter(formatter)
        # logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self.mqtt_client = mqtt.Client(client_id=self.settings["mqtt_client_id"], protocol=mqtt.MQTTv311)
        ssl_ctx = ssl.create_default_context()
        self.mqtt_client.tls_set_context(ssl_ctx)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_message = self.on_message

        self.known_pods = dict()

        # with MongoClient(self.settings["mongo_uri"]) as mongo_client:
        #     db = mongo_client.get_database("nightscout")
        #     coll = db.get_collection("omnipy")
        #     coll.remove({})

    def run(self):

        connected = False

        while not connected:
            try:
                self.mqtt_client.connect(self.settings["mqtt_host"],
                                         self.settings["mqtt_port"], clean_start=mqtt.MQTT_CLEAN_START_FIRST_ONLY)
                connected = True
            except:
                time.sleep(30)

        self.mqtt_client.loop_forever(retry_first_connection=True)

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        self.logger.info("Connected to mqtt")
        client.subscribe("omnipy_json", qos = 1)
        client.publish("omnipy_sync_request", "latest", qos = 1)

    def on_message(self, client, userdata, message: mqtt.MQTTMessage):
        self.logger.info(f"Incoming message:\nTs:{message.timestamp} Topic: {message.topic} Message: {message.payload}")
        if message.topic == "omnipy_json":
            self.add_json_response(message.payload.decode())
        elif message.topic == "omnipy_status":
            js = json.loads(message.payload.decode())

    def on_disconnect(self, client, userdata, rc):
        self.logger.info("Disconnected")

    def direct_import(self, data_path: str):
        with MongoClient(self.settings["mongo_uri"]) as mongo_client:
            db = mongo_client.get_database("nightscout")
            coll = db.get_collection("omnipy")
            for db_path in glob.glob(data_path + "\\*.db"):
                try:
                    self.logger.info(f'Importing from {db_path}')
                    with sqlite3.connect(db_path) as conn:
                        cursor = conn.execute("SELECT pod_json FROM pod_history WHERE pod_state > 0 LIMIT 1")
                        row = cursor.fetchone()
                        if row is not None:
                            js = json.loads(row[0])
                            if "pod_id" not in js or js["pod_id"] is None:
                                pod_id = "L" + str(js["id_lot"]) + "T" + str(js["id_t"])
                            else:
                                pod_id = js["pod_id"]
                        cursor.close()

                        id_entries = coll.find({ 'pod_id': pod_id }, { 'last_command_db_id': 1 })
                        id_list = []
                        for e in id_entries:
                            id_list.append(e['last_command_db_id'])

                        cursor = conn.execute("SELECT rowid, timestamp, pod_json FROM pod_history")
                        for row in cursor.fetchall():
                            if row[0] in id_list:
                                continue
                            js = json.loads(row[2])
                            js["pod_id"] = pod_id
                            js["last_command_db_id"] = row[0]
                            js["last_command_db_ts"] = row[1]
                            coll.insert_one(js)

                except Exception as e:
                    self.logger.warning(f'Skipping {db_path} due error: {e}')

    def add_json_response(self, str_json: str, record_tracking: bool = True):
        jsn = json.loads(str_json)
        pod_id = jsn["pod_id"]
        if pod_id is None:
            return

        sqlite_id = jsn["last_command_db_id"]
        if sqlite_id is None:
            return

        with MongoClient(self.settings["mongo_uri"]) as mongo_client:
            db = mongo_client.get_database("nightscout")
            coll = db.get_collection("omnipy")

            db_filter = {
                '$and': [
                    {'pod_id': pod_id},
                    {'last_command_db_id': sqlite_id},
                    ]
                }

            if coll.find(db_filter).count() == 0:
                try:
                    coll.insert_one(jsn)
                except DuplicateKeyError:
                    pass

            if record_tracking:
                if pod_id in self.known_pods:
                    pod_data = self.known_pods[pod_id]
                else:
                    db_filter = {'pod_id': pod_id}
                    project = {
                        'last_command_db_id': 1
                    }

                    cursor = coll.find(db_filter, project).sort([("last_command_db_id", 1)])
                    known_ids = []
                    for row in cursor:
                        known_ids.append(int(row["last_command_db_id"]))
                    cursor.close()

                    pod_data = PodData(pod_id, known_ids)
                    self.known_pods[pod_id] = pod_data

                requested_ids = pod_data.received_id(sqlite_id)

                if len(requested_ids) > 0:
                    self.mqtt_client.publish(topic="omnipy_sync_request", payload=f"{pod_id} {' '.join(list(map(str, requested_ids)))}", qos=1)
                    self.logger.info(f"requested ids: {requested_ids}")


class PodData:
    def __init__(self, pod_id: str, known_ids: []):
        self.pod_id = pod_id
        self.rowids = known_ids
        self.requested_rowids = []

    def received_id(self, rowid: int) -> []:
        if rowid not in self.rowids:
            self.rowids.append(rowid)
            self.rowids.sort()

            if rowid in self.requested_rowids:
                self.requested_rowids.remove(rowid)

        new_requests = []
        idx = 1
        for existing_id in self.rowids:
            if existing_id == idx:
                idx += 1
                continue

            while existing_id > idx:
                if idx not in self.requested_rowids:
                    new_requests.append(idx)
                    self.requested_rowids.append(idx)
                idx += 1

        return new_requests


if __name__ == '__main__':
    s = OmniPySync()
    s.direct_import('C:\\Users\\kurtl\\OneDrive\\_data\\omnipy\\data')
    s.run()
