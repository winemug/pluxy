from datetime import time

from pymongo import CursorType, MongoClient
from pymongo.collection import Collection
from pymongo.cursor import Cursor

from podsession import PodSession
from settings import get_mongo_uri, get_db_name, get_ns_bg_collection_name, get_omnipy_entries_collection_name, \
    get_omnipy_pods_collection_name
import pandas as pd

def mongo_aggregate(coll: Collection, pipeline) -> []:
    return mongo_result(coll.aggregate(pipeline))


def mongo_find(coll: Collection, query, sort=None, projection=None) -> []:
    return mongo_result(coll.find(filter=query, sort=sort, projection=projection, cursor_type=CursorType.EXHAUST))


def mongo_result(cc: Cursor) -> []:
    ret = []
    with cc:
        for r in cc:
            ret.append(r)
    return ret



def get_bg_series(ts_start: float,
                  ts_end: float = None,
                  lowest_valid_bg: int = 40, highest_valid_bg: int = 400,
                  freq: str = 'T', max_fill: int = None,
                  include_manual_entries: bool = True) -> pd.Series:
    if ts_end is None:
        ts_end = time.time() + 2*60*60

    mongo_uri = get_mongo_uri()
    db_name = get_db_name()
    collection_name = get_ns_bg_collection_name()

    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        db_filter = {
            '$and': [
                {'date': {'$gte': int(ts_start * 1000)}},
                {'date': {'$lt': int(ts_end * 1000)}},
                {'$or': [
                    {'$and': [
                        {'sgv': {'$gte': lowest_valid_bg}},
                        {'sgv': {'$lte': highest_valid_bg}}
                    ]},
                    {'mbg': {'$ne': None}}
                ]},
            ]
        }

        project = {
            'date': 1,
            'sgv': 1,
            'mbg': 1,
            '_id': 0
        }

        coll = db.get_collection(collection_name)
        entries = mongo_find(coll, db_filter, projection=project, sort=[("date", 1)])
        df = pd.DataFrame(entries)

    index = pd.to_datetime(df['date'].convert_dtypes(convert_integer=True), unit='ms', utc=True)
    sgv = pd.Series(df['sgv'].array, index)
    if include_manual_entries and 'mbg' in df:
        mbg = pd.Series(df['mbg'].array, index)
        sgv = sgv.fillna(0) + mbg.fillna(0)
    else:
        sgv.dropna()

    return sgv.resample(freq).mean() \
        .interpolate(method='polynomial', order=2, limit_area='inside', limit=max_fill, limit_direction='backward')


def get_manual_injections(ts_start: float,
                          ts_end: float = None,
                          db_name: str = "nightscout", collection_name: str = "treatments") -> pd.Series:
    if ts_end is None:
        ts_end = time.time() + 2*60*60

    mongo_uri = get_mongo_uri()

    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        db_filter = {
            '$and': [
                {'date_field': {'$lte': ts_end*1000}},
                {'date_field': {'$gte': ts_start*1000}},
                {'insulin': {'$gt': 0}}
                ]}

        agg = [{'$addFields': {'date_field': {'$convert': {'input': {'$toDate': '$created_at'}, 'to': 'long'}}}},
               {'$match': db_filter}
               ]

        coll = db.get_collection(collection_name)
        entries = mongo_aggregate(coll, agg)
        if len(entries) == 0:
            return pd.Series()

        df = pd.DataFrame(entries)

    index = pd.to_datetime(df['date_field'], unit='ms', utc=True)
    return pd.Series(df['insulin'].array, index=index)


def get_pod_sessions(start_ts: float,
                        end_ts: float = None) -> list:

    pod_sessions = []

    mongo_uri = get_mongo_uri()

    with MongoClient(mongo_uri) as mongo_client:
        db = mongo_client.get_database("nightscout")
        coll = db.get_collection(get_omnipy_pods_collection_name())

        if end_ts is None:
            end_ts = time.time() + 80*60*60

        pods = mongo_find(coll, {
            'start': {'$lte': end_ts},
            '$or': [{'end': None}, {'end': {'$gte': start_ts}}],
        })

        coll = db.get_collection(get_omnipy_entries_collection_name())
        for pod in pods:
            pod_sessions.append(get_pod_session(pod["pod_id"], coll, pod["abandoned"]))

        return pod_sessions


def get_pod_session(pod_id: str, coll: Collection, auto_remove: bool = True) -> PodSession:
    pod_entries = mongo_find(coll,
                             {
                                'pod_id': pod_id,
                                'state_progress': { '$gte': 8 }
                             }, [('last_command_db_id', 1)])

    ps = PodSession()
    ps.id(pod_id)

    for pe in pod_entries:
        if ps.ended:
            break

        delivered = float(pe["insulin_delivered"])
        not_delivered = float(pe["insulin_canceled"])
        reservoir_remaining = float(pe["insulin_reservoir"])
        ts = float(pe["state_last_updated"])
        minute = int(pe["state_active_minutes"])

        parameters = pe["last_command"]
        command = parameters["command"]
        success = parameters["success"]

        if pe["fault_event"]:
            pod_minute_failure = int(pe["fault_event_rel_time"])
            log_event(f"FAULTED at minute {pod_minute_failure}", ts, minute, delivered, not_delivered,
                      reservoir_remaining)
            ps.fail(ts, minute, delivered, not_delivered, reservoir_remaining, pod_minute_failure)
        elif command == "START" and success:
            basal_rate = parameters["hourly_rates"][0]
            activation_date = pe["var_activation_date"]
            log_event(f"START {basal_rate}U/h", ts, minute, delivered, not_delivered, reservoir_remaining)
            ps.start(ts, minute, delivered, not_delivered, reservoir_remaining, basal_rate, activation_date)
        elif command == "TEMPBASAL" and success:
            tb_duration_hours = float(parameters["duration_hours"])
            tb_minutes = int(round(tb_duration_hours * 60, 0))
            tb_rate = float(parameters["hourly_rate"])
            log_event(f"TEMPBASAL {tb_rate}U/h {tb_duration_hours}h", ts, minute, delivered, not_delivered,
                      reservoir_remaining)
            ps.temp_basal_start(ts, minute, delivered, not_delivered, reservoir_remaining, tb_rate, tb_minutes)
        elif command == "TEMPBASAL_CANCEL" and success:
            log_event(f"TEMPBASAL CANCEL", ts, minute, delivered, not_delivered, reservoir_remaining)
            ps.temp_basal_end(ts, minute, delivered, not_delivered, reservoir_remaining)
        elif command == "BOLUS" and success:
            if "interval" in parameters:
                p_i = parameters["interval"]
            else:
                p_i = 2
            log_event(f"BOLUS {not_delivered} interval {p_i}", ts, minute, delivered, not_delivered, reservoir_remaining)
            ps.bolus_start(ts, minute, delivered, not_delivered, reservoir_remaining, p_i)
        elif command == "BOLUS_CANCEL" and success:
            log_event(f"BOLUS CANCEL", ts, minute, delivered, not_delivered, reservoir_remaining)
            ps.bolus_end(ts, minute, delivered, not_delivered, reservoir_remaining)
        elif command == "DEACTIVATE" and success:
            log_event(f"DEACTIVATE", ts, minute, delivered, not_delivered, reservoir_remaining)
            ps.deactivate(ts, minute, delivered, not_delivered, reservoir_remaining)
        elif success:
            log_event(f"STATUS", ts, minute, delivered, not_delivered, reservoir_remaining)
            ps.entry(ts, minute, delivered, not_delivered, reservoir_remaining)

    if not ps.ended and auto_remove:
        ps.remove()

    return ps


def log_event(msg: str, ts: float, minute: int,
              total_delivered: float, total_undelivered: float, reservoir_remaining: float):
    pass
    #print(f"{dt.datetime.fromtimestamp(ts)} {minute} {msg}\t{total_delivered} {total_undelivered} {reservoir_remaining}")


