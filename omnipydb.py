from pymongo import MongoClient
from pymongo.collection import Collection

from podsession import PodSession
import json
import pymongo
import datetime as dt
import time
import glob
from bson.son import SON

def get_pod_sessions(mongo_uri: str, start_ts: float,
                        end_ts: float = None) -> list:

    pod_sessions = []

    with MongoClient(mongo_uri) as mongo_client:
        db = mongo_client.get_database("nightscout")
        coll = db.get_collection("omnipy")

        if end_ts is None:
            end_ts = time.time() + 80*60*60

        earliest = coll.find({'state_last_updated': {'$lte': start_ts}}).sort([('state_last_updated', -1)])[0]['last_command_db_ts']
        latest = coll.find({'state_last_updated': {'$lte': end_ts}}).sort([('state_last_updated', -1)])[0]['last_command_db_ts']

        agg = coll.aggregate([
            {'$sort':
                SON([('last_command_db_ts', -1)])
            },
            {'$match':
                 {'last_command_db_ts': {'$gte': earliest, '$lte': latest}}
            },
            {'$group':
                {'_id': '$pod_id'}
            }
            ])

        remove = False
        for ag in agg:
            pod_sessions.append(get_pod_session(ag["_id"], coll, remove))
            remove = True

        return sorted(pod_sessions, key=lambda ps: ps.activation_ts)


def get_pod_session(pod_id: str, coll: Collection, auto_remove: bool = True) -> PodSession:
    pod_entries = coll.find({
        'pod_id': pod_id,
        'state_progress': { '$gte': 8 }
                }).sort([('last_command_db_id', 1)])

    ps = PodSession()

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
    print(
        f"{dt.datetime.fromtimestamp(ts)} {minute} {msg}\t{total_delivered} {total_undelivered} {reservoir_remaining}")
