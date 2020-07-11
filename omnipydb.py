from podsession import PodSession
import json
import sqlite3
import datetime as dt
import time
import glob


def get_pod_sessions(data_path: str, start_ts: float,
                        end_ts: float = None) -> list:

    pod_sessions = []

    if end_ts is None:
        end_ts = time.time() + 80*60*60

    for db_path in glob.glob(data_path + "\\*.db"):

        ts_pod_start = None
        ts_pod_end = None
        removed = None
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT MIN(timestamp), MAX(timestamp)"
                                  "FROM pod_history WHERE pod_state > 0 ORDER BY timestamp")
            row = cursor.fetchone()
            if row is not None:
                ts_pod_start = row[0]
                ts_pod_end = row[1]
                cursor.close()

                removed = not db_path.endswith("pod.db")

        if ts_pod_start is not None:
            if ts_pod_end > start_ts and ts_pod_start < end_ts:
                pod_sessions.append(get_pod_session(db_path, removed))

    return pod_sessions


def get_pod_session(db_path: str, auto_remove: bool = False) -> PodSession:
    jss = []
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT pod_json FROM pod_history WHERE pod_state >= 8 ORDER BY timestamp")
        for row in cursor:
            js = json.loads(row[0])
            jss.append(js)
        cursor.close()

    ps = PodSession()

    for js in jss:
        if ps.ended:
            break

        delivered = float(js["insulin_delivered"])
        not_delivered = float(js["insulin_canceled"])
        reservoir_remaining = float(js["insulin_reservoir"])
        ts = float(js["state_last_updated"])
        minute = int(js["state_active_minutes"])

        parameters = js["last_command"]
        command = parameters["command"]
        success = parameters["success"]

        if js["fault_event"]:
            pod_minute_failure = int(js["fault_event_rel_time"])
            log_event(f"FAULTED at minute {pod_minute_failure}", ts, minute, delivered, not_delivered,
                      reservoir_remaining)
            ps.fail(ts, minute, delivered, not_delivered, reservoir_remaining, pod_minute_failure)
        elif command == "START" and success:
            basal_rate = parameters["hourly_rates"][0]
            activation_date = js["var_activation_date"]
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
            log_event(f"BOLUS {not_delivered}", ts, minute, delivered, not_delivered, reservoir_remaining)
            ps.bolus_start(ts, minute, delivered, not_delivered, reservoir_remaining)
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
