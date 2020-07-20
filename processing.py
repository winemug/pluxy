import time
import pandas as pd
import json
import datetime as dt
from scipy import signal

from insulinmodel import get_insulin_model2, get_insulin_model
from nsmongodb import get_bg_series
from omnipydb import get_pod_sessions
from podsession import PodSession


def get_processed_data(hours_prev: int, hours_next: int,
                       weight: float, height: float):
    with open("settings.json", "r") as stream:
        settings = json.load(stream)

    ts_now = time.time()
    ts_now -= ts_now % 60
    ts_start = ts_now - (hours_prev + 24) * 60 * 60
    ts_clip = ts_now - hours_prev * 60 * 60
    ts_end = ts_now + hours_next * 60 * 60
    pd_dts = pd.to_datetime([ts_start, ts_clip, ts_now, ts_end], unit='s', origin='unix', utc=True)

    index_start_to_end = pd.date_range(pd_dts[0], pd_dts[3], freq='T')
    index_start_to_now = pd.date_range(pd_dts[0], pd_dts[2], freq='T')
    index_now_to_end = pd.date_range(pd_dts[2], pd_dts[3], freq='T')

    index_clip_to_now = pd.date_range(pd_dts[1], pd_dts[2], freq='T')
    index_clip_to_end = pd.date_range(pd_dts[1], pd_dts[3], freq='T')

    ts_bg = get_bg_series(settings["mongo_uri"], ts_start, ts_end)
    ps_insulin = get_pod_sessions(settings["mongo_uri"], ts_start, ts_end)

    # ps_glucagon = get_pod_sessions(settings["glucopy_data_path"], ts_start, ts_end)

    ts_insulin0 = pd.Series(None, index=index_start_to_end).fillna(0)

    # ts_insulin1 = pd.Series(None, index=df_index).fillna(0)
    # ts_insulin2 = pd.Series(None, index=df_index).fillna(0)

    ps: PodSession
    for ps in ps_insulin:
        ts_model = get_insulin_model(ps).reindex(index_start_to_end).fillna(0)
        ts_insulin0 += ts_model
        # if ps.ended:
        #     ts_insulin1 += get_insulin_model(ps).reindex(df_index).fillna(0)
        #     ts_insulin2 += get_insulin_model(ps).reindex(df_index).fillna(0)
        #     pass
        # else:
        #     ts, minute, delivered, undelivered, reservoir = ps.last_entry
        #
        #     ps0 = ps.clone()
        #     ps0.temp_basal_start(ts_now, minute, delivered, undelivered, reservoir, 0, 180)
        #     ts_insulin1 += get_insulin_model(ps0).reindex(df_index).fillna(0)
        #
        #     ps1 = ps.clone()
        #     ps1.temp_basal_start(ts_now, minute, delivered, undelivered, reservoir, 10, 30)
        #     ts_insulin2 += get_insulin_model(ps1).reindex(df_index).fillna(0)

    ts_insulin0 = get_insulin_model2(ts_insulin0, weight, height)
    # ts_insulin1 = get_insulin_model2(ts_insulin1, 50, 136)
    # ts_insulin2 = get_insulin_model2(ts_insulin2, 50, 136)

    ts_bgd = pd.Series(signal.savgol_filter(ts_bg.array, window_length=91, polyorder=3, deriv=1,
                                            delta=2.0, mode='interp')
                       , index=ts_bg.index)

    svg_cf = ts_bg.diff().sum() / ts_bgd.sum()
    ts_bgd *= svg_cf

    ts_gg0 = ts_insulin0 + (ts_bgd / 28)

    ge_last_index = pd.Series(ts_gg0).last_valid_index()
    ge_prediction_range = pd.date_range(ge_last_index + dt.timedelta(minutes=1), pd_dts[3], freq='T')
    index_clip_to_ge_pred = pd.date_range(pd_dts[1], ge_last_index, freq='T')
    index_start_to_ge_pred = pd.date_range(pd_dts[0], ge_last_index, freq='T')

    ts_ge_predicted = ge_prediction(ts_gg0.reindex(index_start_to_ge_pred), ge_prediction_range)

    ts_ge = ts_gg0.reindex(index_clip_to_end).fillna(0) + ts_ge_predicted.reindex(index_clip_to_end).fillna(0)

    # ts_gg1 = ts_insulin1 + (ts_bgd / 28)
    # ts_gg2 = ts_insulin2 + (ts_bgd / 28)

    ts_insulin_past = ts_insulin0.reindex(index_clip_to_now)
    ts_insulin_next = ts_insulin0.reindex(index_now_to_end)
    ts_ge_past = ts_ge.reindex(index_clip_to_ge_pred)
    ts_ge_next = ts_ge.reindex(ge_prediction_range)

    # ts_insulin1 = ts_insulin1.reindex(pd.date_range(pd_dts[2], pd_dts[3], freq='T'))
    # ts_gg1 = ts_gg1.reindex(pd.date_range(pd_dts[2], pd_dts[3], freq='T'))
    # ts_insulin2 = ts_insulin2.reindex(pd.date_range(pd_dts[2], pd_dts[3], freq='T'))
    # ts_gg2 = ts_gg2.reindex(pd.date_range(pd_dts[2], pd_dts[3], freq='T'))

    ts_bg_past = ts_bg.reindex(index_clip_to_now) / 18.02
    ts_bg_next = ts_bg.reindex(index_now_to_end) / 18.02

    return ts_bg_past, ts_bg_next, ts_insulin_past, ts_insulin_next, ts_ge_past, ts_ge_next


def rel_min(s: pd.Series) -> pd.Series:
    return s[(s.shift(1) > s) & (s.shift(-1) > s)]


def rel_max(s: pd.Series) -> pd.Series:
    return s[(s.shift(1) < s) & (s.shift(-1) < s)]


def ge_prediction(ts_ge: pd.Series, prediction_range) -> pd.Series:
    usable_range_end = ts_ge.index[-1]
    usable_range_start = ts_ge[-1:0:-1].last_valid_index()

    ts_ge: pd.Series = ts_ge.reindex(pd.date_range(usable_range_start, usable_range_end, freq='T'))
    min_val = ts_ge.min()

    ts_pr = pd.Series(None, prediction_range)
    return ts_pr.fillna(min_val)
