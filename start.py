import json
import time
import datetime as dt
import pandas as pd
import plotly.graph_objs as go
import plotly as pl
from scipy import signal

from insulinmodel import get_insulin_model
from nsmongodb import get_bg_series
from omnipydb import get_pod_sessions
from podsession import PodSession

pd.options.plotting.backend = "plotly"

with open("settings.json", "r") as stream:
    settings = json.load(stream)

ts_now = time.time()
ts_now -= ts_now % 60
ts_start = ts_now - 24*60*60
ts_end = ts_now + 6*60*60

pd_dts = pd.to_datetime([ts_start, ts_now, ts_end], unit='s', origin='unix', utc=True)
df_index = pd.date_range(pd_dts[0], pd_dts[2], freq='T')
df = pd.DataFrame(index=df_index)
ts_bg = get_bg_series(settings["mongo_uri"], ts_start, ts_end)

ps_insulin = get_pod_sessions(settings["omnipy_data_path"], ts_start, ts_end)
#ps_glucagon = get_pod_sessions(settings["glucopy_data_path"], ts_start, ts_end)

ts_insulin = pd.Series(None, index=df_index).fillna(0)
ts_alt0 = pd.Series(None, index=df_index).fillna(0)
ts_alt1 = pd.Series(None, index=df_index).fillna(0)

ps: PodSession
for ps in ps_insulin:
    ts_model = get_insulin_model(ps).reindex(df_index).fillna(0)
    ts_insulin += ts_model
    if ps.ended:
        ts_alt0 += get_insulin_model(ps).reindex(df_index).fillna(0)
        ts_alt1 += get_insulin_model(ps).reindex(df_index).fillna(0)
    else:
        ts, minute, delivered, undelivered, reservoir = ps.last_entry

        ps0 = ps.clone()
        ps0.temp_basal_start(ts_now, minute, delivered, undelivered, reservoir, 0, 180)
        ts_alt0 += get_insulin_model(ps0).reindex(df_index).fillna(0)

        ps1 = ps.clone()
        ps1.temp_basal_start(ts_now, minute, delivered, undelivered, reservoir, 10, 30)
        ts_alt1 += get_insulin_model(ps1).reindex(df_index).fillna(0)

ts_bgd = pd.Series(signal.savgol_filter(ts_bg.array, window_length=91, polyorder=2, deriv=1,
                                        delta=2.0, mode='interp')
                   , index=ts_bg.index) * -5

ts_insulin = ts_insulin * 100
ts_alt0 = ts_alt0.reindex(pd.date_range(pd_dts[1], pd_dts[2], freq='T')) * 100
ts_alt1 = ts_alt1.reindex(pd.date_range(pd_dts[1], pd_dts[2], freq='T')) * 100

colors = pl.colors.qualitative.Plotly
fig = go.Figure()
fig.add_traces(go.Scatter(x=ts_bg.index, y=ts_bg, mode='lines', line=dict(color=colors[0])))
fig.add_traces(go.Scatter(x=ts_insulin.index, y=ts_insulin, mode='lines', line=dict(color=colors[1])))
fig.add_traces(go.Scatter(x=ts_bgd.index, y=ts_bgd, mode='lines', line=dict(color=colors[2])))
fig.add_traces(go.Scatter(x=ts_alt0.index, y=ts_alt0, mode='lines', line=dict(color=colors[3], dash='dash')))
fig.add_traces(go.Scatter(x=ts_alt1.index, y=ts_alt1, mode='lines', line=dict(color=colors[4], dash='dash')))
fig.show()
