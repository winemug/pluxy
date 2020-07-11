import json
import time
import datetime as dt
import pandas as pd
from plotly.graph_objs import Figure

from insulinmodel import get_insulin_model
from nsmongodb import get_bg_series
from omnipydb import get_pod_sessions

pd.options.plotting.backend = "plotly"

with open("settings.json", "r") as stream:
    settings = json.load(stream)

ts_now = time.time()
ts_now -= ts_now % 60
ts_start = ts_now - 7*24*60*60
ts_end = ts_now + 6*60*60

pd_dts = pd.to_datetime([ts_start, ts_now, ts_end], unit='s', origin='unix', utc=True)
df_index = pd.date_range(pd_dts[0], pd_dts[2], freq='T')
df = pd.DataFrame(index=df_index)
ts_bg = get_bg_series(settings["mongo_uri"], ts_start, ts_end)

ps_insulin = get_pod_sessions(settings["omnipy_data_path"], ts_start, ts_end)
#ps_glucagon = get_pod_sessions(settings["glucopy_data_path"], ts_start, ts_end)

ts_insulin = pd.Series(None, index=df_index).fillna(0)
for ps in ps_insulin:
    ts_model = get_insulin_model(ps).reindex(df_index).fillna(0)
    ts_insulin += ts_model

df["bg"] = ts_bg / 18.02
df["i"] = ts_insulin * 100

fig: Figure = df.plot()
fig.show()

