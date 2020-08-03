import datetime as dt
import time
import json
import pandas as pd
import numpy as np
import scipy as sp
import plotly.graph_objects as go
import scipy.interpolate

figure = go.Figure()

ie = pd.Series([1.83, 1.45, 1.24, 1.11, 1.04, 0.90, 0.81], [50, 60, 70, 80, 90, 200, 300])
ie = 1 / ie
ie = ie.reindex(range(600))
ie = ie.interpolate(method='spline', order=3).bfill()
figure.add_traces(go.Scatter(name='ie', x=ie.index, y=ie))

ie = pd.Series([1.83, 1.45, 1.24, 1.11, 1.04, 0.90, 0.81, 0.], [50, 60, 70, 80, 90, 200, 300])
ie = 1 / ie
ie = ie.reindex(range(600))
ie = ie.interpolate(method='spline', order=3).bfill()
figure.add_traces(go.Scatter(name='ie2', x=ie.index, y=ie))

figure.show()
