import pandas as pd
import plotly.graph_objs as go
import plotly as pl
from processing import get_processed_data


def main():
    pd.options.plotting.backend = "plotly"

    ts_bg_past, ts_bg_next, ts_i_past, ts_i_next, ts_ge_past, ts_ge_next = get_processed_data(6, 3, 50, 140)

    index_past = ts_bg_past.index
    index_next = ts_bg_next.index

    colors = pl.colors.qualitative.Pastel
    figure = go.Figure()

    add_trace(figure, "bg", index_past, index_next, ts_bg_past, ts_bg_next, colors[2])
    add_trace(figure, "i", index_past, index_next, ts_i_past, ts_i_next, colors[0])
    add_trace(figure, "ge", index_past, index_next, ts_ge_past, ts_ge_next, colors[1])

    figure.show()


def add_trace(figure, name, index_past, index_next, data_past, data_next, color):
    figure.add_traces(
        go.Scatter(name=name, x=index_past, y=data_past, mode='lines', line=dict(color=color, dash='solid')))
    figure.add_traces(
        go.Scatter(name=name + "_next", x=index_next, y=data_next, mode='lines', line=dict(color=color, dash='dash')))

    # p = ts_bgd[ts_bgd.shift(1) < ts_bgd][ts_bgd >= 0].index[0]
    # ts_bgd = ts_bgd.reindex(pd.date_range(p, ts_bgd.index[-1], freq='T'))
    # ts_bgd -= ts_bgd[0]
    # ts_bgd = ts_bgd.cumsum()
    # ts_it = ts_insulin.reindex(ts_bgd.index).cumsum()

    # ts_gg = (0.1 * ts_bgd + ts_insulin)
    #
    # while True:
    #     ts_gg = (ts_bgd + ts_insulin)
    #     new_range = pd.date_range(pd_dts[1] - dt.timedelta(hours=24), pd_dts[1], freq='T')
    #     for i in range(0, 14):
    #         ts_gx = ts_gg.shift(24*60*i).reindex(new_range)
    #         #fig2.add_traces(go.Scatter(x=ts_gx.index, y=ts_gx, mode='lines', line=dict(dash='dash')))
    #
    #         write_to_influx(ts_bgd.shift(24*60*i).reindex(new_range), "b" + str(i))
    #         write_to_influx(ts_insulin.shift(24*60*i).reindex(new_range), "i" + str(i))
    #
    #         #fig2.add_traces(go.Scatter(x=ts_ia.index, y=ts_ia, mode='lines', line=dict(dash='dot')))
    #     #fig2.show()
    #


if __name__ == '__main__':
    main()
