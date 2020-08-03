import datetime as dt
import time
import json
import pandas as pd
import plotly.graph_objects as go

from datamodel import *
from plotly.subplots import make_subplots

bgd_color_max = '#ffff00'
bgd_color_mid = '#00ff00'
bgd_color_min = '#ff0000'

COLOR_BGC = '#ff1933'
COLOR_BGD = '#ff1933'
COLOR_BGD2 = ''#ff1933''

import plotly.io as pio


def render_simple(hours_prev: float, hours_next: float, no_show: bool = True, alt_act=None):
    ts_now = dt.datetime.now().replace(second=0, microsecond=0).timestamp()
    ts_start = ts_now - hours_prev * 60 * 60
    ts_end = ts_now + hours_next * 60 * 60

    df = get_data_model(ts_start, ts_end, 50, 140, alternative_action=alt_act)

    bgc = df[DF_C_BGC] / 18.02
    bgd = df[DF_C_BGC_DIFF] / 18.02 * -5
    #bgdd = df[DF_C_BGC_DIFF2] / 18.02 * 5

    rates = df[DF_C_INFUSION_RATE]
    bolus = df[DF_C_BOLUS]
    bolus = bolus[bolus > 0]
    bolus_y = rates.loc[bolus.index]

    infusion = df[DF_C_INFUSION].diff()
    absorption = df[DF_C_ABSORBED_INSULIN].diff()

    insulin_free = df[DF_C_PLASMA_INSULIN] + df[DF_C_INTERSTITIAL_INSULIN] + df[DF_C_HEPATIC_INSULIN]
    insulin_bound = df[DF_C_PERIPHERAL_BOUND_INSULIN] + df[DF_C_LIVER_BOUND_INSULIN]
    insulin_onboard = insulin_free + insulin_bound

    #figure = go.Figure()
    figure = make_subplots(rows=2, cols=1, shared_xaxes=True)

    figure.add_trace(
        go.Scatter(
            name='bgc (mmol/l)',
            x=df.index, y=bgc,
            mode='lines',
            line=dict(
                color='#ff1933',
                width=5
            )
        ), row=1, col=1
    )

    figure.add_trace(
        go.Scatter(
            name='rate (U/h)',
            x=df.index, y=rates,
            mode='lines',
            fill='tozeroy',
            fillcolor='rgba(80, 200, 200, 0.3)',
            line=dict(
                color='rgba(80, 150, 255, 0.3)',
                width=2
            )
        ), row=1, col=1
    )

    figure.add_trace(
        go.Scatter(
            name='bolus (U)',
            x=bolus.index, y=bolus_y,
            mode='markers+text',
            textposition="top center",
            texttemplate='%{marker.size:.2f} U',
            marker=dict(
                size=bolus,
                sizeref=1/100,
                sizemode='area',
                line=dict(
                    width=1,
                    color='DarkSlateGrey'
                )
            )
        ), row=1, col=1
    )

    figure.add_trace(
        go.Scatter(
            name='i bound',
            x=insulin_bound.index, y=insulin_bound,
            mode='lines',
            fill = 'tozeroy',
                   fillcolor = 'rgba(80, 80, 200, 0.3)',
                               line = dict(
                color='rgba(80, 80, 140, 0.3)',
                width=2
            )
        ), row=2, col=1
    )

    figure.add_trace(
        go.Scatter(
            name='bgd',
            x=df.index, y=bgd,
            mode='lines',
            fill = 'tozeroy',
                   fillcolor = 'rgba(200, 80, 70, 0.3)',
                               line = dict(
                color='rgba(160, 80, 70, 0.3)',
                width=2
            )
        ), row=2, col=1
    )

    # figure.add_trace(
    #     go.Scatter(
    #         name='bgd2',
    #         x=df.index, y=bgdd,
    #         mode='lines'
    #     ), row=2, col=1
    # )

    # figure.add_trace(
    #     go.Scatter(
    #         name='iob',
    #         x=insulin_onboard.index, y=insulin_onboard,
    #         mode='lines'
    #     ), row=2, col=1
    # )


    # figure.add_trace(
    #     go.Scatter(
    #         name='i free',
    #         x=insulin_free.index, y=insulin_free,
    #         mode='lines'
    #     ), row=2, col=1
    # )

    # figure.add_trace(
    #     go.Scatter(
    #         name='infusion',
    #         x=infusion.index, y=infusion,
    #         mode='lines',
    #         fill='tozeroy',
    #         fillcolor='rgba(200, 160, 120, 0.3)',
    #         line=dict(
    #             color='rgba(255, 100, 120, 0.3)',
    #             width=2
    #         )
    #     ), row=2, col=1
    # )
    #
    # figure.add_trace(
    #     go.Scatter(
    #         name='absorption',
    #         x=absorption.index, y=absorption,
    #         mode='lines',
    #         fill='tozeroy',
    #         fillcolor='rgba(200, 200, 80, 0.3)',
    #         line=dict(
    #             color='rgba(255, 150, 80, 0.3)',
    #             width=2
    #         )
    #     ), row=2, col=1
    # )

    # bgd41_symbols = pd.Series(index=bgd.index)
    #
    # bgd41_symbols = bgd41_symbols.add(pd.Series(5, bgd[bgd >= 0.5].index), fill_value=0)
    # bgd41_symbols = bgd41_symbols.add(pd.Series(9, bgd[bgd > 0.2][bgd < 0.5].index), fill_value=0)
    # bgd41_symbols = bgd41_symbols.add(pd.Series(10, bgd[bgd < -0.2][bgd > -0.5].index), fill_value=0)
    # bgd41_symbols = bgd41_symbols.add(pd.Series(6, bgd[bgd <= -0.5].index), fill_value=0)
    # bgd41_symbols = bgd41_symbols.fillna(8)
    #
    # bgd41_y = bg.reindex(bgd.index)
    # bgd41_y = pd.Series(index=bgd41.index)
    # bgd41_y = bgd41_y.add(bg.reindex(bgd41[bgd41 >= 0.2].index), fill_value=1)
    # bgd41_y = bgd41_y.add(bg.reindex(bgd41[bgd41 <= -0.2].index), fill_value=-1)
    # bgd41_y = bgd41_y.add(bg.reindex(bgd41[bgd41 > -0.2][bgd41 < -0.2].index), fill_value=0)




    # figure.add_traces(
    #     go.Scatter(name='absorption', x=df.index, y=df[DF_C_ABSORBED_INSULIN], mode='lines', fill='tozeroy',
    #                fillcolor='#68c7d0', line=dict(color='#68c7d0', width=1)))

    #
    # figure.add_traces(go.Scatter(name='bgd', x=bgd.index, y=bgd, mode='lines'))
    # figure.add_traces(go.Scatter(name='bgdd', x=bgdd.index, y=bgdd, mode='lines'))


    # figure.add_traces(
    #     go.Scatter(name="diff bgc (mmol/l min)",
    #                x=bgd.index,
    #                y=bgd,
    #                mode='markers',
    #                marker=dict(
    #                    color=bgd,
    #                    colorscale=[[0, bgd_color_min],[0.5, bgd_color_mid], [1.0, bgd_color_max]],
    #                    cmin=-1,
    #                    cmax=1,
    #                    cmid=0,
    #                    size=(abs(bgd.fillna(0)) + 5) * 1.2,
    #                    line=dict(color='#443344')
    #                    ),
    #                marker_symbol=bgd41_symbols,
    #                )
    #     )

    # pd_now = pd.to_datetime(time.time() + 2 * 60 * 60, unit='s', utc=True)
    # pd_start += dt.timedelta(hours=2)
    # pd_end += dt.timedelta(hours=2)
    #
    # figure.add_shape(
    #     dict(
    #         type="line",
    #         x0=pd_now,
    #         y0=min(bg.min(), bgd.min(), all_rates.min()),
    #         x1=pd_now,
    #         y1=max(bg.max(), bgd.max(), all_rates.max()),
    #         line=dict(
    #             color='#333835',
    #             width=1, dash='dot')))

    # figure.add_shape(
    #     dict(
    #         type="line",
    #         x0=pd_start,
    #         y0=0,
    #         x1=pd_end,
    #         y1=0,
    #         line=dict(
    #             color=bgd_color_mid,
    #             width=1, dash='dot')))
    #
    # figure.add_shape(
    #     dict(
    #         type="line",
    #         x0=pd_start,
    #         y0=1,
    #         x1=pd_end,
    #         y1=1,
    #         line=dict(
    #             color=bgd_color_max,
    #             width=2, dash='dash')), yref='y2')
    #
    # figure.add_shape(
    #     dict(
    #         type="line",
    #         x0=pd_start,
    #         y0=-1,
    #         x1=pd_end,
    #         y1=-1,
    #         line=dict(
    #             color=bgd_color_min,
    #             width=2, dash='dash')), yref='y2')

    # figure.update_layout(
    #     margin=dict(l=5, r=5, t=50, b=5),
    #     yaxis=dict(
    #         tickmode='linear',
    #         tick0=0,
    #         dtick=1,
    #         side='right'
    #     ),
    #     yaxis2=dict(
    #         overlaying="y",
    #         anchor='free',
    #         scaleanchor='y',
    #         scaleratio=5,
    #         showgrid=False,
    #         zeroline=False
    #     ),
    #     coloraxis_showscale=False
    # )

    if no_show:
        figure.layout.hidesources = True
        figure.layout.showlegend = False
        return figure.to_image(format="png", engine="kaleido")
    else:
        figure.show()


def rel_min(s: pd.Series) -> pd.Series:
    return s[(s.shift(1) > s) & (s.shift(-1) > s)]


def rel_max(s: pd.Series) -> pd.Series:
    return s[(s.shift(1) < s) & (s.shift(-1) < s)]


def set_zero(ps: PodSession, ts, minute, delivered, undelivered, reservoir):
    ps.temp_basal_start(ts, minute, delivered, undelivered, reservoir, 0.0, 60*104)


if __name__ == '__main__':
    img = render_simple(6, 6, no_show=False, alt_act=None)
