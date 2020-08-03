from scipy import signal
import pandas as pd

from nsomni import get_bg_series, get_manual_injections, get_pod_sessions
from podsession import PodSession
import datetime as dt

DF_C_BGC = 'bgc'
DF_C_BGC_DIFF = 'bgc_diff'
DF_C_BGC_DIFF2 = 'bgc_diff2'
DF_C_INFUSION_RATE = 'insulin_infusion_rate'
DF_C_BOLUS = 'insulin_bolus'
DF_C_INFUSION = 'insulin_infusion'
DF_C_ABSORBED_INSULIN = 'insulin_absorbed'
DF_C_PLASMA_INSULIN = 'insulin_plasma'
DF_C_HEPATIC_INSULIN = 'insulin_hepatic_'
DF_C_INTERSTITIAL_INSULIN = 'insulin_interstitial'
DF_C_LIVER_BOUND_INSULIN = 'insulin_liver_bound'
DF_C_PERIPHERAL_BOUND_INSULIN = 'insulin_peripheral_bound'


def get_data_model(ts_start: int, ts_end: int, w: float, h: float, alternative_action=None) -> pd.DataFrame:
    ts_start_precursor = ts_start - dt.timedelta(hours=24).total_seconds()

    index = pd.date_range(start=pd.to_datetime(ts_start, unit='s', utc=True),
                                          end=pd.to_datetime(ts_end, unit='s', utc=True),
                                          freq='T');
    df = pd.DataFrame(index=index)

    pss = get_pod_sessions(ts_start_precursor, ts_end)

    all_rates = pd.Series()
    all_bolus = pd.Series()
    infusion_list = []

    for ps in pss:
        if not ps.ended and alternative_action is not None:
            ts, minute, delivered, undelivered, reservoir = ps.last_entry
            alternative_action(ps, ts, minute, delivered, undelivered, reservoir)

        pod_entries = ps.get_entries()
        infusion_list.append(pod_entries)

        all_rates = all_rates.append(ps.get_rates())
        all_bolus = all_bolus.append(ps.get_boluses())

    manual_injections = get_manual_injections(ts_start_precursor, ts_end)
    for i_mi in manual_injections.index:
        single_injection = pd.Series(manual_injections.loc[i_mi], [i_mi])
        infusion_list.append(single_injection)
        all_bolus = all_bolus.append(single_injection)

    all_infusion = pd.Series()
    for infusion in infusion_list:
        all_infusion = all_infusion.append(infusion)

    df[DF_C_INFUSION] = all_infusion.resample('T').sum().cumsum()
    df[DF_C_BOLUS] = all_bolus.resample('T').sum()
    df[DF_C_INFUSION_RATE] = all_rates.resample('T').mean().ffill()
    bg = get_bg_series(ts_start_precursor, ts_end).resample('T').mean()
    df[DF_C_BGC] = bg
    df[DF_C_BGC_DIFF] = savgol_filter(bg, 41, 2, 1, 1.0)
    df[DF_C_BGC_DIFF2] = savgol_filter(bg, 41, 3, 1, 1.0)

    i_absorbed = pd.Series()
    for infusion in infusion_list:
        i_absorbed = i_absorbed.append(simulate_insulin_absorption(infusion.resample('T').sum() * 1000))
    i_absorbed = i_absorbed.resample('T').sum()
    df[DF_C_ABSORBED_INSULIN] = i_absorbed.cumsum() / 1000

    df_sim = simulate_insulin_action(i_absorbed, w, h)

    df[DF_C_PLASMA_INSULIN] = df_sim[DF_C_PLASMA_INSULIN] / 1000
    df[DF_C_HEPATIC_INSULIN] = df_sim[DF_C_HEPATIC_INSULIN] / 1000
    df[DF_C_INTERSTITIAL_INSULIN] = df_sim[DF_C_INTERSTITIAL_INSULIN] / 1000
    df[DF_C_LIVER_BOUND_INSULIN] = df_sim[DF_C_LIVER_BOUND_INSULIN] / 1000
    df[DF_C_PERIPHERAL_BOUND_INSULIN] = df_sim[DF_C_PERIPHERAL_BOUND_INSULIN] / 1000

    return df


def simulate_insulin_absorption(entries: pd.Series,
                      k=0.67, # monomer ratio
                      ka1=0.0112, # slow channel two stage absorption rate in min
                      ka2=0.0210, # fast channel absorption rate in min
                      vmld=1.93, # saturation # mU/min
                      kmld=62.6, #midpoint # mU
                      coe=0.009) -> pd.Series:

    q1a = 0.
    q1b = 0.
    q2 = 0.

    insulin_action = []
    index = 0

    close_zero = 0.05
    steps = 100
    ka1 /= steps
    ka2 /= steps
    vmld /= steps

    while index < len(entries) or q1a + q1b + q2 > close_zero:
        u = 0
        if index < len(entries):
            u = entries[index] # mU/min
            index += 1

        q3 = 0.0
        u /= steps

        for _ in range(steps):
            qi = q1a + q1b + q2
            if qi > 0:
                kqi = 1 / (pow(qi, 2/3) * coe + 1)
            else:
                kqi = 1

            lda = vmld*q1a/(kmld+q1a)
            ldb = vmld*q1b/(kmld+q1b)

            dq1a = k*u-ka1*q1a-lda
            dq1b = (1-k)*u-ka2*kqi*q1b-ldb
            dq2 = ka1*kqi*q1a-ka1*kqi*q2
            dq3 = ka1*kqi*q2+ka2*kqi*q1b # w/o elimination

            q1a += dq1a
            q1b += dq1b
            q2 += dq2
            q3 += dq3

            if min(q1a,q1b,q2,q3) < 0:
                raise Exception

        insulin_action.append(q3)

    return pd.Series(insulin_action, pd.date_range(start=entries.index[0], freq='T', periods=len(insulin_action)))


def simulate_insulin_action(
                       i_abs: pd.Series,
                       w: float,
                       h: float,
                       k01: float = 0.041,
                       k21: float = 0.037,
                       k42: float = 0.790,
                       k24: float = 0.150,
                       k04: float = 0.021,
                       r4: float = 127,
                       r5: float = 82) -> pd.DataFrame:
    v1 = 45.05 * w # ml/kg
    v2 = 150 * w # ml/kg
    v3 = 4.95 * w # ml/kg

    r4 = r4 * w
    r5 = r5 * w

    steps = 100
    k01 /= steps
    k21 /= steps
    k42 /= steps
    k24 /= steps
    k04 /= steps


    ci = 1760  # ml/m^2 1760wtf? #### L/min/m^2  2.6 to 4.2???? /min
    s = 0.007184 * pow(w, 0.425) * pow(h, 0.725) # m^2 (w kg, h cm)
    co = ci * s # ml /min
    k35 = k24
    k05 = k04
    k12 = k21 * v1 / v2
    k31 = 0.3 * co / v1 / steps
    k13 = 0.3 * co / v3 / steps
    k53 = (k42*v2*r5) / (r4*v3)

    q1 = 0.
    q2 = 0.
    q3 = 0.
    q4 = 0.
    q5 = 0.

    df = pd.DataFrame(index=i_abs.index)

    df[DF_C_PLASMA_INSULIN] = pd.Series()
    df[DF_C_HEPATIC_INSULIN] = pd.Series()
    df[DF_C_INTERSTITIAL_INSULIN] = pd.Series()
    df[DF_C_LIVER_BOUND_INSULIN] = pd.Series()
    df[DF_C_PERIPHERAL_BOUND_INSULIN] = pd.Series()

    for index in range(len(i_abs)):
        u1 = i_abs.iloc[index] / steps
        for i in range(steps):
            dq1 = -(k01+k21+k31)*q1 + k12*q2 + k13*q3 + u1
            dq2 = k21*q1 - k12*q2 - k42*(1-q4/r4)*q2 + k24*q4
            dq3 = k31*q1 - k13*q3 - k53*(1-q5/r5)*q3 + k35*q5
            dq4 = k42*(1-q4/r4)*q2 - (k04+k24)*q4
            dq5 = k53*(1-q5/r5)*q3 - (k05+k35)*q5

            q1 += dq1
            q2 += dq2
            q3 += dq3
            q4 += dq4
            q5 += dq5

            if min(q1, q2, q3, q4, q5) < 0:
                raise Exception

        df[DF_C_PLASMA_INSULIN].iloc[index] = q1
        df[DF_C_HEPATIC_INSULIN].iloc[index] = q2
        df[DF_C_INTERSTITIAL_INSULIN].iloc[index] = q3
        df[DF_C_LIVER_BOUND_INSULIN].iloc[index] = q4
        df[DF_C_PERIPHERAL_BOUND_INSULIN].iloc[index] = q5

    return df


def savgol_filter(ts: pd.Series, window_length, polyorder, deriv, delta) -> pd.Series:
    svg = signal.savgol_filter(ts.array[:], window_length=window_length, polyorder=polyorder, deriv=deriv, delta=delta,
                               mode='interp')
    return pd.Series(svg[:], ts.index)