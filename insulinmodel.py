import math
import pandas as pd
from podsession import PodSession
import datetime as dt


def get_insulin_model(ps: PodSession,
                      monomeric_ratio=0.15,
                      disassociation_rate=0.004,
                      primary_absorption_rate=0.026,
                      lymphatic_absorption_rate=0.0015,
                      degradation_saturation_monomers=2.8,
                      degradation_midpoint_monomers=350.0,
                      degradation_saturation_hexamers=2.8,
                      degradation_midpoint_hexamers=350.0,
                      spherical_coefficient=5.9) -> pd.Series:
    fast_compartment = 0.
    slow_compartment = 0.
    circulation = 0.
    q = []
    for i in range(0, 9):
        q.append(0.0)

    entries = ps.get_entries().resample('T').sum()

    insulin_action = []
    index = []

    pdt = entries.index[0]
    entries = entries.reindex(pd.date_range(entries.index[0], entries.index[-1] + dt.timedelta(hours=24), freq='T')) \
        .fillna(0)

    for deposit in entries:
        # incoming
        slow_compartment += deposit * (1. - monomeric_ratio)
        fast_compartment += deposit * monomeric_ratio

        # breakdown
        disassociation = disassociation_rate * slow_compartment
        slow_compartment -= disassociation
        fast_compartment += disassociation

        # surface availability
        volume_depot = fast_compartment + slow_compartment
        availability = 1
        if volume_depot > 0:
            radius_depot = spherical_radius(volume_depot)
            availability = (1 - (radius_depot / (radius_depot + spherical_coefficient)))

        fast_available = availability * fast_compartment
        slow_available = availability * slow_compartment

        # local degradation
        fast_degradation = degradation_saturation_monomers * fast_compartment \
                           / (degradation_midpoint_monomers + fast_compartment)
        slow_degradation = degradation_saturation_hexamers * slow_compartment \
                           / (degradation_midpoint_hexamers + slow_compartment)

        fast_compartment -= fast_degradation
        slow_compartment -= slow_degradation

        # to direct circulation
        primary_transfer = primary_absorption_rate * fast_available
        lymphatic_transfer = lymphatic_absorption_rate * slow_available

        fast_compartment -= primary_transfer
        slow_compartment -= lymphatic_transfer

        to_circulation = primary_transfer + lymphatic_transfer

        index.append(pdt)
        insulin_action.append(to_circulation)
        pdt += dt.timedelta(minutes=1)

    insulin_action = pd.Series(insulin_action, index)
    correction = entries.sum() / insulin_action.sum()
    return insulin_action * correction


def spherical_radius(volume):
    return math.pow(volume / ((4. / 3.) * math.pi), 1. / 3.)


def spherical_volume(radius):
    return (4. / 3.) * math.pi * math.pow(radius, 3)

def get_insulin_model2(i: pd.Series,
                       w: float,
                       h: float,
                       k01: float = 0.050, # se 0.005
                       k04: float = 0.017, # se 0.002
                       k21: float = 0.076, # se 0.017
                       k24: float = 0.064, # se 0.011
                       k42: float = 0.840, # se 0.190
                       r4: float = 0.000183,                  # se 25
                       r5: float = 0.000204) -> pd.Series:    # se 22
    ima = []
    q1 = 0.
    q2 = 0.
    q3 = 0.
    q4 = 0.
    q5 = 0.

    v1 = 0.04505 * w
    v2 = 0.150 * w
    v3 = 0.00495 * w
    r4 = r4 * w
    r5 = r5 * w
    ci = 1.760
    s = 0.007184 * pow(w, 0.425) * pow(h, 0.725)
    c0 = ci * s
    k35 = k24
    k05 = k04
    k12 = k21 * v1 / v2
    k31 = 0.3 * c0 / (v1*1000)
    k13 = 0.3 * c0 / (v3*1000)
    k53 = (k42*v2*r5) / (r4*v3*1000)

    for u1 in i:
        u2 = 0.0
        dqr4 = min(max((1-q4/r4), 0), 1)
        dqr5 = min(max((1-q5/r5), 0), 1)
        dq1 = -(k01+k21+k31)*q1 + k12*q2 + k13*q3 + u1
        dq2 = k21*q1 - k12*q2 - k42*dqr4*q2 + k24*q4
        dq3 = k31*q1 - k13*q3 - k53*dqr5*q3 + k35*q5 + u2
        dq4 = k42*dqr4*q2 - (k04+k24)*q4
        dq5 = k53*dqr5*q3 - (k05+k35)*q5
        q1 += dq1
        q2 += dq2
        q3 += dq3
        q4 += dq4
        q5 += dq5
        c = q1 / v1
        print(f'{q1:3.3f} {q2:3.3f} {q3:3.3f} {q4:3.3f} {q5:3.3f}')
        ima.append(c)

    return pd.Series(ima, i.index)
