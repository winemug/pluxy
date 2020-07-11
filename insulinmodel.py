import math
import pandas as pd
from podsession import PodSession
import datetime as dt

def get_insulin_model(ps: PodSession,
                      monomeric_ratio=0.15, \
                      disassociation_rate=0.004, \
                      primary_absorption_rate=0.021, \
                      lymphatic_absorption_rate=0.0015, \
                      elimination_rate=0.0328, \
                      degradation_saturation_monomers=2.8, \
                      degradation_midpoint_monomers=350.0, \
                      degradation_saturation_hexamers=2.8, \
                      degradation_midpoint_hexamers=350.0, \
                      spherical_coefficient=0.9) -> pd.Series:
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
    entries = entries.reindex(pd.date_range(entries.index[0], entries.index[-1] + dt.timedelta(hours=24), freq='T'))\
        .fillna(0)
    
    for deposit in entries:
        #incoming
        slow_compartment += deposit * (1. - monomeric_ratio)
        fast_compartment += deposit * monomeric_ratio

        #breakdown
        disassociation = disassociation_rate * slow_compartment
        slow_compartment -= disassociation
        fast_compartment += disassociation

        #surface availability
        volume_depot = fast_compartment + slow_compartment
        fast_available = 0
        slow_available = 0
        radius_depot = 0
        availability = 1
        if volume_depot > 0:
            radius_depot = spherical_radius(volume_depot)
            availability = (1-(radius_depot/(radius_depot+spherical_coefficient)))

        fast_available = availability * fast_compartment
        slow_available = availability * slow_compartment

        #local degradation
        fast_degradation = degradation_saturation_monomers * fast_compartment \
                            / (degradation_midpoint_monomers + fast_compartment)
        slow_degradation = degradation_saturation_hexamers * slow_compartment \
                           / (degradation_midpoint_hexamers + slow_compartment)


        #fast_compartment -= fast_degradation
        #slow_compartment -= slow_degradation

        #to direct circulation
        primary_transfer = primary_absorption_rate * fast_available
        lymphatic_transfer = lymphatic_absorption_rate * slow_available

        fast_compartment -= primary_transfer
        slow_compartment -= lymphatic_transfer

        circulation -= elimination_rate * circulation
        circulation += primary_transfer + lymphatic_transfer

        for a in range(8, 0, -1):
            q[a] = q[a-1]

        q[0] = primary_transfer + lymphatic_transfer

        qt = 0.0
        for qv in q:
            qt += qv

        index.append(pdt)
        insulin_action.append(circulation)
        pdt += dt.timedelta(minutes=1)

    insulin_action = pd.Series(insulin_action, index)
    correction = entries.sum() / insulin_action.sum()
    return insulin_action * correction


def spherical_radius(volume):
    return math.pow(volume / ((4./3.) * math.pi), 1./3.)


def spherical_volume(radius):
    return (4. / 3.) * math.pi * math.pow(radius, 3)
