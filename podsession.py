import pandas as pd
import datetime as dt


def get_half_hour_ticks(ticks_per_half_hour: int, offset: float) -> []:
    seconds = []
    if ticks_per_half_hour == 0:
        return seconds
    interval = 1800 / ticks_per_half_hour
    second = offset - 2
    while ticks_per_half_hour > 0:
        second += interval
        second %= 3600
        seconds.append(second)
        ticks_per_half_hour -= 1
    return seconds


def get_ticking_seconds(ticks_per_hour: int, ts_start: float = None):
    hh_ticks = int(ticks_per_hour / 2)
    offset = 0
    if ts_start is not None:
        offset = ts_start % 3600

    seconds_a = get_half_hour_ticks(hh_ticks, offset)
    if ticks_per_hour % 2 == 0:
        seconds_b = get_half_hour_ticks(hh_ticks, offset + 1800)
    else:
        seconds_b = get_half_hour_ticks(hh_ticks + 1, offset + 1800)
    for second in seconds_b:
        seconds_a.append(second)
    seconds_a.sort()
    return seconds_a


def append_rate_ticks(ts_start: float, ts_end: float, tick_seconds: list, append_to: list):
    lts = len(tick_seconds)
    if lts == 0:
        return

    start_second = ts_start % 3600
    idx = 0
    for i in range(0, lts):
        if tick_seconds[i] > start_second:
            break
        idx += 1

    ts = ts_start - start_second

    while True:
        if idx == lts:
            idx = 0
            ts += 3600

        next_ts = ts + tick_seconds[idx]
        if next_ts >= ts_end:
            break
        append_to.append(next_ts)
        idx += 1


def append_bolus_ticks(bolus_start: float, bolus_ticks: int, append_to: list):
    dx = bolus_start
    idx = 0
    for dt_tick in append_to:
        if dt_tick > dx:
            break
        idx += 1

    while bolus_ticks > 0:
        idx, dx = find_bolus_slot(idx, dx, append_to)
        append_to.insert(idx, dx)
        idx += 1
        dx += 2
        bolus_ticks -= 1


def find_bolus_slot(idx: int, bolus_tick_ts, tick_list: list) -> (int, float):
    if idx > 0 and len(tick_list) > 0:
        prev_tick = tick_list[idx - 1]
        if bolus_tick_ts - prev_tick < 2.0:
            bolus_tick_ts = prev_tick + 2.0

    if idx < len(tick_list):
        next_tick = tick_list[idx]
        while next_tick - bolus_tick_ts < 2.0:
            idx += 1
            bolus_tick_ts = next_tick + 2.0
            if idx == len(tick_list):
                break
            next_tick = tick_list[idx]

    return idx, bolus_tick_ts


class PodSession:
    def __init__(self,
                 min_reservoir: float = 170,
                 start_delivery_offset: float = 2.85,
                 precision: float = 0.05):
        self.deliveries = dict()
        self.fixed_stamps = []
        self.fixed_deliveries = []
        self.ts_baseline = None
        self.ts_baseline_min = None
        self.ts_baseline_max = None
        self.precision = precision
        self.reservoir = self._pi(min_reservoir)
        self.start_delivery_offset = self._pi(start_delivery_offset)

        self.activation_ts = None
        self.start_ts = None
        self.basal_rate = None
        self.end_ts = None

        self.temp_basal_start_min = None
        self.temp_basal_end_min = None
        self.temp_basal_total = None

        self.bolus_start_min = None
        self.bolus_end_min = None
        self.bolus_total = None

        self.ended = False

        self.temp_basals = []
        self.boluses = []
        self.last_entry = None

    def get_entries(self) -> pd.Series:
        ts_ticks = []
        deliveries = []

        basal_ticks = get_ticking_seconds(self.basal_rate)

        basal_start = self.start_ts
        for rate_start, rate_end, rate in self.temp_basals:
            basal_end = rate_start
            if basal_end > basal_start:
                append_rate_ticks(basal_start, basal_end, basal_ticks, ts_ticks)

            if self.ended and rate_end > self.end_ts:
                rate_end = self.end_ts

            if rate_end > rate_start:
                tb_tick_list = get_ticking_seconds(rate)
                append_rate_ticks(rate_start, rate_end, tb_tick_list, ts_ticks)
            basal_start = rate_end

        if self.ended:
            basal_end = self.end_ts
        else:
            basal_end = self.activation_ts + 80 * 60 * 60

        append_rate_ticks(basal_start, basal_end, basal_ticks, ts_ticks)

        for bolus_start, bolus_amount in self.boluses:
            append_bolus_ticks(bolus_start, bolus_amount, ts_ticks)

        #        t = 0.0
        for i in range(0, len(ts_ticks)):
            #            t += 0.05
            deliveries.append(0.05)

        return pd.Series(deliveries, index=pd.to_datetime(ts_ticks, unit='s', origin='unix', utc=True))

    def start(self,
              ts: float, minute: int,
              total_delivered: float, total_undelivered: float, reservoir_remaining: float,
              basal_rate: float, activation_date: float):
        self.basal_rate = self._pi(basal_rate)
        self.start_delivery_offset = self._pi(total_delivered)
        self.start_ts = ts
        self.activation_ts = activation_date
        self._add_entry(ts, minute, total_delivered, total_undelivered, reservoir_remaining)

    def deactivate(self,
                   ts: float, minute: int,
                   total_delivered: float, total_undelivered: float, reservoir_remaining: float):
        self.bolus_end(ts, minute, total_delivered, total_undelivered, reservoir_remaining)
        self.temp_basal_end(ts, minute, total_delivered, total_undelivered, reservoir_remaining)
        self.end_ts = ts
        self.ended = True

    def remove(self):
        ts, minute, delivered, undelivered, reservoir = self.last_entry
        self.deactivate(ts, minute, delivered, undelivered, reservoir)

    def fail(self,
             ts: float, minute: int,
             total_delivered: float, total_undelivered: float, reservoir_remaining: float,
             failed_minute: int):
        self.bolus_end(ts, failed_minute, total_delivered, total_undelivered, reservoir_remaining)
        self.temp_basal_end(ts, failed_minute, total_delivered, total_undelivered, reservoir_remaining)
        self.end_ts = self.activation_ts + (failed_minute * 60) + 59
        self.ended = True

    def temp_basal_start(self,
                         ts: float, minute: int,
                         total_delivered: float, total_undelivered: float, reservoir_remaining: float,
                         temp_basal_rate: float, temp_basal_minutes: int):
        if len(self.temp_basals) > 0:
            last_rate_start, last_rate_end, last_rate = self.temp_basals[-1]
            if last_rate_end > ts:
                self.temp_basals[-1] = (last_rate_start, ts, last_rate)

        self._add_entry(ts, minute, total_delivered, total_undelivered, reservoir_remaining)
        self.temp_basal_start_min = minute
        self.temp_basal_end_min = minute + temp_basal_minutes
        self.temp_basal_total = self._pi(temp_basal_rate * temp_basal_minutes / 60)

        self.temp_basals.append((ts, ts + temp_basal_minutes * 60, self._pi(temp_basal_rate)))

    def temp_basal_end(self,
                       ts: float, minute: int,
                       total_delivered: float, total_undelivered: float, reservoir_remaining: float):
        if len(self.temp_basals) > 0:
            last_rate_start, last_rate_end, last_rate = self.temp_basals[-1]
            if last_rate_end > ts:
                self.temp_basals[-1] = (last_rate_start, ts, last_rate)
        self._add_entry(ts, minute, total_delivered, total_undelivered, reservoir_remaining)
        self.temp_basal_start_min = None
        self.temp_basal_end_min = None
        self.temp_basal_total = None

    def bolus_start(self,
                    ts: float, minute: int,
                    total_delivered: float, total_undelivered: float, reservoir_remaining: float):
        self._add_entry(ts, minute, total_delivered, total_undelivered, reservoir_remaining)
        self.bolus_start_min = minute
        self.bolus_end_min = minute + int(self._pi(total_undelivered) / 30) + 1
        self.bolus_total = self._pi(total_undelivered)
        self.boluses.append((ts, self.bolus_total))

    def bolus_end(self,
                  ts: float, minute: int,
                  total_delivered: float, total_undelivered: float, reservoir_remaining: float):
        if self.bolus_start_min is not None:
            last_bolus_start, last_bolus = self.boluses[-1]
            self.boluses[-1] = (last_bolus_start, last_bolus - self._pi(total_undelivered))
        self._add_entry(ts, minute, total_delivered, total_undelivered, reservoir_remaining)
        self.bolus_start_min = None
        self.bolus_end_min = None
        self.bolus_total = None

    def entry(self,
              ts: float, minute: int,
              total_delivered: float, total_undelivered: float, reservoir_remaining: float):
        self._add_entry(ts, minute, total_delivered, total_undelivered, reservoir_remaining)

    def _update_reservoir(self, reservoir: float, delivered: float):
        ri = self._pi(reservoir)
        rv = ri + self._pi(delivered)
        if ri >= 1023 and rv < self.reservoir:
            return
        self.reservoir = rv

    def _add_entry(self, ts: float, minute: int, delivered: float, undelivered: float, reservoir: float):
        self.last_entry = ts, minute, delivered, undelivered, reservoir
        self._fill_missing_entries(minute, delivered, undelivered)
        self.deliveries[minute] = self._pi(delivered)
        self._update_reservoir(reservoir, delivered)
        baseline = ts - minute * 60
        if self.ts_baseline_min is None:
            self.ts_baseline_min = baseline
        if self.ts_baseline_max is None:
            self.ts_baseline_max = baseline

        self.ts_baseline_min = min(baseline, self.ts_baseline_min)
        self.ts_baseline_max = max(baseline, self.ts_baseline_max)

        # print("%.0f\t%.0f" % (self.ts_baseline_min, self.ts_baseline_max))
        self.fixed_stamps.append(ts)
        self.fixed_deliveries.append(delivered)

    def _fill_missing_entries(self, minute: int, delivered: float, undelivered: float):
        pass
        # d = self._pi(delivered)
        # u = self._pi(undelivered)
        #
        # if self.bolus_start_min and self.temp_basal_start_min:
        #     if self.bolus_end_min <= self.temp_basal_end_min:
        #         pass
        #     else:
        #         pass
        #
        # if self.bolus_start_min:
        #     if u == 0:
        #         self.bolus_start_min = None
        #         self.bolus_end_min = None
        #         self.bolus_total = None
        #     else:
        #         self.bolus_start_min = minute
        #         self.bolus_end_min = minute + int(undelivered / 30) + 1
        #         self.bolus_total = undelivered
        # elif self.temp_basal_start_min:
        #     if minute > self.temp_basal_end_min:
        #         self.temp_basal_start_min = None
        #         self.temp_basal_end_min = None
        #         self.temp_basal_total = None
        #     else:
        #         pass

    def _pi(self, fpv: float) -> int:
        return int(round(fpv / self.precision, 0))
