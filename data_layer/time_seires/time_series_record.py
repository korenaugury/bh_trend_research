import datetime

import numpy as np
import pandas as pd

from utils.date_functions import move_date


class TimeSeriesRecord:

    def __init__(self, time_series_df: pd.DataFrame, label: pd.Series):
        self._time_series_df = time_series_df
        self._label = label

        self.pivot_data = {}

        self._slope = {}
        self._r2 = {}

        self._build()

    def _build(self):

        for component_id in self._time_series_df['component_id'].unique():
            if component_id not in self.pivot_data:
                self.pivot_data[component_id] = {}
                self._slope[component_id] = {}
                self._r2[component_id] = {}
            for bearing in self._time_series_df['bearing'].unique():
                if bearing not in self.pivot_data[component_id]:
                    self.pivot_data[component_id][bearing] = {}
                    self._slope[component_id][bearing] = {}
                    self._r2[component_id][bearing] = {}
                bearing_df = self._time_series_df[(self._time_series_df['component_id'] == component_id) &
                                                  (self._time_series_df['bearing'] == bearing)]
                if not len(bearing_df):
                    continue
                temperature_series = bearing_df[['datetime', 'temperature_eptemp']][~bearing_df['temperature_eptemp'].isna()]
                temperature_series = temperature_series.set_index('datetime')
                if not len(temperature_series):
                    temperature_series.loc[bearing_df['datetime'].max()] = np.nan
                self.pivot_data[component_id][bearing]['temperature'] = self._fill_time_series(temperature_series)

                for plane in range(3):
                    if f'plane_{plane}' not in self.pivot_data[component_id][bearing]:
                        self.pivot_data[component_id][bearing][f'plane_{plane}'] = {}
                        self._slope[component_id][bearing][f'plane_{plane}'] = {}
                        self._r2[component_id][bearing][f'plane_{plane}'] = {}
                    for feature in ['vibration_vel_rms', 'vibration_acc_p2p']:
                        feature_series = bearing_df[['datetime', feature]][bearing_df['plane'] == plane]
                        feature_series = feature_series.set_index('datetime')
                        if not len(feature_series):
                            feature_series.loc[bearing_df['datetime'].max()] = np.nan
                        self.pivot_data[component_id][bearing][f'plane_{plane}'][feature] = self._fill_time_series(feature_series)

    @staticmethod
    def _fill_time_series(time_series):

        until = time_series.index.max()
        since = move_date(until, -45)

        to_add = []

        dt = since
        while dt <= until:
            if dt not in time_series.index:
                to_add.append({'datetime': dt, list(time_series.columns)[0]: np.nan})
            dt = move_date(dt, 1, 'hours')
        time_series = time_series.append(pd.DataFrame(to_add).set_index('datetime'))
        time_series = time_series.sort_index()
        return time_series

    def set_slope_and_r2(self, component_id, bearing, plane, feature, slope, r2):
        self._slope[component_id][bearing][plane][feature] = slope
        self._r2[component_id][bearing][plane][feature] = r2


