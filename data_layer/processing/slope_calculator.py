import numpy as np
import pandas as pd
from scipy.stats import linregress
from sklearn.preprocessing import MinMaxScaler


class SlopeCalculator:

    def __init__(self, time_series_records):
        self._time_series_records = time_series_records

    def calculate(self):
        for idx, record in enumerate(self._time_series_records):
            if idx % 50 == 0:
                print(f"Calculation slope for record #{idx}")
            pivot_data = record.pivot_data
            for component_id in pivot_data:
                for bearing in pivot_data[component_id]:
                    for plane in pivot_data[component_id][bearing]:
                        if 'plane' not in plane:
                            continue
                        for feature in pivot_data[component_id][bearing][plane]:
                            feature_time_series = pivot_data[component_id][bearing][plane][feature]
                            times = np.array(feature_time_series.index)
                            values = feature_time_series[feature].values

                            temp_df = pd.DataFrame({'times': times, 'values': values}).sort_values(by='times')
                            temp_df['values'] = self._scale(temp_df['values'])
                            temp_df['values'] = self._remove_outliers(temp_df['values'], percentiles=[0.05, 0.9])
                            temp_df = temp_df[~pd.isna(temp_df['values'])]

                            if len(temp_df) < 10:
                                slope, r2 = np.nan, np.nan
                            else:
                                W = 12
                                smoothed_values = self._moving_average(temp_df['values'].values, W)
                                smoothed_times = temp_df['times'].values[W - 1:]

                                if len(temp_df) < W:
                                    slope, r2 = np.nan, np.nan
                                else:
                                    slope, r2 = self._calc_slope(smoothed_times, smoothed_values)

                            record.set_slope_and_r2(component_id, bearing, plane, feature, slope, r2)

    @staticmethod
    def _remove_outliers(values, percentiles):
        values = values[values.between(values.quantile(percentiles[0]),
                                       values.quantile(percentiles[1]), inclusive="both")]
        return values

    @staticmethod
    def _moving_average(x, w=12):
        return np.convolve(x, np.ones(w), 'valid') / w

    @staticmethod
    def _calc_slope(x, smoothed_values):
        results = linregress(list(range(len(x))), smoothed_values)
        slope, r2 = results.slope, results.rvalue ** 2
        return slope * len(x), r2

    @staticmethod
    def _scale(values):
        if values.max() <= values.min():
            return values
        if len(values) < 2:
            return values
        return MinMaxScaler(feature_range=(0, 1)).fit_transform(values.values.reshape(-1, 1))
