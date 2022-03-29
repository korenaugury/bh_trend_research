import numpy as np
import pandas as pd

from configuration.config import Config
from data_layer.processing.slope_calculator import SlopeCalculator
from data_layer.time_seires.time_series_builder import TimeSeriesBuilder


class BHTrendPipeline:

    def __init__(self):
        self._features_data = None
        self._labels = None
        self._time_series_builder = None
        self._time_series_records = None
        self._slope_calculator = None

    def load_data(self):
        self._features_data = pd.read_csv(Config.FEATURES_DATA_PATH)
        self._labels = pd.read_csv(Config.LABELS_DATA_PATH)

        if Config.FAST_RUN:
            machine_ids = np.random.choice(self._labels['machine_id'], 200, replace=False)
            self._features_data = self._features_data[self._features_data['machine_id'].isin(machine_ids)]
            self._labels = self._labels[self._labels['machine_id'].isin(machine_ids)]

    def split_per_machine(self):
        self._time_series_builder = TimeSeriesBuilder(
            features_data=self._features_data,
            labels=self._labels,
        )
        self._time_series_builder.split_to_df_per_machine()

    def build_records(self):
        self._time_series_records = self._time_series_builder.build_time_series_records()

    def calc_slope_and_r2(self):
        self._slope_calculator = SlopeCalculator(self._time_series_records)
        self._slope_calculator.calculate()
