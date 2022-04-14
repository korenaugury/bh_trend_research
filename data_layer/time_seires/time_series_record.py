import pandas as pd
from joblib import Parallel, delayed

from configuration.config import Config


class TimeSeriesRecord:

    def __init__(self, machine_event_df: pd.DataFrame, label: pd.Series):
        self._machine_event_df = machine_event_df
        self._label = label

        self.time_serieses = []

        self._slope = {}
        self._r2 = {}
        self.trend_scores = {feature: [] for feature in Config.AVAILABLE_FEATURES}

        self._build()

    @property
    def label(self):
        if pd.isna(self._label['review']):
            if pd.isna(self._label['orig_review']):
                return 'TN'
            return bool(self._label['orig_review'])
        if self._label['review'] == 'IGNORE':
            return None
        return self._label['review'] == 'True'

    def _build(self):

        gb_df = self._machine_event_df.groupby(['machine_id', 'component_id', 'bearing', 'plane'])
        meta_columns = ['component_id', 'bearing', 'plane', 'session_id']

        def parallel_for(group):
            df = gb_df.get_group(group)
            df.set_index('datetime', inplace=True)
            df = df[meta_columns + Config.AVAILABLE_FEATURES]
            return df

        self.time_serieses = Parallel(n_jobs=1)(delayed(parallel_for)(group)
                                                for group in gb_df.groups)

    def set_slope_and_r2(self, component_id, bearing, plane, slope_length, feature, slope, r2):
        if component_id not in self._slope:
            self._slope[component_id] = {}
            self._r2[component_id] = {}
        if bearing not in self._slope[component_id]:
            self._slope[component_id][bearing] = {}
            self._r2[component_id][bearing] = {}
        if plane not in self._slope[component_id][bearing]:
            self._slope[component_id][bearing][plane] = {}
            self._r2[component_id][bearing][plane] = {}
        if slope_length not in self._slope[component_id][bearing][plane]:
            self._slope[component_id][bearing][plane][slope_length] = {}
            self._r2[component_id][bearing][plane][slope_length] = {}

        self._slope[component_id][bearing][plane][slope_length][feature] = slope
        self._r2[component_id][bearing][plane][slope_length][feature] = r2
        self.trend_scores[feature].append(slope * r2)
