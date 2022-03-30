import datetime

import pandas as pd
from joblib import Parallel, delayed

from data_layer.time_seires.time_series_record import TimeSeriesRecord


class TimeSeriesBuilder:

    def __init__(self, features_data: pd.DataFrame, labels: pd.DataFrame):
        self._features_data = features_data.sort_values('timestamp')
        self._features_data['datetime'] = self._features_data['timestamp'].apply(self._timestamp_to_date_str_round_by_h)
        self._labels = labels
        self._labels = self._labels.set_index('session_id', drop=False)

        self._df_per_machine = None
        self._time_series_records = None

    def split_to_df_per_machine(self):
        gb_df = self._features_data.groupby('machine_id')
        def parallel_for(idx, machine_id):
            if idx % 10 == 0:
                print(f"Build records for machine #{idx}")
            return gb_df.get_group(machine_id)

        self._df_per_machine = Parallel(n_jobs=1)(delayed(parallel_for)(idx, machine_id)
                                                  for idx, machine_id in enumerate(set(self._labels['machine_id'])))

    def build_time_series_records(self):
        time_series_until_per_machine = self._labels[['machine_id', 'until', 'session_id']].groupby('machine_id').agg(
            {'until': lambda x: list(x), 'session_id': lambda x: list(x)})

        def parallel_for(idx, machine_df):
            if idx % 10 == 0:
                print(f"Build records for machine #{idx}")
            records = []
            untils = sorted(time_series_until_per_machine.loc[machine_df['machine_id'].iloc[0]]['until'])
            session_ids = sorted(time_series_until_per_machine.loc[machine_df['machine_id'].iloc[0]]['session_id'])
            sub_df = machine_df[machine_df['datetime'] <= untils[0]]
            records.append(TimeSeriesRecord(sub_df, self._labels.loc[session_ids[0]]))
            if len(session_ids) > 1:
                for datetime_start, datetime_end, session_id in zip(untils[:-1], untils[1:], session_ids[1:]):
                    sub_df = machine_df[machine_df['datetime'] <= datetime_end]
                    sub_df = sub_df[sub_df['datetime'] > datetime_start]
                    records.append(TimeSeriesRecord(sub_df, self._labels.loc[session_id]))
            return records

        self._time_series_records = Parallel(n_jobs=1)(delayed(parallel_for)(idx, machine_df)
                                                       for idx, machine_df in enumerate(self._df_per_machine))
        self._time_series_records = sum(self._time_series_records, [])
        return self._time_series_records

    @staticmethod
    def _timestamp_to_date_str_round_by_h(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:00:00')
