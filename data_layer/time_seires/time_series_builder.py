import datetime

import pandas as pd
from joblib import Parallel, delayed

from data_layer.time_seires.time_series_record import TimeSeriesRecord
from utils.date_functions import calc_time_delta


class TimeSeriesBuilder:

    def __init__(self, features_data: pd.DataFrame, labels: pd.DataFrame):
        self._features_data = features_data.sort_values('timestamp')
        self._features_data['datetime'] = self._features_data['timestamp'].apply(self._timestamp_to_date_str_round_by_h)
        self._labels = labels

        self._df_per_machine = None
        self._time_series_records = []

    def split_to_df_per_machine(self):
        gb_df = self._features_data.groupby('machine_id')

        def parallel_for(idx, machine_id):
            if idx % 10 == 0:
                print(f"Build records for machine #{idx}")
            return gb_df.get_group(machine_id)

        self._df_per_machine = Parallel(n_jobs=1)(delayed(parallel_for)(idx, machine_id)
                                                  for idx, machine_id in enumerate(gb_df.groups))

    def build_time_series_records(self):
        self._time_series_records = []
        time_series_until_per_machine = self._labels[['machine_id', 'until', 'session_id']].groupby('machine_id').agg(
            {'until': lambda x: list(x), 'session_id': lambda x: list(x)})

        def parallel_for(idx, machine_df):
            if idx % 10 == 0:
                print(f"Build records for machine #{idx}")
            records = []
            untils = sorted(time_series_until_per_machine.loc[machine_df['machine_id'].iloc[0]]['until'])

            sub_df = machine_df[machine_df['datetime'].apply(lambda rec_dt: calc_time_delta(rec_dt, untils[0])) <= 45]
            label = self._get_label(machine_id=machine_df['machine_id'].values[0],
                                    dt=machine_df['datetime'].max())
            if label is None:
                records.append(None)
            else:
                records.append(TimeSeriesRecord(sub_df, label))
            if len(untils) > 1:
                for datetime_start, datetime_end in zip(untils[:-1], untils[1:]):
                    sub_df = machine_df[machine_df['datetime'] <= datetime_end]
                    sub_df = sub_df[sub_df['datetime'] > datetime_start]
                    label = self._get_label(machine_id=machine_df['machine_id'].values[0],
                                            dt=machine_df['datetime'].max())
                    if label is None:
                        records.append(None)
                    else:
                        records.append(TimeSeriesRecord(sub_df, label))
            return records

        time_series_records = Parallel(n_jobs=1)(delayed(parallel_for)(idx, machine_df)
                                                 for idx, machine_df in enumerate(self._df_per_machine))
        time_series_records = sum(time_series_records, [])
        no_labels_counter = 0
        for record in time_series_records:
            if record is not None:
                self._time_series_records.append(record)
            else:
                no_labels_counter += 1
        print(f"{no_labels_counter} records had no label")
        return self._time_series_records

    def _get_label(self, machine_id, dt):
        machine_labels = self._labels[self._labels['machine_id'] == machine_id]
        date_diff = machine_labels['until'].apply(lambda x: abs(calc_time_delta(x, dt)))
        if date_diff.min() <= 7:
            return machine_labels.loc[date_diff.idxmin()]

    @staticmethod
    def _timestamp_to_date_str_round_by_h(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:00:00')
