import os

import numpy as np
import pandas as pd
import datetime

from joblib import Parallel, delayed

from configuration.config import Config
from data_layer.prerequisites.ground_truth_creator import GroundTruthCreator
from data_layer.utils.mongo_data_reader import MongoReader
from utils.date_functions import str_to_datetime


class TriggersCreator:
    def __init__(self):
        np.random.seed(0)
        self._mongo_reader = MongoReader()

        self._active_machines = self._get_active_machines()
        self._labels = self._get_labels()
        self._fp_tp_triggers = None
        self._tn_triggers = None

    def create(self):
        self._fp_tp_triggers = self._get_fp_tp_triggers()
        self._tn_triggers = self._get_tn_triggers()
        pd.concat([self._fp_tp_triggers, self._tn_triggers]).to_csv('triggers_file.csv', index=False)

    def _get_labels(self):
        gt_creator = GroundTruthCreator()
        gt_creator.create()
        return gt_creator.ground_truth

    def _get_fp_tp_triggers(self):
        df = self._labels[['machine_id', 'until']].copy()
        df.columns = ['machine_id', 'end']

        df['start'] = df['end'].apply(lambda dt: dt - datetime.timedelta(days=45))
        df['end'] = df['end'].apply(lambda dt: dt + datetime.timedelta(days=1))

        df['start'] = df['start'].apply(lambda dt: dt.strftime("%Y/%m/%d/%H"))
        df['end'] = df['end'].apply(lambda dt: dt.strftime("%Y/%m/%d/%H"))

        df = df[['machine_id', 'start', 'end']]
        df.to_csv(Config.POS_TRIGGERS_LOCAL_PATH, index=False)
        return df[['machine_id', 'start', 'end']]

    def _get_tn_triggers(self):

        true_machines = list(self._fp_tp_triggers['machine_id'].values)
        false_machines = [machine_id for machine_id in self._active_machines.index if machine_id not in true_machines]

        true_machines = np.random.choice(true_machines, len(true_machines)//2, replace=False)
        false_machines = np.random.choice(false_machines, len(true_machines), replace=True)

        all_machines = list(true_machines) + list(false_machines)
        all_dates = [str_to_datetime(Config.SINCE) + datetime.timedelta(days=i) for i in range(Config.N_DAYS)]

        events_ts_per_machine = self._get_events_ts_per_machine(all_machines)

        def parallel_for(idx, machine_id):
            print(f"Fetching TN sessions for machine #{idx}")
            for _ in range(10):
                session_date = np.random.choice(all_dates)
                if machine_id in events_ts_per_machine:
                    distances_to_anomalies = [abs((ts - session_date).total_seconds()) for ts in events_ts_per_machine[machine_id]]
                    if min(distances_to_anomalies) < 10 * 24 * 60 * 60:  # 45 days
                        continue
                detector_trends = self._mongo_reader.get_detector_trends(
                    machine_id=machine_id,
                    timestamp=session_date
                )

                if len(detector_trends) == 0:
                    continue

                chosen_record = detector_trends.loc[np.random.choice(detector_trends.index)]
                return {
                    'machine_id': machine_id,
                    'start': chosen_record['timestamp'] - datetime.timedelta(days=45),
                    'end': chosen_record['timestamp'] + datetime.timedelta(days=1),
                }

        records = Parallel(n_jobs=-1)(delayed(parallel_for)(idx, machine_id) for idx, machine_id in enumerate(all_machines))
        records = [record for record in records if record]
        df = pd.DataFrame(records)
        df['start'] = df['start'].apply(lambda dt: dt.strftime("%Y/%m/%d/%H"))
        df['end'] = df['end'].apply(lambda dt: dt.strftime("%Y/%m/%d/%H"))
        self._update_labels(df.copy())
        return df

    def _update_labels(self, neg_df):
        neg_df = neg_df[['machine_id', 'end']].copy()
        neg_df['end'] = neg_df['end'].apply(lambda x: x.replace('/', '-'))
        neg_df['end'] = neg_df['end'].apply(lambda x: f"{x[:10]} {x[-2:]}:00:00")
        neg_df.columns = ['machine_id', 'until']
        self._labels = self._labels.append(neg_df)
        self._labels.to_csv('final_labels.csv')

    def _get_active_machines(self):
        if os.path.isfile(Config.ACTIVE_MACHINES_LOCAL_PATH):
            return pd.read_csv(Config.ACTIVE_MACHINES_LOCAL_PATH, index_col='_id')
        machine_records = self._mongo_reader.get_active_machines()
        machine_records.set_index('_id', inplace=True)
        machine_records.to_csv(Config.ACTIVE_MACHINES_LOCAL_PATH)
        return machine_records

    def _get_events_ts_per_machine(self, all_machines):
        detections = self._mongo_reader.get_detections_in_window(
            machine_ids=list(set(all_machines)),
            since=Config.SINCE
        )
        detections['machine_id'] = detections['context'].apply(lambda x: str(x['machine']['_id']))
        detections = detections[['machine_id', 'timestamp']]

        mhi_changes = self._mongo_reader.get_mhi_changes_in_window(
            machine_ids=list(set(all_machines)),
            since=Config.SINCE
        )
        mhi_changes['machine_id'] = mhi_changes['machine'].apply(lambda x: str(x['_id']))
        mhi_changes = mhi_changes[['machine_id', 'created_at']]
        mhi_changes.columns = ['machine_id', 'timestamp']

        events = mhi_changes.append(detections).groupby('machine_id').agg(
            {'timestamp': lambda x: sorted(set(x))})
        return events.to_dict()['timestamp']


if __name__ == '__main__':
    Config()
    TriggersCreator().create()