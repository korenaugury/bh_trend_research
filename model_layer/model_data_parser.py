import pandas as pd

from configuration.config import Config


class ModelDataParser:

    def __init__(self, time_series_records, labels):
        self._time_series_records = time_series_records
        self._labels = labels

        self.X = None
        self.y = None

    def parse(self):
        parsed_records = []
        labels = []
        for idx, record in enumerate(self._time_series_records):
            if idx % 50 == 0:
                print(f"Parsing record #{idx}")
            label = record.label
            if label is None:
                continue
            parsed_record = {'session_id': record._label['session_id']}
            for feature in Config.AVAILABLE_FEATURES:
                parsed_record[feature] = max(record.trend_scores[feature])
            parsed_records.append(parsed_record)
            labels.append(label)
        self.X = pd.DataFrame(parsed_records)
        self.y = pd.Series(labels)
