import datetime

import pandas as pd
from bson import ObjectId
from pymongo import MongoClient

from data_layer.utils.mongo_credentials import USERNAME, PASSWORD
from utils.date_functions import str_to_datetime


class MongoAPI:
    def __init__(self, env='production'):
        self._host = f'mongodb://{USERNAME}:{PASSWORD}@'
        self._host += {
            'staging': 'staging-azure-shard-00-00.yaz2t.mongodb.net:27017,'
                       'staging-azure-shard-00-01.yaz2t.mongodb.net:27017,'
                       'staging-azure-shard-00-02.yaz2t.mongodb.net:27017/'
                       'test?replicaSet=atlas-4u4stp-shard-0&ssl=true&authSource=admin',
            'production': 'production-gcp-shard-00-00-kxuwc.gcp.mongodb.net:27017,'
                          'production-gcp-shard-00-01-kxuwc.gcp.mongodb.net:27017,'
                          'production-gcp-shard-00-02-kxuwc.gcp.mongodb.net:27017/'
                          'production?ssl=true&replicaSet=production-gcp-shard-0&authSource=admin'
        }[env]
        self._env = env

    def execute(self, collection_name, query, action='find', is_cursor=False):
        mongo_client = MongoClient(self._host, connect=True)
        try:
            mongodb_db = mongo_client[self._env]
            print(f"MongoDB env: {self._env}")
            print(f"MongoDB query: {collection_name}.{action}({query})")
            res = getattr(getattr(mongodb_db, collection_name), action)(query)
            if is_cursor:
                return res
            return list(res)
        finally:
            mongo_client.close()


class MongoReader:
    def __init__(self, env='production'):
        self._mongo_api = MongoAPI(env)

    def get_detections_in_window(self, machine_ids, since):
        collection_name = 'detections'

        machine_ids = [ObjectId(machine_id) for machine_id in machine_ids]
        query = {
            "context.machine._id":
                {'$in': machine_ids},
            'timestamp': {
                '$gte': str_to_datetime(since),
            },
            'review': {'$exists': True}
        }
        return pd.DataFrame(self._mongo_api.execute(collection_name, query))

    def get_mhi_changes_in_window(self, machine_ids: list, since: str):
        collection_name = 'machine_health_changes'

        machine_ids = [ObjectId(machine_id) for machine_id in machine_ids]
        query = {
            "machine._id":
                {'$in': machine_ids},
            'created_at':
                {'$gte': str_to_datetime(since)}
        }
        return pd.DataFrame(self._mongo_api.execute(collection_name, query))

    def get_active_machines(self, last_recorded='2022-04-01'):
        collection_name = 'machines'

        query = {
            'continuous': True,
            'baseline.status': 'finished',
            'lastRecorded.timestamp': {
                '$gte': str_to_datetime(last_recorded)
            }
        }
        return pd.DataFrame(self._mongo_api.execute(collection_name, query))

    def get_detector_trends(self, machine_id: str, timestamp: datetime.datetime):
        collection_name = 'detector_trends'
        machine_id = ObjectId(machine_id)
        query = {
            "detectorName": 'trend',
            "machineId": machine_id,
            'timestamp': {
                '$gte': timestamp,
                '$lt': timestamp + datetime.timedelta(days=1)
                },
        }
        return pd.DataFrame(self._mongo_api.execute(collection_name, query))
