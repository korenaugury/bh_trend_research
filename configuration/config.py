import os
from pathlib import Path


ROOT_DIR = Path(__file__).parent.parent
WORK_DIR = '/Users/kgast/work_dir'


class Config:

    FAST_RUN: bool
    PROJECT_PATH: str
    PIPELINE_PATH: str

    FEATURES_DATA_PATH: str
    LABELS_DATA_PATH: str

    AVAILABLE_FEATURES: list

    SEED: int = 777

    def __init__(self, fast_run, pipeline_unique_name=None):
        self.__class__.FAST_RUN = fast_run

        pipeline_pkl_name = pipeline_unique_name or 'Pipeline'
        if fast_run:
            pipeline_pkl_name += '_FAST_RUN.pkl'
        else:
            pipeline_pkl_name += '.pkl'
        self.__class__.PROJECT_PATH = ROOT_DIR
        self.__class__.PIPELINE_PATH = os.path.join(ROOT_DIR, 'pipeline', pipeline_pkl_name)

        self.__class__.FEATURES_DATA_PATH = os.path.join(WORK_DIR, 'bh_trend', '1648122343', 'output.csv')
        # self.__class__.FEATURES_DATA_PATH = 'gs://augury-datasets-research/bh-trend-research/1648122343/output.csv'
        """
                The FEATURES_DATA_PATH is a path to the output from get-data repo run using the triggers file: 
                  data_layer/prerequisites/triggers_file.csv
        """
        self.__class__.LABELS_DATA_PATH = os.path.join(ROOT_DIR, 'data_layer', 'prerequisites',
                                                       'trend_cleaned_relabelling.csv')

        self.__class__.AVAILABLE_FEATURES = ['vibration_vel_rms', 'vibration_acc_p2p']


