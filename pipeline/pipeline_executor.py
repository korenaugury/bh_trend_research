import logging

from configuration.config import Config
from utils.files_operations import is_file_exist, save_to_pickle, load_pickle
from utils.timer import Timer


class PipelineExecutor:

    def __init__(self, pipeline_class, clear_history=True, pipeline_path=None, save_state=True, pickle_data_store=True):

        self.pipeline_class = pipeline_class
        self.clear_history = clear_history
        self.pipeline_path = pipeline_path or Config.PIPELINE_PATH
        self.save_state = save_state
        self.pickle_data_store = pickle_data_store

        self.pipeline = self._get_pipeline(pipeline_class)

        self._validate_input_args()

    def execute(self, steps):

        for step in steps:

            step_name = step.__name__
            logging.info("================ Executing: " + step_name + "================")

            getattr(self.pipeline, step_name)()

            self._save_pipeline_if_needed()
            logging.info("================ Finished: " + step_name + "================")

    def _get_pipeline(self, pipeline_class):

        if self.clear_history:
            return pipeline_class()

        elif not is_file_exist(self.pipeline_path):
            return pipeline_class()

        else:
            return self._load_pipeline()

    def _save_pipeline_if_needed(self):

        if self.save_state:
            timer = Timer('saving pipeline...').start()

            save_to_pickle(self.pipeline, self.pipeline_path)

            timer.end()

    def _load_pipeline(self):

        timer = Timer('Loading pipeline...').start()

        pipeline = load_pickle(self.pipeline_path)

        timer.end()

        return pipeline

    def _validate_input_args(self):

        if self.save_state and self.pipeline_path is None:
            raise ValueError("Please specify pipeline pickle path")