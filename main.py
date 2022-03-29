import logging

from configuration.config import Config
from pipeline.bh_trend_pipeline import BHTrendPipeline
from pipeline.pipeline_executor import PipelineExecutor

CLEAR_HISTORY = False
SAVE_STATE = True

FAST_RUN = True

STEPS = [
    # BHTrendPipeline.load_data,
    # BHTrendPipeline.split_per_machine,
    # BHTrendPipeline.build_records,
    BHTrendPipeline.calc_slope_and_r2,
]

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
    Config(fast_run=FAST_RUN)
    executor = PipelineExecutor(
        pipeline_class=BHTrendPipeline,
        clear_history=CLEAR_HISTORY,
        save_state=SAVE_STATE,
    )

    executor.execute(STEPS)
