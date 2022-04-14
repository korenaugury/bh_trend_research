import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from itertools import combinations

from configuration.config import Config


class Analyzer:

    def __init__(self, X, y):
        self._X = X
        self._y = y

    def analyze(self):
        df = self._X.copy(deep=True)
        df['y'] = self._y
        df = df[~(df['y'] == False)]
        df['y'] = df['y'].apply(lambda x: False if x == 'TN' else x)
        self._compare_hist(df)
        self._compare_2d_hist(df)

    @staticmethod
    def _compare_hist(df):
        for feature in Config.AVAILABLE_FEATURES:
            feature_df = df[[feature, 'y']]
            feature_df[~feature_df['y']][feature].hist(label='false', bins=50, density=True)
            feature_df[feature_df['y']][feature].hist(label='true', bins=50, alpha=0.6, density=True)
            plt.legend()
            plt.title(feature)
            plt.show()

    @staticmethod
    def _compare_2d_hist(df):
        for feature_pair in list(combinations(Config.AVAILABLE_FEATURES, 2)):
            sns.displot(df, x=feature_pair[0], y=feature_pair[1], kind="kde", hue="y")
            plt.show()
