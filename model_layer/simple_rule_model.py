from sklearn.base import ClassifierMixin, BaseEstimator

from configuration.config import Config


class SimpleRuleModel(BaseEstimator, ClassifierMixin):

    def __init__(self, target_recall):
        self._target_recall = target_recall
        self._thresholds = {}

    def fit(self, X, y):
        X = X[Config.AVAILABLE_FEATURES]
        X = self._impute(X)
        bp = 0

    @staticmethod
    def _impute(X):
        return X.fillna(0)
