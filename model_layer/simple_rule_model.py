

class SimpleRuleModel:

    def __init__(self, target_recall):
        self._target_recall = target_recall
        self._thresholds = {}

    def fit(self, X, y):
        X = self._impute(X)
        bp = 0

    @staticmethod
    def _impute(X):
        return X.fillna(0)
