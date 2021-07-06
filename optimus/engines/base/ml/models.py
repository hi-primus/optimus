from optimus.helpers.types import ModelType

class ML:
    def __init__(self, root):
        self.root = root

    def linear_regression(self, features, target, *args, **kwargs) -> ModelType:
        pass

    def logistic_regression(self, features, target, *args, **kwargs) -> ModelType:
        pass

    def KMeans(self, features, n_centers, *args, **kwargs) -> ModelType:
        pass

    def logistic_regression_text(self, input_col) -> ModelType:
        pass

    def decision_tree(self, columns, input_col, **kwargs) -> ModelType:
        pass

    def random_forest(self, features, target, *args, **kwargs) -> ModelType:
        pass

    def gbt(self, columns, input_col, **kwargs) -> ModelType:
        pass
