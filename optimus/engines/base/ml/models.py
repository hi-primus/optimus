from optimus.helpers.types import *


class BaseML:
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def linear_regression(self, features, target, *args, **kwargs) -> 'ModelType':
        """
        Fit a linear regression model. This ensure that the data is ready converting to numeric and dropping NA
        :param features:
        :param target: Column to predict
        :param args:
        :param kwargs:
        :return:
        """
        raise NotImplementedError(f"\"linear_regression\" is not available using {type(self.root).__name__}")

    def logistic_regression(self, features, target, *args, **kwargs) -> 'ModelType':
        """
        Runs a logistic regression for input DataFrame.
        :param target: Column to predict
        :return: DataFrame with logistic regression and prediction run.
        """
        raise NotImplementedError(f"\"logistic_regression\" is not available using {type(self.root).__name__}")

    def k_means(self, features, n_centers, *args, **kwargs) -> 'ModelType':
        raise NotImplementedError(f"\"k_means\" is not available using {type(self.root).__name__}")

    def logistic_regression_text(self, target) -> 'ModelType':
        """
        Runs a logistic regression for input (text) DataFrame.
        :param target: Column to predict
        :return: DataFrame with logistic regression and prediction run.
        """
        raise NotImplementedError(f"\"logistic_regression_text\" is not available using {type(self.root).__name__}")

    def decision_tree(self, features, target, **kwargs) -> 'ModelType':
        """
        Runs a decision tree classifier for input DataFrame.
        :param features: List of columns to select for prediction.
        :param target: Column to predict.
        :return: DataFrame with decision tree and prediction run.
        """
        raise NotImplementedError(f"\"decision_tree\" is not available using {type(self.root).__name__}")

    def pca(self, features, target, *args, **kwargs):
        raise NotImplementedError(f"\"pca\" is not available using {type(self.root).__name__}")

    def random_forest(self, features, target, *args, **kwargs) -> 'ModelType':
        """
        Runs a random forest classifier for input DataFrame.
        :param features: List of columns to select for prediction.
        :param target: Column to predict.
        :return: DataFrame with random forest and prediction run.
        """
        raise NotImplementedError(f"\"random_forest\" is not available using {type(self.root).__name__}")

    def gbt(self, features, target, **kwargs) -> 'ModelType':
        """
        Runs a gradient boosting tree classifier for input DataFrame.
        :param features: List of columns to select for prediction.
        :param target: Column to predict.
        :return: DataFrame with gradient boosting tree and prediction run.
        """
        raise NotImplementedError(f"\"gbt\" is not available using {type(self.root).__name__}")
