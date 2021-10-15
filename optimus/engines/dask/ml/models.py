from dask_ml.cluster import KMeans
from dask_ml.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib

from optimus.engines.base.ml.models import BaseML
from optimus.helpers.columns import parse_columns
from optimus.helpers.types import *


class ML(BaseML):
    def __init__(self, root: 'DataFrameType'):
        super().__init__(root)
        self.root = root

    def linear_regression(self, features, target, *args, **kwargs):
        """
        Fit a linear regression model. This ensure that the data is ready converting to numeric and dropping NA
        :param features:
        :param target:
        :param args:
        :param kwargs:
        :return:
        """
        df = self.root
        lm = LinearRegression(*args, **kwargs)

        features = parse_columns(df, features)
        target = parse_columns(df, target)

        df = df.cols.select(features + target).cols.to_float().rows.drop_missings()
        X_train = df[features]._to_values()
        y_train = df[target]._to_values()

        return lm.fit(X_train, y_train)

    def logistic_regression(self, features, target, *args, **kwargs):
        df = self.root
        lm = LogisticRegression(*args, **kwargs)

        features = parse_columns(df, features)
        target = parse_columns(df, target)

        df = df.cols.select(features + target).cols.to_float().rows.drop_missings()
        # LogisticRegression requires that the Dask Array passed to it has known chunk sizes
        # https://stackoverflow.com/questions/61756328/dusk-ml-logisticregression-throws-this-error-notimplementederror-can-not-add
        X_train = df[features]._to_values().compute_chunk_sizes()
        y_train = df[target]._to_values().compute_chunk_sizes()

        return lm.fit(X_train, y_train)

    def k_means(self, features, n_centers, *args, **kwargs):
        df = self.root
        k_means = KMeans(n_clusters=n_centers)
        features = parse_columns(df, features)
        df = df.cols.select(features).cols.to_float().rows.drop_missings()
        X_train = df[features]._to_values().compute_chunk_sizes()
        print(X_train)
        k_means.fit(X_train)
        return

    def random_forest(self, features, target, *args, **kwargs):
        """
        Runs a random forest classifier for input DataFrame.
        :return: Model with random forest and prediction run.
        """
        df = self.root
        da_rf = RandomForestClassifier(*args, **kwargs)

        df = df.cols.select(features + target).cols.to_float().rows.drop_missings()
        # LogisticRegression requires that the Dask Array passed to it has known chunk sizes
        # https://stackoverflow.com/questions/61756328/dusk-ml-logisticregression-throws-this-error-notimplementederror-can-not-add
        X = df[features]._to_values().compute_chunk_sizes()
        y = df[target]._to_values().compute_chunk_sizes()

        with joblib.parallel_backend("dask"):
            da_rf.fit(X, y)
        return da_rf
