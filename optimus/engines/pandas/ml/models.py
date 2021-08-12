from time import time
import joblib
import matplotlib.pyplot as plt
import numpy as np
from sklearn import decomposition
from sklearn import metrics
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_validate
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from optimus.helpers.types import *
from optimus.engines.base.ml.models import BaseML

from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import format_dict
from optimus.helpers.core import val_to_list
from optimus.infer import is_numeric


class Model:
    def __init__(self, model, X_train, y_train, X_test, y_test, **kwargs):
        self.model = model
        self.X_train = X_train
        self.y_train = y_train

        self.X_test = X_test
        self.y_test = y_test
        self.y_pred = None
        self.score = None
        self.evaluation = None
        self.kwargs = kwargs
        # print("111",self.kwargs)

    def predict(self, value):
        """

        :param value: Can be a value a list of features inside a list
        :return:
        """
        if is_numeric(value):
            value = [[value]]

        return list(self.model.predict(value))

    def predict_proba(self, value):
        return self.model.predict_proba(value).tolist()

    def roc_auc(self):
        from yellowbrick.classifier import ROCAUC
        visualizer = ROCAUC(self.model, classes=["0", "1", "2"])

        visualizer.fit(self.X_train, self.y_train)  # Fit the training data to the visualizer
        visualizer.score(self.X_test, self.y_test)  # Evaluate the model on the test data
        visualizer.show()  # Finalize and show the figure

    def precision_recall(self):
        from yellowbrick.classifier import PrecisionRecallCurve
        viz = PrecisionRecallCurve(
            self.model,
            per_class=False,
            cmap="Set1"
        )
        viz.fit(self.X_train, self.y_train)
        viz.score(self.X_test, self.y_test)
        viz.show()

    def confusion_matrix(self):
        from yellowbrick.classifier import ConfusionMatrix
        cm = ConfusionMatrix(self.model, classes=[0, 1, 2])
        cm.fit(self.X_train, self.y_train)
        cm.score(self.X_test, self.y_test)

        cm.show()

        # y_pred = self.model.predict(self.X_test)
        # return metrics.confusion_matrix(self.y_test, y_pred)

    def coef(self):
        return list(self.model.coef_)

    def intercept(self):
        return self.model.intercept_

    def scores(self):
        return self.score

    def evaluate(self):
        return self.evaluation
        # print("%0.2f accuracy with a standard deviation of %0.2f" % (scores.mean(), scores.std()))

    # def score(self):
    #     return self.model.score(self.X_test, self.y_test)

    def plot(self):
        y_pred = self.model.predict(self.X_test)
        # plt.figure(dpi=100)
        plt.scatter(self.X_test, self.y_test, color='black')
        plt.plot(self.X_test, y_pred, color='blue', linewidth=3)

        plt.xticks(())
        plt.yticks(())

        plt.show()

    def plot_PCA(self):
        final_df = self.root.data
        fig = plt.figure(figsize=(8, 8))
        ax = fig.add_subplot(1, 1, 1)
        ax.set_xlabel('Principal Component 1', fontsize=15)
        ax.set_ylabel('Principal Component 2', fontsize=15)

        ax.set_title('2 component PCA', fontsize=20)
        targets = ['Iris-setosa', 'Iris-versicolor', 'Iris-virginica']
        colors = ['r', 'g', 'b']
        for target, color in zip(targets, colors):
            indicesToKeep = final_df['target'] == target
            ax.scatter(final_df.loc[indicesToKeep, 'principal component 1']
                       , final_df.loc[indicesToKeep, 'principal component 2']
                       , c=color
                       , s=50)
        ax.legend(targets)
        ax.grid()

    def plot_elbow(self, start=4, end=12):
        from yellowbrick.cluster import KElbowVisualizer
        visualizer = KElbowVisualizer(self.model, k=(start, end))

        visualizer.fit(self.X_test)  # Fit the data to the visualizer
        visualizer.show()
        #
        # X =self.X_test
        # distorsions = []
        # for k in range(2, total_clusters):
        #     kmeans = KMeans(n_clusters=total_clusters)
        #     kmeans.fit(X)
        #     distorsions.append(kmeans.inertia_)
        #
        # fig = plt.figure(figsize=(15, 5))
        # plt.plot(range(2, total_clusters), distorsions)
        # plt.grid(True)
        # plt.title('Elbow curve')

    def plot_clusters(self):
        reduced_data = PCA(n_components=2).fit_transform(self.X_test)
        kmeans = self.model
        kmeans.fit(reduced_data)

        # Step size of the mesh. Decrease to increase the quality of the VQ.
        h = .02  # point in the mesh [x_min, x_max]x[y_min, y_max].

        # Plot the decision boundary. For that, we will assign a color to each
        x_min, x_max = reduced_data[:, 0].min() - 1, reduced_data[:, 0].max() + 1
        y_min, y_max = reduced_data[:, 1].min() - 1, reduced_data[:, 1].max() + 1
        xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))

        # Obtain labels for each point in mesh. Use last trained model.
        Z = kmeans.predict(np.c_[xx.ravel(), yy.ravel()])

        # Put the result into a color plot
        Z = Z.reshape(xx.shape)
        plt.figure(1)
        plt.clf()
        plt.imshow(Z, interpolation="nearest",
                   extent=(xx.min(), xx.max(), yy.min(), yy.max()),
                   cmap=plt.cm.Paired, aspect="auto", origin="lower")

        plt.plot(reduced_data[:, 0], reduced_data[:, 1], 'k.', markersize=5)
        # Plot the centroids as a white X
        centroids = kmeans.cluster_centers_
        plt.scatter(centroids[:, 0], centroids[:, 1], marker="x", s=169, linewidths=3,
                    color="w", zorder=10)
        # plt.title("K-means clustering on the digits dataset (PCA-reduced data)\n"
        #           "Centroids are marked with white cross")
        plt.xlim(x_min, x_max)
        plt.ylim(y_min, y_max)
        plt.xticks(())
        plt.yticks(())
        plt.show()

    def save(self, path):
        joblib.dump(self.model, path)


class ML(BaseML):
    def __init__(self, root: 'DataFrameType'):
        self.root = root

    @staticmethod
    def _split(X, y, **kwargs):
        test_size = kwargs.pop('test_size', 0.2)
        train_size = kwargs.pop('train_size', None)
        random_state = kwargs.pop('random_state', 0)
        # TODO: in case of categorilca data stratify stratify = kwargs.pop('stratify', y)
        stratify = kwargs.pop('stratify', None)
        shuffle = kwargs.pop('shuffle', True)
        return train_test_split(X, y,
                                test_size=test_size,
                                train_size=train_size,
                                random_state=random_state,
                                stratify=stratify,
                                shuffle=shuffle)

    @staticmethod
    def _bench_k_means(kmeans, data, labels):
        # Reference https://scikit-learn.org/stable/auto_examples/cluster/plot_kmeans_digits.html
        """Benchmark to evaluate the KMeans initialization methods.

        Parameters
        ----------
        kmeans : KMeans instance
            A :class:`~sklearn.cluster.KMeans` instance with the initialization
            already set.
        name : str
            Name given to the strategy. It will be used to show the results in a
            table.
        data : ndarray of shape (n_samples, n_features)
            The data to cluster.
        labels : ndarray of shape (n_samples,)
            The labels used to compute the clustering metrics which requires some
            supervision.
        """
        t0 = time()
        estimator = make_pipeline(StandardScaler(), kmeans).fit(data)
        fit_time = time() - t0
        results = {}

        results.update({"inertia": estimator[-1].inertia_})
        # Define the metrics which require only the true labels and estimator
        # labels
        clustering_metrics = [
            metrics.homogeneity_score,
            metrics.completeness_score,
            metrics.v_measure_score,
            metrics.adjusted_rand_score,
            metrics.adjusted_mutual_info_score,
        ]
        results.update({m.__name__: m(labels, estimator[-1].labels_) for m in clustering_metrics})

        # The silhouette score requires the full dataset
        results["silhouette_score"] = metrics.silhouette_score(data, estimator[-1].labels_,
                                                               metric="euclidean", sample_size=300, )

        return results

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

        features = parse_columns(df, features)
        target = parse_columns(df, target)

        df = df.cols.select(features + target).cols.to_float().rows.drop_missings()
        X = df[features]._to_values()
        y = df[target]._to_values()

        X_train, X_test, y_train, y_test = self._split(X, y, **kwargs)

        fit_intercept = kwargs.pop('fit_intercept', 20)
        normalize = kwargs.pop('normalize', "mse")
        copy_X = kwargs.pop('copy_X', None)
        test_size = kwargs.pop('test_size', 0.2)
        n_jobs = kwargs.pop('n_jobs', 2)

        lm = LinearRegression(fit_intercept=fit_intercept,
                              normalize=normalize,
                              copy_X=copy_X,
                              n_jobs=n_jobs
                              )

        kf = KFold(n_splits=5, shuffle=True, random_state=100)
        scoring = {"neg_mean_absolute_error": "neg_mean_absolute_error",
                   "neg_mean_squared_error": "neg_mean_squared_error",
                   "neg_root_mean_squared_error": "neg_root_mean_squared_error",
                   "r2": "r2"
                   }

        score = cross_validate(lm, X_train, y_train.ravel(), cv=kf, scoring=scoring)

        fm = lm.fit(X_train, y_train.ravel())
        model = Model(fm, X_train, y_train, X_test, y_test)
        model.score = {i: list(score["test_" + i]) for i in scoring.keys()}
        model.evaluation = {"accuracy": lm.score(X, y),
                            "standard deviation": score["test_r2"].std()}

        return model

    def logistic_regression(self, features, target, *args, **kwargs):
        """
        Runs a logistic regression for input (text) DataFrame.
        :return: Model with logistic regression and prediction run.
        """

        df = self.root

        features = parse_columns(df, features)
        target = parse_columns(df, target)

        df = df.cols.select(features + target).cols.to_float().rows.drop_missings()
        X = df[features]._to_values()
        y = df[target]._to_values()

        X_train, X_test, y_train, y_test = self._split(X, y, **kwargs)

        fit_intercept = kwargs.pop('fit_intercept', True)
        penalty = kwargs.pop('solver', "l2")
        solver = kwargs.pop('solver', "liblinear")
        # For small datasets, ‘liblinear’ is a good choice, whereas ‘sag’ and ‘saga’ are faster for large ones.

        max_iter = kwargs.pop('max_iter', 100)
        n_jobs = kwargs.pop('n_jobs', 1)

        lm = LogisticRegression(
            multi_class="auto", solver="liblinear",
            fit_intercept=fit_intercept,
            penalty=penalty,
            # solver=solver,
            max_iter=max_iter,
            n_jobs=n_jobs
        )

        kf = KFold(n_splits=5, shuffle=True, random_state=100)
        score = cross_validate(lm, X_train, y_train.ravel(), cv=kf)

        #
        fm = lm.fit(X_train, y_train.ravel())
        model = Model(fm, X_train, y_train, X_test, y_test)
        model.evaluation = {"accuracy": fm.score(X, y),
                            "standard deviation": score["test_score"].mean()}
        #
        return model

    def k_means(self, features, target, n_clusters, methods="k-means++", *args, **kwargs):
        df = self.root

        features = parse_columns(df, features)
        target = parse_columns(df, target)

        df = df.cols.select(features + target).cols.to_float().rows.drop_missings()
        X = df[features]._to_values()
        y = df[target]._to_values().ravel()
        methods = val_to_list(methods)
        # methods = ["k-means++", "random"]
        scores = {}
        models = []

        X_train, X_test, y_train, y_test = self._split(X, y, **kwargs)

        for method in methods:
            kmeans = KMeans(init=method, n_clusters=n_clusters, n_init=4,
                            random_state=0)
            models.append(kmeans)
            scores[method] = self._bench_k_means(kmeans=kmeans, data=X, labels=y)

        kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(X_train)
        # model = Model(one_list_to_val(models), X, y)
        model = Model(kmeans, X_train, y_train, X_test, y_test, n_cluster=n_clusters)

        model.score = format_dict(scores)
        # https://stackoverflow.com/questions/19197715/scikit-learn-k-means-elbow-criterion
        # https://stackoverflow.com/questions/41540751/sklearn-kmeans-equivalent-of-elbow-method
        # from sklearn.cluster import KMeans
        # from matplotlib import pyplot as plt
        #
        # X =  # <your_data>
        # distorsions = []
        # for k in range(2, 20):
        #     kmeans = KMeans(n_clusters=k)
        #     kmeans.fit(X)
        #     distorsions.append(kmeans.inertia_)
        #
        # fig = plt.figure(figsize=(15, 5))
        # plt.plot(range(2, 20), distorsions)
        # plt.grid(True)
        # plt.title('Elbow curve')

        #
        # pca = PCA(n_components=n_digits).fit(data)
        # kmeans = KMeans(init=pca.components_, n_clusters=n_digits, n_init=1)
        # self.bench_k_means(kmeans=kmeans, name="PCA-based", data=data, labels=labels)

        return model

    def pca(self, features, target=None, *args, **kwargs):
        import pandas as pd
        n_components = kwargs.pop('n_components', 2)

        df = self.root
        features = parse_columns(df, features)
        if target is not None:
            target = parse_columns(df, target)

        X = df[features]._to_values()
        X = StandardScaler().fit_transform(X)
        pca = decomposition.PCA(n_components=n_components)
        principalComponents = pca.fit_transform(X)

        col_names = ["PCA_" + str(i) for i in range(n_components)]
        principal_df = pd.DataFrame(data=principalComponents, columns=col_names)
        if target:
            finalDf = pd.concat([principal_df, df.data[target]], axis=1)
        else:
            finalDf = principal_df
        return self.root.new(finalDf)

    def random_forest(self, features, target, *args, **kwargs):
        """
        Runs a random forest classifier for input DataFrame.
        :param features: List of columns to select for prediction.
        :param target: Column to predict.
        :return: Model with random forest and prediction run.
        """

        df = self.root

        features = parse_columns(df, features)
        target = parse_columns(df, target)

        df = df.cols.select(features + target).cols.to_float().rows.drop_missings()
        # LogisticRegression requires that the Dask Array passed to it has known chunk sizes
        # https://stackoverflow.com/questions/61756328/dusk-ml-logisticregression-throws-this-error-notimplementederror-can-not-add
        X = df[features]._to_values()
        y = df[target]._to_values()

        # Split params
        X_train, X_test, y_train, y_test = self._split(X, y, **kwargs)
        # X_train, X_test, y_train, y_test = ML._split(y, **kwargs)

        # Random Forest
        n_estimators = kwargs.pop('n_estimators', 20)
        criterion = kwargs.pop('criterion', "mse")
        max_depth = kwargs.pop('max_depth', None)
        min_samples_split = kwargs.pop('min_samples_split', 2)
        min_samples_leaf = kwargs.pop('min_samples_leaf', 1)
        min_weight_fraction_leaf = kwargs.pop('min_weight_fraction_leaf', 0.)
        max_features = kwargs.pop('max_features', "auto")
        max_leaf_nodes = kwargs.pop('max_leaf_nodes', None)
        min_impurity_decrease = kwargs.pop('min_impurity_decrease', 0.)
        min_impurity_split = kwargs.pop('min_impurity_split', None)
        bootstrap = kwargs.pop('bootstrap', True)
        oob_score = kwargs.pop('oob_score', False)
        n_jobs = kwargs.pop('n_jobs', None)
        random_state = kwargs.pop('random_state', 0)
        verbose = kwargs.pop('verbose', 0)
        warm_start = kwargs.pop('warm_start', False)
        ccp_alpha = kwargs.pop('ccp_alpha', 0.0)
        max_samples = kwargs.pop('max_samples', None)

        regressor = RandomForestRegressor(n_estimators,
                                          criterion,
                                          max_depth,
                                          min_samples_split,
                                          min_samples_leaf,
                                          min_weight_fraction_leaf,
                                          max_features,
                                          max_leaf_nodes,
                                          min_impurity_decrease,
                                          min_impurity_split,
                                          bootstrap,
                                          oob_score,
                                          n_jobs,
                                          random_state,
                                          verbose,
                                          warm_start,
                                          ccp_alpha,
                                          max_samples)
        fm = regressor.fit(X_train, y_train.ravel())
        model = Model(fm, X_train, y_train, X_test, y_test)
        # y_pred = regressor.predict(X_test

        return model


