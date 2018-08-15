from io import BytesIO

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from multipledispatch import dispatch
from numpy import array
from pyspark.ml.stat import Correlation
from pyspark.sql import DataFrame

from optimus.helpers.decorators import add_attr


def plots(self):
    @add_attr(plots)
    @dispatch(object)
    def hist(column_data=None):
        """
        Plot a histogram
        obj = {"col_name":[{'lower': -87.36666870117188, 'upper': -70.51333465576172, 'value': 0},
        {'lower': -70.51333465576172, 'upper': -53.66000061035157, 'value': 22094},
        {'lower': -53.66000061035157, 'upper': -36.80666656494141, 'value': 2},
        ...
        ]}
        :param column_data:
        :return:
        """

        for col_name, data in column_data.items():

            bins = []
            for d in data:
                bins.append(d['lower'])

            last = data[len(data) - 1]["upper"]
            bins.append(last)

            # Transform hist Optimus format to matplot lib
            hist = []
            for d in data:
                if d is not None:
                    hist.append(d["value"])

            bins = array(bins)
            center = (bins[:-1] + bins[1:]) / 2
            width = 0.9 * (bins[1] - bins[0])

            # Plot
            fig = plt.figure()
            plt.bar(center, hist, width=width)
            plt.title("Histogram " + col_name)

            # Save as base64
            figfile = BytesIO()
            plt.savefig(figfile, format='png')
            figfile.seek(0)  # rewind to beginning of file
            import base64
            # figdata_png = base64.b64encode(figfile.read())
            figdata_png = base64.b64encode(figfile.getvalue())
            plt.close(fig)

            return figdata_png.decode('utf8')

    # @add_attr(plots)
    # @dispatch(object)
    # def hist(col_name=None):
    #    pass

    @add_attr(plots)
    def correlation(vec_col, method="pearson"):
        """
        Compute the correlation matrix for the input dataset of Vectors using the specified method. Method
        mapped from  pyspark.ml.stat.Correlation.
        :param vec_col: The name of the column of vectors for which the correlation coefficient needs to be computed.
        This must be a column of the dataset, and it must contain Vector objects.
        :param method: String specifying the method to use for computing correlation. Supported: pearson (default),
        spearman.
        :return: Heatmap plot of the corr matrix using seaborn.
        """

        assert isinstance(method, str), "Error, method argument provided must be a string."

        assert method == 'pearson' or (
                method == 'spearman'), "Error, method only can be 'pearson' or 'sepearman'."

        cor = Correlation.corr(self._df, vec_col, method).head()[0].toArray()
        return sns.heatmap(cor, mask=np.zeros_like(cor, dtype=np.bool), cmap=sns.diverging_palette(220, 10,
                                                                                                   as_cmap=True))

    return plots


DataFrame.plots = property(plots)
