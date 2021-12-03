from optimus.helpers.columns import parse_columns
from optimus.plots.functions import plot_heatmap, plot_boxplot, plot_frequency, plot_hist, \
    plot_correlation, plot_qqplot


class Plot:
    def __init__(self, df):
        self.df = df

    def hist(self, columns=None, buckets=10, output_format="plot", output_path=None):
        """
        Plot histogram
        :param columns: Columns to be printed
        :param buckets: Number of buckets
        :param output_format:
        :param output_path: path where the image is going to be saved
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)

        data = df.cols.hist(columns, buckets)["hist"]
        for col_name in data.keys():
            plot_hist({col_name: data[col_name]}, output=output_format, path=output_path)

    def heatmap(self, col_x, col_y, bins_x=10, bins_y=10, output_format="plot", output_path=None):
        """
        Plot heat map
        :param col_x: columns to be printed
        :param bins_x: number of buckets
        :param output_format:
        :param output_path: path where the image is going to be saved
        :return:
        """
        df = self.df
        data = df.cols.heatmap(col_x, col_y, bins_x, bins_y)

        plot_heatmap(data, output=output_format, path=output_path)

    def box(self, columns=None, output_format="plot", output_path=None):
        """
        Plot boxplot
        :param columns: Columns to be printed
        :param output_format:
        :param output_path: path where the image is going to be saved
        :return:
        """
        stats = self.df.cols.boxplot(columns)
        plot_boxplot(stats, output=output_format, path=output_path)

    def frequency(self, columns=None, buckets=10, output_format="plot", output_path=None):
        """
        Plot frequency chart
        :param columns: Columns to be printed
        :param buckets: Number of buckets
        :param output_format:
        :param output_path: path where the image is going to be saved
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        data = df.cols.frequency(columns, buckets, tidy=True)

        for k, v in data.items():
            plot_frequency({k: v}, output=output_format, path=output_path)

    def correlation(self, cols, method="pearson", output_format="plot", output_path=None):
        """
        Compute the correlation matrix for the input data set of Vectors using the specified method. Method
        mapped from pyspark.ml.stat.Correlation.
        :param cols: The name of the columns for which the correlation coefficient needs to be computed.
        :param method: String specifying the method to use for computing correlation. Supported: pearson (default),
        spearman.
        :param output_format: Output image format
        :param output_path: Output path
        :return: Heatmap plot of the corr matrix using seaborn.
        """
        df = self.df
        cols_data = df.cols.correlation(cols, method, tidy=False)
        plot_correlation(cols_data, output=output_format, path=output_path)

    def qqplot(self, columns, n=100, output_format="plot", output_path=None):
        """
        QQ plot
        :param columns:
        :param n: Sample size
        :param output_format: Output format
        :param output_path: Path to the output file
        :return:
        """
        df = self.df

        columns = parse_columns(
            df, cols_args=columns, filter_by_column_types=df.constants.NUMERIC_TYPES)

        if columns is not None:
            sample_data = df.sample(n=n, random=True)
            for col_name in columns:
                plot_qqplot(col_name, sample_data, output=output_format, path=output_path)
