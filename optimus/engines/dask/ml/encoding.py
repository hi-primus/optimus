import re

import dask.dataframe as dd

from optimus.helpers.raiseit import RaiseIt
from optimus.helpers.logger import logger
from optimus.helpers.columns import parse_columns, name_col
from optimus.infer import is_, is_str

from optimus.helpers.types import *
from optimus.engines.base.ml.encoding import BaseEncoding


class Encoding(BaseEncoding):

    def one_hot_encoder(self, cols="*", prefix=None, drop=True, **kwargs):
        """
        Maps a column of label indices to a column of binary vectors, with at most a single one-value.
        :param cols: Columns to be encoded.
        :param prefix: Column where the output is going to be saved.
        :param drop:
        :return: Dataframe with encoded columns.
        """

        df = self.root
        dfd = df.data

        all_cols = parse_columns(df, cols)
        cols = parse_columns(df, cols, filter_by_column_types=self.root.constants.OBJECT_TYPES)

        excluded_cols = list(set(all_cols) - set(cols))

        if len(excluded_cols):
            logger.warn(f"{RaiseIt._and(excluded_cols)} cannot be encoded using 'one_hot_encoder'")

        try:
            from dask_ml.preprocessing import DummyEncoder

            de = DummyEncoder()
            categorized_df = dfd[cols].categorize()

            encoded_df = de.fit_transform(categorized_df)

            if prefix:
                cols_regex = r"^(%s)_" % "|".join(cols)

            for col_name in encoded_df.columns:
                new_col_name = col_name

                if prefix:
                    new_col_name = re.sub(cols_regex, f"{prefix}_", col_name)

                dfd[new_col_name] = encoded_df[col_name]

            df = self.root.new(dfd)

        except ModuleNotFoundError:
            
            # Use default encoder
            df = self.root.new(dd.concat([dfd, dd.get_dummies(dfd[cols].categorize(index=False), prefix=prefix)], axis=1))

        if drop:
            df = df.cols.drop(cols)

        return df

    def normalizer(df, input_cols, output_col=None, p=2.0):
        """
        Transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which
        specifies the p-norm used for normalization. (p=2) by default.
        :param df: Dataframe to be transformed
        :param input_cols: Columns to be normalized.
        :param output_col: Column where the output is going to be saved.
        :param p:  p-norm used for normalization.
        :return: Dataframe with normalized columns.
        """

        # Check if columns argument must be a string or list datat ype:
        if not is_(input_cols, (str, list)):
            RaiseIt.type_error(input_cols, ["str", "list"])

        if is_str(input_cols):
            input_cols = [input_cols]

        if is_(input_cols, (float, int)):
            RaiseIt.type_error(input_cols, ["float", "int"])

        # Try to create a vector
        if len(input_cols) > 1:
            df = df.cols.cast(input_cols, "vector")

        if output_col is None:
            output_col = name_col(input_cols, "normalizer")

        # TODO https://developer.ibm.com/code/2018/04/10/improve-performance-ml-pipelines-wide-dataframes-apache-spark-2-3/
        normal = [Normalizer(inputCol=col_name, outputCol=output_col, p=p) for col_name in
                list(set(input_cols))]

        pipeline = Pipeline(stages=normal)

        df = pipeline.fit(df).transform(df)

        return df
