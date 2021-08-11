from nltk.util import ngrams
from pyspark.sql import functions as F

from optimus.helpers.columns import parse_columns, name_col
from optimus.helpers.json import dump_json
from optimus.engines.base.ml.constants import CLUSTER_COL, COUNT_COL, RECOMMENDED_COL, CLUSTER_COUNT_COL, \
    FINGERPRINT_COL, \
    CLUSTER_SUM_COL


def fingerprint(df, input_cols):
    """
    Create the fingerprint for a column
    :param df: Dataframe to be processed
    :param input_cols: Column to be processed
    :return:
    """

    # https://github.com/OpenRefine/OpenRefine/blob/master/main/src/com/google/refine/clustering/binning/FingerprintKeyer.java#L56
    def _split_sort_remove_join(value):
        """
        Helper function to split, remove duplicates, sort and join back together
        """
        # Split into whitespace-separated token
        # print("value", type(value), value)
        split_key = value.split()

        # Sort and remove duplicated items
        split_key = sorted(set(split_key))

        # join the tokens back together
        return "".join(split_key)

    input_cols = parse_columns(df, input_cols)
    for input_col in input_cols:
        output_col = name_col(input_col, FINGERPRINT_COL)
        df = (df
              .cols.trim(input_col, output_col)
              .cols.lower(output_col)
              .cols.remove_special_chars(output_col)
              .cols.normalize_chars(output_col)
              .cols.apply(output_col, _split_sort_remove_join, "string", mode="map")
              )
    return df


def n_gram_fingerprint(df, input_cols, n_size=2):
    """
    Calculate the ngram for a fingerprinted string
    :param df: Dataframe to be processed
    :param input_cols: Columns to be processed
    :param n_size:
    :return:
    """

    def calculate_ngrams(value, args):
        # remove white spaces
        ngram = list(ngrams(value, n_size))

        # sort and remove duplicated
        ngram = sorted(set(ngram))

        _result = ""
        for item in ngram:
            for i in item:
                _result = _result + i

        # join the tokens back together
        _result = "".join(_result)

        return _result

    input_cols = parse_columns(df, input_cols)

    for input_col in input_cols:
        ngram_fingerprint_col = name_col(input_col, FINGERPRINT_COL)
        # ngram_fingerprint_col = name_col(input_col, NGRAM_FINGERPRINT_COL)

        df = (df
              .cols.copy(input_col, ngram_fingerprint_col)
              .cols.lower(ngram_fingerprint_col)
              .cols.remove_white_spaces(ngram_fingerprint_col)
              .cols.apply(ngram_fingerprint_col, calculate_ngrams, "string", output_cols=ngram_fingerprint_col)
              .cols.remove_special_chars(ngram_fingerprint_col)
              .cols.normalize_chars(ngram_fingerprint_col)

              )

    return df


def fingerprint_cluster(df, input_cols, output: str = "dict"):
    return base_clustering_function(df, input_cols, output, func=fingerprint, args=[input_cols])


def n_gram_fingerprint_cluster(df, input_cols, n_size=2, output: str = "dict"):
    return base_clustering_function(df, input_cols, output, func=n_gram_fingerprint, args=[input_cols, n_size])


def base_clustering_function(df, input_cols, output, func=None, args=None):
    """
    Cluster a dataframe column based on the Fingerprint algorithm
    :return:
    """

    # df = self.df
    input_cols = parse_columns(df, input_cols)
    result = {}
    for input_col in input_cols:
        output_col = name_col(input_col, FINGERPRINT_COL)
        count_col = name_col(input_col, COUNT_COL)
        # Instead of apply the fingerprint to the whole data set we group by names
        df = (df
              .groupBy(input_col)
              .agg(F.count(input_col).alias(count_col))
              .select(count_col, input_col)
              ).h_repartition(1)

        # Calculate the fingerprint
        recommended_col = name_col(input_col, RECOMMENDED_COL)
        cluster_col = name_col(input_col, CLUSTER_COL)
        cluster_sum_col = name_col(input_col, CLUSTER_SUM_COL)
        cluster_count_col = name_col(input_col, CLUSTER_COUNT_COL)

        # Apply function cluster
        df = func(df, *args)

        df = df.withColumn(cluster_col, F.create_map([input_col, count_col]))

        df = df.groupBy(output_col).agg(F.max(F.struct(F.col(count_col), F.col(input_col))).alias(recommended_col),
                                        F.collect_list(cluster_col).alias(cluster_col),
                                        F.count(output_col).alias(cluster_count_col),
                                        F.sum(count_col).alias(cluster_sum_col))
        df = df.select(recommended_col + "." + input_col, cluster_col, cluster_count_col, cluster_sum_col)

        for row in df.collect():
            _row = list(row.asDict().values())
            # List of dict to dict
            flatted_dict = {k: v for element in _row[1] for k, v in element.items()}
            result[_row[0]] = {"similar": flatted_dict, "count": _row[2], "sum": _row[3]}

    if output == "json":
        result = dump_json(result)
    return result
