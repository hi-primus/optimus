from nltk.util import ngrams

from optimus.engines.base.ml.constants import CLUSTER_COL, RECOMMENDED_COL, FINGERPRINT_COL, \
    CLUSTER_SUM_COL
from optimus.helpers.columns import parse_columns, name_col


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
        Helper function to split, remove duplicate, sort and join back together
        """
        split_key = value.split()

        split_key = sorted(set(split_key))

        return "".join(split_key)

    input_cols = parse_columns(df, input_cols)
    for input_col in input_cols:
        output_col = name_col(input_col, FINGERPRINT_COL)
        df = (df
              .cols.copy(input_col, output_col)
              .cols.trim(output_col)
              .cols.lower(output_col)
              .cols.remove_special_chars(output_col)
              .cols.normalize_chars(output_col))
        df[output_col] = df[output_col].map(_split_sort_remove_join)
    return df


def n_gram_fingerprint(df, input_cols, n_size=2):
    """
    Calculate the ngram for a fingerprinted string
    :param df: Dataframe to be processed
    :param input_cols: Columns to be processed
    :param n_size:
    :return:
    """

    def calculate_ngrams(value):
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

        df = df.cols.copy(input_col, ngram_fingerprint_col).cols.lower(ngram_fingerprint_col).cols.remove_white_spaces(
            ngram_fingerprint_col)

        df[ngram_fingerprint_col] = df[ngram_fingerprint_col].map(calculate_ngrams)
        df = df.cols.remove_special_chars(ngram_fingerprint_col).cols.normalize_chars(ngram_fingerprint_col)

    return df


def fingerprint_cluster(df, input_cols, output: str = "dict"):
    return base_clustering_function(df, input_cols, output, func=fingerprint)


def n_gram_fingerprint_cluster(df, input_cols, n_size=2, output: str = "dict"):
    return base_clustering_function(df, input_cols, output, func=n_gram_fingerprint, n_size=n_size)


def base_clustering_function(df, input_cols, output, func=None, **kwargs):
    """
    Cluster a dataframe column based on the Fingerprint algorithm
    :return:
    """

    input_cols = parse_columns(df, input_cols)

    for input_col in input_cols:
        fingerprint_col = name_col(input_col, FINGERPRINT_COL)
        recommended_col = name_col(input_col, RECOMMENDED_COL)
        cluster_col = name_col(input_col, CLUSTER_COL)
        cluster_sum_col = name_col(input_col, CLUSTER_SUM_COL)

        df = func(df, input_col, **kwargs)

        kw = {cluster_col: (input_col, list), recommended_col: (input_col, "first"),
              cluster_sum_col: ("count", "sum")}

        df = df.groupby([input_col, fingerprint_col]).agg(count=(fingerprint_col, "size")).sort_values(
            [fingerprint_col, "count"], ascending=False).reset_index(drop=False).groupby(fingerprint_col).agg(**kw)

    return df
