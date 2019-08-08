from pyspark.ml.feature import NGram
from pyspark.sql import functions as F

from optimus import Optimus
from optimus.helpers.columns import parse_columns, name_col
from optimus.ml.contants import CLUSTER_COL, COUNT_COL, RECOMMENDED_COL, CLUSTER_SIZE_COL, NGRAM_COL, FINGERPRINT_COL, \
    NGRAM_FINGERPRINT_COL


def fingerprint(df, input_cols):
    """
    Create the fingerprint for a column
    :param df: Dataframe to be processed
    :param input_cols: Column to be processed
    :return:
    """

    def _split_sort_remove_join(value, args):
        """
        Helper function to split, remove duplicates, sort and join back together
        """
        # Split into whitespace-separated token
        split_key = value.split()

        # Sort and remove duplicated items
        sorted(set(split_key))

        # join the tokens back together
        return "".join(split_key)

    input_cols = parse_columns(df, input_cols)
    for input_col in input_cols:
        output_col = name_col(input_col, FINGERPRINT_COL)
        df = (df
              .withColumn(output_col, F.col(input_col))
              .cols.trim(output_col)
              .cols.lower(output_col)
              .cols.remove_special_chars(output_col)
              .cols.remove_accents(output_col)
              .cols.apply(output_col, _split_sort_remove_join, "string")
              )
        if Optimus.cache:
            df = df.cache()
    return df


def fingerprint_cluster(df, input_cols):
    """
    Cluster a dataframe column based on the Fingerprint algorithm
    :param df: Dataframe to be processed
    :param input_cols: Columns to be processed
    :return:
    """
    # df = self.df
    input_cols = parse_columns(df, input_cols)

    for input_col in input_cols:
        output_col = name_col(input_col, FINGERPRINT_COL)
        # Instead of apply the fingerprint to the whole data set we group by names
        df = (df
              .groupBy(input_col)
              .count()
              .select('count', input_col)
              )

        if Optimus.cache:
            df = df.cache()

        # Calculate the fingerprint
        df = fingerprint(df, input_col)

        count_col = name_col(input_col, COUNT_COL)
        cluster_col = name_col(input_col, CLUSTER_COL)
        recommended_col = name_col(input_col, RECOMMENDED_COL)
        cluster_size_col = name_col(input_col, CLUSTER_SIZE_COL)

        df = df.groupby(output_col).agg(
            F.collect_set(input_col).alias(cluster_col),
            F.sum("count").alias(count_col),
            F.first(input_col).alias(recommended_col),
            F.size(F.collect_set(input_col)).alias(cluster_size_col)
        ).select(cluster_size_col, cluster_col, count_col, recommended_col)
    return df


def n_gram_fingerprint(df, input_cols, n_size=2):
    """
    Calculate the ngram for a fingerprinted string
    :param df: Dataframe to be processed
    :param input_cols: Columns to be processed
    :param n_size:
    :return:
    """

    def remote_white_spaces_remove_sort_join(value, args):
        # remove white spaces
        value = [x.replace(" ", "") for x in value]

        # sort and remove duplicated
        value = sorted(set(value))

        # join the tokens back together
        value = "".join(value)

        return value

    input_cols = parse_columns(df, input_cols)

    for input_col in input_cols:
        ngram_col = name_col(input_col, NGRAM_COL)
        ngram_fingerprint_col = name_col(input_col, NGRAM_FINGERPRINT_COL)

        df = (df
              .cols.copy(input_col, name_col(input_col, NGRAM_COL))
              .cols.lower(ngram_col)
              .cols.remove_white_spaces(ngram_col)
              .cols.remove_special_chars(ngram_col)
              .cols.remove_accents(ngram_col)
              # For create n-grams we need an Array type column
              .cols.nest(input_cols=ngram_col, output_col=ngram_col, shape='array')
              )
        if Optimus.cache:
            df = df.cache()

        n_gram = NGram(n=n_size, inputCol=ngram_col, outputCol=ngram_fingerprint_col)
        df = n_gram.transform(df)
        df = df.cols.apply(ngram_fingerprint_col, remote_white_spaces_remove_sort_join, "string")

    return df


def n_gram_fingerprint_cluster(df, input_cols, n_size=2):
    """
    Cluster a DataFrame column based on the N-Gram Fingerprint algorithm
    :param df: Dataframe to be processed
    :param input_cols: Columns to be processed
    :param n_size:
    :return:
    """
    input_cols = parse_columns(df, input_cols)
    for input_col in input_cols:
        ngram_fingerprint_col = name_col(input_col, NGRAM_FINGERPRINT_COL)

        # Prepare a group so we do not need to apply the fingerprint to the whole data set
        df = (df.select(input_col)
              .groupBy(input_col)
              .count()
              .select('count', input_col)
              )

        if Optimus.cache:
            df = df.cache()
        df = n_gram_fingerprint(df, input_col, n_size)

        count_col = name_col(input_col, COUNT_COL)
        cluster_col = name_col(input_col, CLUSTER_COL)
        recommended_col = name_col(input_col, RECOMMENDED_COL)
        cluster_size_col = name_col(input_col, CLUSTER_SIZE_COL)

        df = df.groupby(ngram_fingerprint_col).agg(
            F.collect_set(input_col).alias(cluster_col),
            F.sum("count").alias(count_col),
            F.first(input_col).alias(recommended_col),
            F.size(F.collect_set(input_col)).alias(cluster_size_col)
        ).select(cluster_size_col, cluster_col, count_col, recommended_col)

        return df
