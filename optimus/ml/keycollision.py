from pyspark.ml.feature import NGram
from pyspark.sql import functions as F

from optimus.helpers.functions import parse_columns


def fingerprint(df, columns):
    """
    Create the fingerprint for a
    :param df:
    :param columns:
    :return:
    """

    def _split_sort_remove_join(value, args):
        """
        Helper function to split, remove duplicates, sort and join back together
        :param value:
        :param args:
        :return:
        """
        # Split into whitespace-separated token
        split_key = value.split()

        # Sort and remove duplicated items
        sorted(set(split_key))

        # join the tokens back together
        return "".join(split_key)

    columns = parse_columns(df, columns)
    for col_name in columns:
        output_col = col_name + "_FINGERPRINT"
        df = (df
              .withColumn(output_col, F.col(col_name))
              .cols.trim(output_col)
              .cols.lower(output_col)
              .cols.remove_special_chars(output_col)
              .cols.remove_accents(output_col)
              .cols.apply(output_col, _split_sort_remove_join, "string")
              .repartition(1)
              .cache()
              )
    return df


def fingerprint_cluster(df, columns):
    """
    Cluster a dataframe column based on the Fingerprint algorithm
    :param df:
    :param columns: Columns to be processed
    :return:
    """
    # df = self.df
    columns = parse_columns(df, columns)

    for col_name in columns:
        output_col = col_name + "_FINGERPRINT"
        # Instead of apply the fingerprint to the whole data set we group by names
        df = (df
              .groupBy(col_name)
              .count()
              .select('count', col_name)
              .repartition(1)  # Needed for optimization in a single machine
              .cache()
              )
        # Calculate the fingeprint
        df = fingerprint(df, col_name)

        # Create cluster
        df = df.groupby(output_col).agg(
            F.collect_set(col_name).alias("cluster"),
            F.sum("count").alias("count"),
            F.first(col_name).alias("recommended"),
            F.size(F.collect_set(col_name)).alias("cluster_size")
        ) \
            .select("cluster_size", "cluster", "count", "recommended")
    return df


def n_gram_fingerprint(df, columns, n_size):
    """
    Calculate the ngram for a fingerprinted string
    :param df:
    :param columns:
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

    columns = parse_columns(df, columns)
    for col_name in columns:
        output_col = col_name + "_NGRAM"
        n_gram_col = col_name + "_NGRAM_FINGERPRINT"

        df = (df
              .withColumn(output_col, F.col(col_name))
              .cols.lower(output_col)
              .cols.remove_white_spaces(output_col)
              .cols.remove_special_chars(output_col)
              .cols.remove_accents(output_col)
              # For create n-grams we need a Array type column
              .cols.split(output_col, "")
              .repartition(1)  # Needed for optimization in a single machine
              .cache()
              )

        n_gram = NGram(n=n_size, inputCol=output_col, outputCol=n_gram_col)
        df = n_gram.transform(df)
        df = df.cols.apply(n_gram_col, remote_white_spaces_remove_sort_join, "string")

    return df


def n_gram_fingerprint_cluster(df, columns, n_size=2):
    """
    Cluster a DataFrame column based on the N-Gram Fingerprint algorithm
    :param df:
    :param columns:
    :param n_size:
    :return:
    """
    columns = parse_columns(df, columns)
    for col_name in columns:
        n_gram_col = col_name + "_ngram_fingerprint"

        # Prepare a group so we don need to apply the fingerprint to the whole data set
        df = (df.select(col_name)
              .groupBy(col_name)
              .count()
              .select('count', col_name)
              .repartition(1)  # Needed for optimization in a single machine
              .cache())

        df = n_gram_fingerprint(df, col_name, n_size)
        # df.table()
        df = df.groupby(n_gram_col).agg(
            F.collect_set(col_name).alias("cluster"),
            F.sum("count").alias("count"),
            F.first(col_name).alias("recommended"),
            F.size(F.collect_set(col_name)).alias("cluster_size")
        ).select("cluster_size", "cluster", "count", "recommended")

        return df


def to_json(df, column, n_size=2):
    return n_gram_fingerprint_cluster(df, column, n_size).to_json()
