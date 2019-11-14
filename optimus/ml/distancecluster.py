from pyspark.sql import functions as F

from optimus import Optimus
from optimus.helpers.columns import name_col
from optimus.ml import keycollision
from optimus.ml.contants import FINGERPRINT_COL, CLUSTER_COL, CLUSTER_SIZE_COL, RECOMMENDED_COL, COUNT_COL, \
    LEVENSHTEIN_DISTANCE


def levenshtein_json(df, input_col):
    """
    Output the levenshtein distance in json format
    :param df: Spark Dataframe
    :param input_col:
    :return:
    """
    df = keycollision.fingerprint(df, input_col)
    # df.table()
    fingerprint_col = name_col(input_col, FINGERPRINT_COL)
    distance_col_name = name_col(input_col, LEVENSHTEIN_DISTANCE)

    temp_col_1 = input_col + "_LEVENSHTEIN_1"
    temp_col_2 = input_col + "_LEVENSHTEIN_2"

    # Prepare the columns to calculate the cross join
    result = df.select(input_col, F.col(fingerprint_col).alias(temp_col_1)).distinct()

    df = df.select(input_col, F.col(fingerprint_col).alias(temp_col_1),
                   F.col(fingerprint_col).alias(temp_col_2)).distinct()

    # Create all the combination between the string to calculate the levenshtein distance
    df = df.select(temp_col_1).crossJoin(df.select(temp_col_2)) \
        .withColumn(distance_col_name, F.levenshtein(F.col(temp_col_1), F.col(temp_col_2)))

    # if Optimus.cache:
    #     df = df.cache()

    # Select only the string with shortest path
    distance_col = name_col(input_col, LEVENSHTEIN_DISTANCE)
    distance_r_col = input_col + "_LEVENSHTEIN_DISTANCE_R"
    temp_r = "TEMP_R"

    df_r = (df.rows.drop(F.col(distance_col) == 0)
            .groupby(temp_col_1)
            .agg(F.min(distance_col).alias(distance_r_col))
            .cols.rename(temp_col_1, temp_r)).repartition(1)

    df = df.join(df_r, ((df_r[temp_r] == df[temp_col_1]) & (df_r[distance_r_col] == df[distance_col]))) \
        .select(temp_col_1, distance_col, temp_col_2).repartition(1)

    # Create the clusters/lists

    df = (df.groupby(temp_col_1)
          .agg(F.collect_list(temp_col_2)))

    kv_dict = {}
    for row in result.collect():
        _row = list(row.asDict().values())
        kv_dict[_row[1]] = _row[0]

    kv_result_df = {}
    for row in df.collect():
        _row = list(row.asDict().values())
        kv_result_df[_row[0]] = _row[1]

    result = {}
    for k, v in kv_result_df.items():
        a = result[kv_dict[k]] = []
        for iv in v:
            a.append(kv_dict[iv])

    return result


def levenshtein_matrix(df, input_col):
    """
    Create a couple of column with all the string combination
    :param df: Spark Dataframe
    :param input_col:
    :return:
    """
    df = keycollision.fingerprint(df, input_col)
    # df.table()
    fingerprint_col = name_col(input_col, FINGERPRINT_COL)
    distance_col_name = name_col(input_col, LEVENSHTEIN_DISTANCE)

    temp_col_1 = input_col + "_LEVENSHTEIN_1"
    temp_col_2 = input_col + "_LEVENSHTEIN_2"

    # Prepare the columns to calculate the cross join
    df = df.select(F.col(fingerprint_col).alias(temp_col_1),
                   F.col(fingerprint_col).alias(temp_col_2)).distinct()

    #  Create all the combination between the string to calculate the levenshtein distance
    df = df.select(temp_col_1).crossJoin(df.select(temp_col_2)) \
        .withColumn(distance_col_name, F.levenshtein(F.col(temp_col_1), F.col(temp_col_2)))

    if Optimus.cache:
        df = df.cache()

    return df


def levenshtein_filter(df, input_col, func=F.min):
    """
    Get the nearest string
    :param df: Spark Dataframe
    :param input_col:
    :param func: F.min by default can filter
    :return:
    """

    distance_col = name_col(input_col, LEVENSHTEIN_DISTANCE)
    distance_r_col = input_col + "_LEVENSHTEIN_DISTANCE_R"

    temp_col_1 = input_col + "_LEVENSHTEIN_1"
    temp_col_2 = input_col + "_LEVENSHTEIN_2"
    temp_r = "TEMP_R"

    df = levenshtein_matrix(df, input_col)

    # get the closest word
    df_r = (df.rows.drop(F.col(distance_col) == 0)
            .groupby(temp_col_1)
            .agg(func(distance_col).alias(distance_r_col))
            .cols.rename(temp_col_1, temp_r))
    # df_r.show()

    # if Optimus.cache:
    #     df_r = df_r.cache()

    df = df.join(df_r, ((df_r[temp_r] == df[temp_col_1]) & (df_r[distance_r_col] == df[distance_col]))) \
        .select(temp_col_1, distance_col, temp_col_2)

    df = df \
        .cols.rename([(temp_col_1, input_col + "_FROM"), (temp_col_2, input_col + "_TO")])

    return df


def levenshtein_cluster(df, input_col):
    """
    Return a dataframe with a string of cluster related to a string
    :param df: Spark Dataframe
    :param input_col:
    :return:
    """
    # Prepare a group so we don't need to apply the fingerprint to the whole data set
    df = df.select(input_col).groupby(input_col).agg(F.count(input_col).alias("count"))
    df = keycollision.fingerprint(df, input_col)

    count_col = name_col(input_col, COUNT_COL)
    cluster_col = name_col(input_col, CLUSTER_COL)
    recommended_col = name_col(input_col, RECOMMENDED_COL)
    cluster_size_col = name_col(input_col, CLUSTER_SIZE_COL)
    fingerprint_col = name_col(input_col, FINGERPRINT_COL)

    df_t = df.groupby(fingerprint_col).agg(F.collect_list(input_col).alias(cluster_col),
                                           F.size(F.collect_list(input_col)).alias(cluster_size_col),
                                           F.first(input_col).alias(recommended_col),
                                           F.sum("count").alias(count_col)).repartition(1)
    # if Optimus.cache:
    #     df_t = df_t.cache()

    # Filter nearest string
    df_l = levenshtein_filter(df, input_col).repartition(1)

    if Optimus.cache:
        df_l = df_l.cache()

    # Create Cluster
    df_l = df_l.join(df_t, (df_l[input_col + "_FROM"] == df_t[fingerprint_col]), how="left") \
        .cols.drop(fingerprint_col) \
        .cols.drop([input_col + "_FROM", input_col + "_TO", name_col(input_col, "LEVENSHTEIN_DISTANCE")])

    return df_l
