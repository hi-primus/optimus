from pyspark.sql import functions as F

from optimus.helpers.columns import name_col
from optimus.helpers.json import dump_json
from optimus.ml import keycollision
from optimus.ml.constants import FINGERPRINT_COL, LEVENSHTEIN_DISTANCE


def levenshtein_cluster(df, input_col, threshold: int = None, output: str = "dict"):
    """
    Output the levenshtein distance in json format
    :param df: Spark Dataframe
    :param input_col: Column to be processed
    :param threshold: number
    :param output: "dict" or "json"
    :return:
    """
    # Create fingerprint
    df_fingerprint = keycollision.fingerprint(df, input_col)

    # Names
    fingerprint_col = name_col(input_col, FINGERPRINT_COL)
    distance_col_name = name_col(input_col, LEVENSHTEIN_DISTANCE)
    temp_col_1 = input_col + "_LEVENSHTEIN_1"
    temp_col_2 = input_col + "_LEVENSHTEIN_2"
    count = "count"

    # Prepare the columns to calculate the cross join
    fingerprint_count = df_fingerprint.select(input_col, fingerprint_col).groupby(input_col) \
        .agg(F.first(input_col).alias(temp_col_1), F.first(fingerprint_col).alias(temp_col_2),
             F.count(input_col).alias(count)) \
        .select(temp_col_1, temp_col_2, count).collect()

    df = df_fingerprint.select(input_col, F.col(fingerprint_col).alias(temp_col_1),
                               F.col(fingerprint_col).alias(temp_col_2)).distinct()

    # Create all the combination between the string to calculate the levenshtein distance
    df = df.select(temp_col_1).crossJoin(df.select(temp_col_2)) \
        .withColumn(distance_col_name, F.levenshtein(F.col(temp_col_1), F.col(temp_col_2)))

    # Select only the string with shortest path
    distance_col = name_col(input_col, LEVENSHTEIN_DISTANCE)
    distance_r_col = input_col + "_LEVENSHTEIN_DISTANCE_R"
    temp_r = "TEMP_R"

    if threshold is None:
        where = ((F.col(distance_col) == 0) & (F.col(temp_col_1) != F.col(temp_col_2)))
    else:
        where = (F.col(distance_col) == 0) | (F.col(distance_col) > threshold)

    df_r = (df.rows.drop(where)
            .cols.replace(distance_col, 0, None, search_by="numeric")
            .groupby(temp_col_1)
            .agg(F.min(distance_col).alias(distance_r_col))
            # .cols.rename(distance_col, distance_r_col)
            .cols.rename(temp_col_1, temp_r)).repartition(1)

    df = df.join(df_r, ((df_r[temp_r] == df[temp_col_1]) & (df_r[distance_r_col] == df[distance_col]))) \
        .select(temp_col_1, distance_col, temp_col_2).repartition(1)

    # Create the clusters/lists
    df = (df.groupby(temp_col_1)
          .agg(F.collect_list(temp_col_2), F.count(temp_col_2)))

    # Replace ngram per string
    kv_dict = {}
    for row in fingerprint_count:
        _row = list(row.asDict().values())
        kv_dict[_row[1]] = {_row[0]: _row[2]}

    result = {}
    for row in df.collect():
        _row = list(row.asDict().values())
        d = {}
        for i in _row[1]:
            key = list(kv_dict[i].keys())[0]
            value = list(kv_dict[i].values())[0]
            d[key] = value
        key = list(kv_dict[_row[0]].keys())[0]
        value = list(kv_dict[_row[0]].values())[0]
        d.update({key: value})
        result[key] = d

    # Calculate count and sum
    f = {}
    for k, v in result.items():
        _sum = 0
        for x, y in v.items():
            _sum = _sum + y
        f[k] = {"similar": v, "count": len(v), "sum": _sum}

    result = f
    if output == "json":
        result = dump_json(result)
    return result
