from pyspark.sql import functions as F

from optimus.helpers.functions import parse_columns
from optimus.ml.Keycollision import KeyCollision


class DistanceCluster:

    # def ppm Reference https://github.com/xiaowec/PPM-coding/blob/master/ppm.py
    @staticmethod
    def levenshtein_matrix(df, column):
        df = KeyCollision.fingerprint(df, column)

        col_fingerprint = column + "_FINGERPRINT"
        col_distance = column + "_LEVENSHTEIN_DISTANCE"

        temp_col_1 = column + "_LEVENSHTEIN_1"
        temp_col_2 = column + "_LEVENSHTEIN_2"

        # Prepare the columns to calculate the cross join
        df = df.select(col_fingerprint).distinct().select(F.col(col_fingerprint).alias(temp_col_1),
                                                          F.col(col_fingerprint).alias(temp_col_2))

        #  Create all the combination between the string to calculate the levenshtein distance
        df = df.select(temp_col_1).crossJoin(df.select(temp_col_2)) \
            .withColumn(col_distance, F.levenshtein(F.col(temp_col_1), F.col(temp_col_2)))

        return df

    @staticmethod
    def levenshtein_filter(df, column, threshold=None):
        """

        :param df:
        :param column:
        :param threshold:
        :return:
        """
        # TODO: must filter by and exprs
        func = F.min

        col_distance = column + "_LEVENSHTEIN_DISTANCE"
        col_distance_r = column + "_LEVENSHTEIN_DISTANCE_R"

        temp_col_1 = column + "_LEVENSHTEIN_1"
        temp_col_2 = column + "_LEVENSHTEIN_2"
        temp_r = "TEMP_R"

        df = DistanceCluster.levenshtein_matrix(df, column)

        # get the closest word
        df_r = (df.rows.drop(F.col(col_distance) == 0)
                .groupby(temp_col_1)
                .agg(func(col_distance).alias(col_distance_r))
                .cols.rename(temp_col_1, temp_r))

        df = df.join(df_r, ((df_r[temp_r] == df[temp_col_1]) & (df_r[col_distance_r] == df[col_distance]))).select(
            temp_col_1,
            col_distance,
            temp_col_2)

        df = df \
            .cols.rename([(temp_col_1, column + "_FROM"), (temp_col_2, column + "_TO")])

        df.unpersist()
        df_r.unpersist()

        return df

    @staticmethod
    def levenshtein_cluster(df, col_name):
        # Prepare a group so we don need to apply the fingerprint to the whole data set
        df = df.select(col_name).groupby(col_name).agg(F.count(col_name).alias("count"))

        df = KeyCollision.fingerprint(df, col_name)

        df_t = df.groupby(col_name + "_FINGERPRINT").agg(F.collect_list(col_name).alias("cluster"),
                                                         F.size(F.collect_list(col_name)).alias("cluster_size"),
                                                         F.first(col_name).alias("recommended"),
                                                         F.sum("count").alias("count"))

        # Filter min distance
        df_l = DistanceCluster.levenshtein_filter(df, col_name)

        # Cluster
        df_l = df_l.join(df_t, (df_l[col_name + "_FROM"] == df_t[col_name + "_FINGERPRINT"]), how="left") \
            .cols.drop(col_name + "_FINGERPRINT") \
            .cols.drop([col_name + "_FROM", col_name + "_TO", col_name + "_LEVENSHTEIN_DISTANCE"]).table()

        return df_l
