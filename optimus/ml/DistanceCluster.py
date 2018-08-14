from pyspark.sql import functions as F
from optimus.ml.Keycollision import KeyCollision


class DistanceCluster:

    # def ppm Reference https://github.com/xiaowec/PPM-coding/blob/master/ppm.py
    @staticmethod
    def levenshtein(df, column, threshold=None):
        """

        :param df:
        :param column:
        :param threshold:
        :return:
        """

        df = KeyCollision.fingerprint(df, column)

        col_fingerprint = column + "_fingerprint"
        temp = "temp"
        temp1 = "temp_1"

        #
        df = df.select(col_fingerprint).distinct().select(F.col("STATE_fingerprint").alias(temp),
                                                          F.col("STATE_fingerprint").alias(temp1))

        #  Create all the combination between the string to calculate the levenshtein distance
        df = df.select(temp).crossJoin(df.select(temp1)) \
            .withColumn("l", F.levenshtein(F.col(temp), F.col(temp1)))

        # get the closest word
        df_r = (df.rows.drop(F.col("l") == 0)
                .groupby(temp)
                .agg(F.min("l").alias("l_r"))
                .cols.rename(temp, "temp_r"))

        df = df.join(df_r, ((df_r.temp_r == df.temp) & (df_r.l_r == df.l))).select(temp, "l", temp1)
        df = df \
            .cols.rename([(temp, column), ("l", "distance"), (temp1, "closest")])

        df.unpersist()
        df_r.unpersist()

        return df
