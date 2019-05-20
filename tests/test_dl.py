from optimus import Optimus
from optimus.dl.models import DL
import pyspark
from pyspark.sql.functions import lit

import os
SUBMIT_ARGS = "--packages databricks:spark-deep-learning:1.5.0-spark2.4-s_2.11 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

op = Optimus()
dl = DL()

# rdd = op.sc.parallelize([Row(predicted_labels=['daisy', '0.8918145298957825']),
#                         Row(predicted_labels=['picket_fence', '0.14247830212116241']),
#                         Row(predicted_labels=['daisy', '0.9532104134559631'])])

# df_row = spark.createDataFrame(rdd)


def assert_spark_df(df):
    assert isinstance(df, pyspark.sql.dataframe.DataFrame), "Not a Spark DF"


def assert_spark_model(model):
    assert isinstance(model, pyspark.ml.PipelineModel), "Not a model"


tulips_df = op.spark.read.format("image").load("optimus/tests/testtulips/").withColumn("label", lit(1))
daisy_df = op.spark.read.format("image").load("optimus/tests/testdaisy/").withColumn("label", lit(0))

train_df = tulips_df.unionAll(daisy_df)


def test_image_classifier_lr():
    model, df_preds = dl.image_classifier_lr(train_df)

    assert_spark_model(model)
    assert_spark_df(df_preds)


def test_evaluate_img_lr():
    model, df_preds = dl.image_classifier_lr(train_df)
    result = dl.evaluate_image_classifier(train_df, model)

    assert isinstance(result, float), "Not a float"


def test_image_predictor():
    preds = dl.image_predictor("tests/sampleimg/")
    assert_spark_df(preds)
