from optimus import Optimus
import pyspark
from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit

op = Optimus(dl=True)
from sparkdl.image import imageIO


# rdd = op.sc.parallelize([Row(predicted_labels=['daisy', '0.8918145298957825']),
#                         Row(predicted_labels=['picket_fence', '0.14247830212116241']),
#                         Row(predicted_labels=['daisy', '0.9532104134559631'])])

# df_row = spark.createDataFrame(rdd)


def assert_spark_df(df):
    assert isinstance(df, pyspark.sql.dataframe.DataFrame), "Not a Spark DF"


def assert_spark_model(model):
    assert isinstance(model, pyspark.ml.PipelineModel), "Not a model"


tulips_df = ImageSchema.readImages("tests/testtulips/").withColumn("label", lit(1))
daisy_df = imageIO.readImagesWithCustomFn("tests/testdaisy/", decode_f=imageIO.PIL_decode).withColumn("label", lit(0))

train_df = tulips_df.unionAll(daisy_df)


def test_image_classifier_lr():
    model, df_preds = op.dl.image_classifier_lr(train_df)

    assert_spark_model(model)
    assert_spark_df(df_preds)


def test_evaluate_img_lr():
    model, df_preds = op.dl.image_classifier_lr(train_df)
    result = op.dl.evaluate_image_classifier(train_df, model)

    assert isinstance(result, float), "Not a float"


def test_image_predictor():
    preds = op.dl.image_predictor("tests/sampleimg/")
    assert_spark_df(preds)
