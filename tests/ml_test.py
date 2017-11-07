import optimus as op
from pyspark.sql import Row
from pyspark.ml import feature, classification
from nose.tools import assert_equal
import pyspark


def test_ml_pipe():
    df = op.sc. \
        parallelize([Row(sentence='this is a test', label=0.),
                     Row(sentence='this is another test', label=1.)]). \
        toDF()

    pl = feature.Tokenizer().setInputCol('sentence') | feature.CountVectorizer()
    ml = pl | classification.LogisticRegression()

    ml_model = ml.fit(df)
    assert_equal(ml_model.transform(df).count(), 2)


def test_logistic_regression_text():
    df = op.sc. \
        parallelize([Row(sentence='this is a test', label=0.),
                     Row(sentence='this is another test', label=1.)]). \
        toDF()

    df_predict, ml_model = op.ml.logistic_regression_text(df, "sentence")

    assert isinstance(df_predict, pyspark.sql.dataframe.DataFrame),  "Error for df_predict"

    assert isinstance(ml_model, pyspark.ml.PipelineModel), "Error for ml_model"
