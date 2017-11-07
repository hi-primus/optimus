import optimus as op
from pyspark.sql import Row, types
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


def test_n_gram():
    df = op.sc. \
        parallelize([['this is the best sentence ever'],
                     ['this is however the worst sentence available']]). \
        toDF(schema=types.StructType().add('sentence', types.StringType()))

    df_model = op.ml.n_gram(df, input_col="sentence", n=2)

    assert_equal(df_model.select('sentence', 'features').count(), 2)
