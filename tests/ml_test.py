import optimus as op
from pyspark.sql import Row
from pyspark.ml import feature, classification
from nose.tools import assert_equal


class TestML(object):

    @classmethod
    def test_ml_pipe(cls):
        df = op.sc. \
            parallelize([Row(sentence='this is a test', label=0.),
                         Row(sentence='this is another test', label=1.)]). \
            toDF()

        pl = feature.Tokenizer().setInputCol('sentence') | feature.CountVectorizer()
        ml = pl | classification.LogisticRegression()

        ml_model = ml.fit(df)
        assert_equal(ml_model.transform(df).count(), 2)
