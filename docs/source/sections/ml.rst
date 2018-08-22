Machine Learning with Optimus
==================================

Machine Learning is one of the last steps, and the goal for most Data Science WorkFlows.

Apache Spark created a library called MLlib where they coded great algorithms for Machine Learning. Now
with the ML library we can take advantage of the Dataframe API and its optimization to create easily
Machine Learning Pipelines.

Even though this task is not extremely hard, is not easy. The way most Machine Learning models work on Spark
are not straightforward, and they need lots feature engineering to work. That's why we created the feature engineering
section inside the Transformer.

To import the Machine Learning Library you just need to say to import Optimus and the ML API:

.. code-block:: python

    from pyspark.sql import Row, types
    from pyspark.ml import feature, classification
    from optimus import Optimus
    from optimus.ml.models import ML
    from optimus.ml.functions import *

    op = Optimus()
    ml = ML()
    spark = op.spark
    sc = op.sc

Now with Optimus you can use this really easy feature engineering with our Machine Learning Library.

Let's take a look of what Optimus can do for you:

Methods for Machine Learning
---------------------------------

ml.logistic_regression_text(df, input_col)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method runs a logistic regression for input (text) DataFrame.

Let's create a sample dataframe to see how it works.

.. code-block:: python

    # Import Row from pyspark
    from pyspark.sql import Row
    # Importing Optimus
    import optimus as op

    df = op.sc. \
        parallelize([Row(sentence='this is a test', label=0.),
                     Row(sentence='this is another test', label=1.)]). \
        toDF()

    df.table()

+-----+--------------------+
|label|            sentence|
+-----+--------------------+
|  0.0|      this is a test|
+-----+--------------------+
|  1.0|this is another test|
+-----+--------------------+

.. code-block:: python

    df_predict, ml_model = ml.logistic_regression_text(df, "sentence")

This instruction will return two things, first the DataFrame with predictions and steps to build it with a
pipeline and a Spark machine learning model where the third step will be the logistic regression.

The columns of df_predict are:

.. code-block:: python

    df_predict.columns

    ['label',
    'sentence',
    'Tokenizer_4df79504b43d7aca6c0b__output',
    'CountVectorizer_421c9454cfd127d9deff__output',
    'LogisticRegression_406a8cef8029cfbbfeda__rawPrediction',
    'LogisticRegression_406a8cef8029cfbbfeda__probability',
    'LogisticRegression_406a8cef8029cfbbfeda__prediction']

The names are long because those are the uid for each step in the pipeline. So lets see the prediction compared
with the actual labels:

.. code-block:: python

    df_predict.cols.select([[0,6]).table()

+-----+---------------------------------------------------+
|label|LogisticRegression_406a8cef8029cfbbfeda__prediction|
+-----+---------------------------------------------------+
|  0.0|                                                0.0|
+-----+---------------------------------------------------+
|  1.0|                                                1.0|
+-----+---------------------------------------------------+

So we just did ML with a single line in Optimus. The model is also exposed in the `ml_model` variable so you can
save it and evaluate it.


ml.n_gram(df, input_col, n=2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method converts the input array of strings inside of a Spark DF into an array of n-grams. The default n is 2 so
it will produce bi-grams.

Let's create a sample dataframe to see how it works.

.. code-block:: python

    df = op.sc. \
        parallelize([['this is the best sentence ever'],
                     ['this is however the worst sentence available']]). \
        toDF(schema=types.StructType().add('sentence', types.StringType()))

    df_model, tfidf_model = n_gram(df, input_col="sentence", n=2)

The columns of df_predict are:

.. code-block:: python

    ['sentence',
     'Tokenizer_4a0eb7921c3a33b0bec5__output',
     'StopWordsRemover_4c5b9a5473e194516f3f__output',
     'CountVectorizer_41638674bb4c4a8d454c__output',
     'NGram_4e1d89fc70917c522134__output',
     'CountVectorizer_4513a7ba6ce22e617be7__output',
     'VectorAssembler_42719455dc1bde0c2a24__output',
     'features']

So lets see the bi-grams (we can change n as we want) for the sentences:

.. code-block:: python

     df_model.cols.select([[0,4]).table()

+--------------------------------------------+---------------------------------------------------+
|sentence                                    |NGram_4e1d89fc70917c522134__output                 |
+--------------------------------------------+---------------------------------------------------+
|this is the best sentence ever              |[best sentence, sentence ever]                     |
+--------------------------------------------+---------------------------------------------------+
|this is however the worst sentence available|[however worst, worst sentence, sentence available]|
+--------------------------------------------+---------------------------------------------------+

And that's it. N-grams with only one line of code.

Above we've been using the Pyspark Pipes definitions of Daniel Acuña, that he merged with Optimus, and because
we use multiple pipelines we need those big names for the resulting columns, so we can know which uid correspond
to each step.

**Tree models with Optimus**

Yes the rumor is true, now you can build Decision Trees, Random Forest models and also Gradient Boosted Trees
with just one line of code in Optimus. Let's download some sample data for analysis.

We got this dataset from Kaggle. The features are computed from a digitized image of a fine needle aspirate (FNA) of
a breast mass. They describe characteristics of the cell nuclei present in the image. n the 3-dimensional
space is that described in: [K. P. Bennett and O. L. Mangasarian: "Robust Linear Programming Discrimination of
Two Linearly Inseparable Sets", Optimization Methods and Software 1, 1992, 23-34].

Let's download it with Optimus and save it into a DF:

.. code-block:: python

    # Downloading and creating Spark DF
    df = op.load.url("https://raw.githubusercontent.com/ironmussa/Optimus/master/tests/data_cancer.csv")

ml.random_forest(df, columns, input_col)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One of the best "tree" models for machine learning is Random Forest. What about creating a RF model with just
one line? With Optimus is really easy.

Let's download some sample data for analysis.

.. code-block:: python

    df_predict, rf_model = ml.random_forest(df_cancer, columns, "diagnosis")

This will create a DataFrame with the predictions of the Random Forest model.

Let's see df_predict:

.. code-block:: python

    ['label',
     'diagnosis',
     'radius_mean',
     'texture_mean',
     'perimeter_mean',
     'area_mean',
     'smoothness_mean',
     'compactness_mean',
     'concavity_mean',
     'concave points_mean',
     'symmetry_mean',
     'fractal_dimension_mean',
     'features',
     'rawPrediction',
     'probability',
     'prediction']

So lets see the prediction compared with the actual label:

.. code-block:: python

    df_predict.select([0,15]).table()

+-----+----------+
|label|prediction|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       0.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  0.0|       0.0|
+-----+----------+
only showing top 20 rows

The rf_model variable contains the Random Forest model for analysis.

It will be the same for Decision Trees and Gradient Boosted Trees, let's check it out.

ml.decision_tree(df, columns, input_col)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df_predict, dt_model = ml.random_forest(df_cancer, columns, "diagnosis")

This will create a DataFrame with the predictions of the Decision Tree model.

Let's see df_predict:

.. code-block:: python

    ['label',
     'diagnosis',
     'radius_mean',
     'texture_mean',
     'perimeter_mean',
     'area_mean',
     'smoothness_mean',
     'compactness_mean',
     'concavity_mean',
     'concave points_mean',
     'symmetry_mean',
     'fractal_dimension_mean',
     'features',
     'rawPrediction',
     'probability',
     'prediction']

So lets see the prediction compared with the actual label:

.. code-block:: python

    df_predict.select([0,15]).table()


+-----+----------+
|label|prediction|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  0.0|       0.0|
+-----+----------+
only showing top 20 rows


ml.gbt(df, columns, input_col)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    df_predict, gbt_model = ml.gbt(df_cancer, columns, "diagnosis")

This will create a DataFrame with the predictions of the Gradient Boosted Trees model.

Let's see df_predict:

.. code-block:: python

    ['label',
     'diagnosis',
     'radius_mean',
     'texture_mean',
     'perimeter_mean',
     'area_mean',
     'smoothness_mean',
     'compactness_mean',
     'concavity_mean',
     'concave points_mean',
     'symmetry_mean',
     'fractal_dimension_mean',
     'features',
     'rawPrediction',
     'probability',
     'prediction']

So lets see the prediction compared with the actual label:

.. code-block:: python

    df_predict.select([0,15]).show()


+-----+----------+
|label|prediction|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  1.0|       1.0|
+-----+----------+
|  0.0|       0.0|
+-----+----------+
only showing top 20 rows
