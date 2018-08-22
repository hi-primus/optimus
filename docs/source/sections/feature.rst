.. _feature-engineering:

Feature Engineering with Optimus
==================================

Now with Optimus we have made easy the process of Feature Engineering.


When we talk about Feature Engineering we refer to creating new features from your existing ones to improve model
performance. Sometimes this is the case, or sometimes you need to do it because a certain model doesn't recognize
the data as you have it, so these transformations let you run most of Machine and Deep Learning algorithms.

These methods are part of the DataFrameTransformer, and they are a high level of abstraction for Spark Feature
Engineering methods. You'll see how easy it is to prepare your data with Optimus for Machine Learning.


Methods for Feature Engineering
---------------------------------

fe.string_to_index(input_cols)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method maps a string column of labels to an ML column of label indices. If the input column is numeric, we cast it
to string and index the string values.


``df`` Data frame to transform
``input_cols`` argument receives a list of columns to be indexed.

Let's start by creating a DataFrame with Optimus.

.. code:: python

    from pyspark.sql import Row, types
    from pyspark.ml import feature, classification

    from optimus import Optimus

    from optimus.ml.models import ML
    import optimus.ml.feature as fe

    op = Optimus()
    ml = ML()
    spark = op.spark
    sc = op.sc

    # Creating sample DF
    data = [('Japan', 'Tokyo', 37800000),('USA', 'New York', 19795791),('France', 'Paris', 12341418),
              ('Spain','Madrid',6489162)]
    df = op.spark.createDataFrame(data, ["country", "city", "population"])

    df.table()

+-------+--------+----------+
|country|    city|population|
+-------+--------+----------+
|  Japan|   Tokyo|  37800000|
+-------+--------+----------+
|    USA|New York|  19795791|
+-------+--------+----------+
| France|   Paris|  12341418|
+-------+--------+----------+
|  Spain|  Madrid|   6489162|
+-------+--------+----------+

.. code:: python

    # Indexing columns 'city" and 'country'
    df_sti = fe.string_to_index(df, input_cols=["city", "country"])

    # Show indexed DF
    df_sti.table()

+-------+--------+----------+----------+-------------+
|country|    city|population|city_index|country_index|
+-------+--------+----------+----------+-------------+
|  Japan|   Tokyo|  37800000|       1.0|          1.0|
+-------+--------+----------+----------+-------------+
|    USA|New York|  19795791|       2.0|          3.0|
+-------+--------+----------+----------+-------------+
| France|   Paris|  12341418|       3.0|          2.0|
+-------+--------+----------+----------+-------------+
|  Spain|  Madrid|   6489162|       0.0|          0.0|
+-------+--------+----------+----------+-------------+


fe.index_to_string(input_cols)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method maps a column of indices back to a new column of corresponding string values. The index-string mapping is
either from the ML (Spark) attributes of the input column, or from user-supplied labels (which take precedence over
ML attributes).

``df`` Data frame to transform
``input_cols`` argument receives a list of columns to be indexed.

Let's go back to strings with the DataFrame we created in the last step.

.. code:: python

    # Indexing columns 'city" and 'country'
    df_sti = fe.string_to_index(df, input_cols=["city", "country"])

    # Show indexed DF
    df_sti.table()

+-------+--------+----------+----------+-------------+
|country|    city|population|city_index|country_index|
+-------+--------+----------+----------+-------------+
|  Japan|   Tokyo|  37800000|       1.0|          1.0|
+-------+--------+----------+----------+-------------+
|    USA|New York|  19795791|       2.0|          3.0|
+-------+--------+----------+----------+-------------+
| France|   Paris|  12341418|       3.0|          2.0|
+-------+--------+----------+----------+-------------+
|  Spain|  Madrid|   6489162|       0.0|          0.0|
+-------+--------+----------+----------+-------------+

.. code:: python

    # Going back to strings from index
    df_its = fe.string_to_index(df_sti, input_cols=["country_index"])

    # Show DF with column "county_index" back to string
    df_its.table()

+-------+--------+----------+-------------+----------+--------------------+
|country|    city|population|country_index|city_index|country_index_string|
+-------+--------+----------+-------------+----------+--------------------+
|  Japan|   Tokyo|  37800000|          1.0|       1.0|              Japan |
+-------+--------+----------+-------------+----------+--------------------+
|    USA|New York|  19795791|          3.0|       2.0|                USA |
+-------+--------+----------+-------------+----------+--------------------+
| France|   Paris|  12341418|          2.0|       3.0|             France |
+-------+--------+----------+-------------+----------+--------------------+
|  Spain|  Madrid|   6489162|          0.0|       0.0|              Spain |
+-------+--------+----------+-------------+----------+--------------------+


fe.one_hot_encoder(input_cols)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method maps a column of label indices to a column of binary vectors, with at most a single one-value.

``df`` Data frame to transform
``input_cols`` argument receives a list of columns to be encoded.

Let's create a sample dataframe to see what OHE does:

.. code:: python

    # Creating DataFrame
    data = [
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "c")
    ]
    df = op.spark.createDataFrame(data,["id", "category"])

    # One Hot Encoding
    df_ohe = fe.one_hot_encoder(df, input_cols=["id"])

    # Show encoded dataframe
    df_ohe.table()

+---+--------+-------------+
| id|category|   id_encoded|
+---+--------+-------------+
|  0|       a|(5,[0],[1.0])|
+---+--------+-------------+
|  1|       b|(5,[1],[1.0])|
+---+--------+-------------+
|  2|       c|(5,[2],[1.0])|
+---+--------+-------------+
|  3|       a|(5,[3],[1.0])|
+---+--------+-------------+
|  4|       a|(5,[4],[1.0])|
+---+--------+-------------+
|  5|       c|    (5,[],[])|
+---+--------+-------------+

Transformer.vector_assembler(input_cols)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method combines a given list of columns into a single vector column.

``input_cols`` argument receives a list of columns to be encoded.

This is very important because lots of Machine Learning algorithms in Spark need this format to work.

Let's create a sample dataframe to see what vector assembler does:

.. code-block:: python

    # Import Vectors
    from pyspark.ml.linalg import Vectors

    # Creating DataFrame
    data = [(0, 18, 1.0, Vectors.dense([0.0, 10.0, 0.5]), 1.0)]

    df = op.spark.createDataFrame(data,["id", "hour", "mobile", "user_features", "clicked"])

    # Assemble features
    df_va = fe.vector_assembler(df, input_cols=["hour", "mobile", "user_features"])

    # Show assembled df
    print("Assembled columns 'hour', 'mobile', 'user_features' to vector column 'features'")
    df_va.select("features", "clicked").table()


+-----------------------+-------+
|features               |clicked|
+-----------------------+-------+
|[18.0,1.0,0.0,10.0,0.5]|1.0    |
+-----------------------+-------+

fe.normalizer(input_cols,p=2.0)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method transforms a dataset of Vector rows, normalizing each Vector to have unit norm. It takes parameter p, which
specifies the p-norm used for normalization. (p=2) by default.

``input_cols`` argument receives a list of columns to be normalized.

``p`` argument is the p-norm used for normalization.



Let's create a sample dataframe to see what normalizer does:

.. code:: python
    # Import Vectors
    from pyspark.ml.linalg import Vectors

    data = [
    (0, Vectors.dense([1.0, 0.5, -1.0]),),
    (1, Vectors.dense([2.0, 1.0, 1.0]),),
    (2, Vectors.dense([4.0, 10.0, 2.0]),)
    ]

    df = op.spark.createDataFrame(data,["id", "features"])

    df_norm = fe.normalizer(df, input_cols=["features"], p=2.0)

    df_norm.table()


+---+--------------+-----------------------------------------------------------+
|id |features      |features_normalized                                        |
+---+--------------+-----------------------------------------------------------+
|0  |[1.0,0.5,-1.0]|[0.6666666666666666,0.3333333333333333,-0.6666666666666666]|
+---+--------------+-----------------------------------------------------------+
|1  |[2.0,1.0,1.0] |[0.8164965809277261,0.4082482904638631,0.4082482904638631] |
+---+--------------+-----------------------------------------------------------+
|2  |[4.0,10.0,2.0]|[0.3651483716701107,0.9128709291752769,0.18257418583505536]|
+---+--------------+-----------------------------------------------------------+