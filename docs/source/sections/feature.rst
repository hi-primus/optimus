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

Transformer.string_to_index(input_cols)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method maps a string column of labels to an ML column of label indices. If the input column is numeric, we cast it
to string and index the string values.

``input_cols`` argument receives a list of columns to be indexed.

Let's start by creating a DataFrame with Optimus.

.. code:: python

    # Importing Optimus
    import optimus as op
    #Importing utilities
    tools = op.Utilities()

    # Creating DF with Optimus
    data = [('Japan', 'Tokyo', 37800000),('USA', 'New York', 19795791),('France', 'Paris', 12341418),
                  ('Spain','Madrid',6489162)]
    df = tools.create_data_frame(data, ["country", "city", "population"])

    # Instantiating transformer
    transformer = op.DataFrameTransformer(df)

    # Show DF
    transformer.show()

    +-------+--------+----------+
    |country|    city|population|
    +-------+--------+----------+
    |  Japan|   Tokyo|  37800000|
    |    USA|New York|  19795791|
    | France|   Paris|  12341418|
    |  Spain|  Madrid|   6489162|
    +-------+--------+----------+

    # Indexing columns 'city" and 'country'
    transformer.string_to_index(["city", "country"])

    # Show indexed DF
    transformer.show()

    +-------+--------+----------+----------+-------------+
    |country|    city|population|city_index|country_index|
    +-------+--------+----------+----------+-------------+
    |  Japan|   Tokyo|  37800000|       1.0|          1.0|
    |    USA|New York|  19795791|       2.0|          3.0|
    | France|   Paris|  12341418|       3.0|          2.0|
    |  Spain|  Madrid|   6489162|       0.0|          0.0|
    +-------+--------+----------+----------+-------------+


Transformer.index_to_string(input_cols)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method maps a column of indices back to a new column of corresponding string values. The index-string mapping is
either from the ML (Spark) attributes of the input column, or from user-supplied labels (which take precedence over
ML attributes).

``input_cols`` argument receives a list of columns to be indexed.

Let's go back to strings with the DataFrame we created in the last step.

.. code:: python

    # Importing Optimus
    import optimus as op
    #Importing utilities
    tools = op.Utilities()

    # Instantiating transformer
    transformer = op.DataFrameTransformer(df)

    # Show DF
    transformer.show()

    +-------+--------+----------+
    |country|    city|population|
    +-------+--------+----------+
    |  Japan|   Tokyo|  37800000|
    |    USA|New York|  19795791|
    | France|   Paris|  12341418|
    |  Spain|  Madrid|   6489162|
    +-------+--------+----------+

    # Indexing columns 'city" and 'country'
    transformer.string_to_index(["city", "country"])

    # Show indexed DF
    transformer.show()

    +-------+--------+----------+----------+-------------+
    |country|    city|population|city_index|country_index|
    +-------+--------+----------+----------+-------------+
    |  Japan|   Tokyo|  37800000|       1.0|          1.0|
    |    USA|New York|  19795791|       2.0|          3.0|
    | France|   Paris|  12341418|       3.0|          2.0|
    |  Spain|  Madrid|   6489162|       0.0|          0.0|
    +-------+--------+----------+----------+-------------+

    # Going back to strings from index
    transformer.index_to_string(["country_index"])

    # Show DF with column "county_index" back to string
    transformer.show()

    +-------+--------+----------+-------------+----------+--------------------+
    |country|    city|population|country_index|city_index|country_index_string|
    +-------+--------+----------+-------------+----------+--------------------+
    |  Japan|   Tokyo|  37800000|          1.0|       1.0|              Japan |
    |    USA|New York|  19795791|          3.0|       2.0|                USA |
    | France|   Paris|  12341418|          2.0|       3.0|             France |
    |  Spain|  Madrid|   6489162|          0.0|       0.0|              Spain |
    +-------+--------+----------+-------------+----------+--------------------+


Transformer.one_hot_encoder(input_cols)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This method maps a column of label indices to a column of binary vectors, with at most a single one-value.

``input_cols`` argument receives a list of columns to be encoded.

Let's create a sample dataframe to see what does OHE does:

.. code:: python

    # Importing Optimus
    import optimus as op
    #Importing utilities
    tools = op.Utilities()

    # Creating DataFrame
    data = [
    (0, "a"),
    (1, "b"),
    (2, "c"),
    (3, "a"),
    (4, "a"),
    (5, "c")
    ]
    df = tools.create_data_frame(data,["id", "category"])

    # Instantiating the transformer
    transformer = op.DataFrameTransformer(df)

    # One Hot Encoding
    transformer.one_hot_encoder(["id"])

    # Show encoded dataframe
    transformer.show()

    +---+--------+-------------+
    | id|category|   id_encoded|
    +---+--------+-------------+
    |  0|       a|(5,[0],[1.0])|
    |  1|       b|(5,[1],[1.0])|
    |  2|       c|(5,[2],[1.0])|
    |  3|       a|(5,[3],[1.0])|
    |  4|       a|(5,[4],[1.0])|
    |  5|       c|    (5,[],[])|
    +---+--------+-------------+
