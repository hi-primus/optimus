Columns Operation
======================

Here you will see a detailed overview of all the columns operations available in Optimus.
You can access the operation via ``df.cols``

Let's create a sample dataframe to start working.

.. code:: python

    # Import Optimus
    from optimus import Optimus
    # Create Optimus instance
    op = Optimus()

    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

    df = op.create.df(
                [
                    ("words", "str", True),
                    ("num", "int", True),
                    ("animals", "str", True),
                    ("thing", StringType(), True),
                    ("two strings", StringType(), True),
                    ("filter", StringType(), True),
                    ("num 2", "string", True),
                    ("col_array",  ArrayType(StringType()), True),
                    ("col_int",  ArrayType(IntegerType()), True)

                ]
    ,
    [
                    ("  I like     fish  ", 1, "dog", "housé", "cat-car", "a","1",["baby", "sorry"],[1,2,3]),
                    ("    zombies", 2, "cat", "tv", "dog-tv", "b","2",["baby 1", "sorry 1"],[3,4]),
                    ("simpsons   cat lady", 2, "frog", "table","eagle-tv-plus","1","3", ["baby 2", "sorry 2"], [5,6,7]),
                    (None, 3, "eagle", "glass", "lion-pc", "c","4", ["baby 3", "sorry 3"] ,[7,8])
                ])

To see the dataframe we will use the ``table()`` function, a much better way to see your results,
instead of the built-in ```show()`` function.

.. code-block:: python

    df.table()


+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+
|              words|num|animals|thing|  two strings|filter|num 2|        col_array|  col_int|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+
|  I like     fish  |  1|    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+
|            zombies|  2|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+
|simpsons   cat lady|  2|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+
|               null|  3|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+

