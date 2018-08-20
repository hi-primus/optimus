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

cols.append(col_name=None, value=None)
-----------------------------------------

Appends a column to a Dataframe

.. code-block:: python

    df = df.cols.append("new_col_1", 1)
    df.table()

+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|              words|num|animals|thing|  two strings|filter|num 2|        col_array|  col_int|new_col_1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|  I like     fish  |  1|    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|            zombies|  2|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|simpsons   cat lady|  2|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|               null|  3|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+


.. code-block:: python

    from pyspark.sql.functions import *

    df.cols.append([
        ("new_col_2", 2.22),
        ("new_col_3", lit(3))
        ]).table()

+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+
|              words|num|animals|thing|  two strings|filter|num 2|        col_array|  col_int|new_col_1|new_col_2|new_col_3|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+
|  I like     fish  |  1|    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|     2.22|        3|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+
|            zombies|  2|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|     2.22|        3|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+
|simpsons   cat lady|  2|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|     2.22|        3|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+
|               null|  3|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|     2.22|        3|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+

.. code-block:: python

    df.cols.append([
    ("new_col_4", "test"),
    ("new_col_5", df['num']*2),
    ("new_col_6", [1,2,3])
    ]).table()

+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+---------+
|              words|num|animals|thing|  two strings|filter|num 2|        col_array|  col_int|new_col_1|new_col_4|new_col_5|new_col_6|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+---------+
|  I like     fish  |  1|    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|     test|        2|[1, 2, 3]|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+---------+
|            zombies|  2|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|     test|        4|[1, 2, 3]|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+---------+
|simpsons   cat lady|  2|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|     test|        4|[1, 2, 3]|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+---------+
|               null|  3|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|     test|        6|[1, 2, 3]|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+---------+---------+---------+

cols.select(columns=None, regex=None, data_type=None)
------------------------------------------------------

Select columns using index, column name, regex or data type

.. code-block:: python

    columns = ["words", 1, "animals", 3]
    df.cols.select(columns).table()

+-------------------+---+-------+-----+
|              words|num|animals|thing|
+-------------------+---+-------+-----+
|  I like     fish  |  1|    dog|housé|
+-------------------+---+-------+-----+
|            zombies|  2|    cat|   tv|
+-------------------+---+-------+-----+
|simpsons   cat lady|  2|   frog|table|
+-------------------+---+-------+-----+
|               null|  3|  eagle|glass|
+-------------------+---+-------+-----+

.. code-block:: python

    df.cols.select("n.*", regex = True).show()

+---+-----+---------+
|num|num 2|new_col_1|
+---+-----+---------+
|  1|    1|        1|
+---+-----+---------+
|  2|    2|        1|
+---+-----+---------+
|  2|    3|        1|
+---+-----+---------+
|  3|    4|        1|
+---+-----+---------+

.. code-block:: python

    df.cols.select("*", data_type = "str").table()

+-----+-------------------+-------+------+-------------+-----+
|thing|              words|animals|filter|  two strings|num 2|
+-----+-------------------+-------+------+-------------+-----+
|housé|  I like     fish  |    dog|     a|      cat-car|    1|
+-----+-------------------+-------+------+-------------+-----+
|   tv|            zombies|    cat|     b|       dog-tv|    2|
+-----+-------------------+-------+------+-------------+-----+
|table|simpsons   cat lady|   frog|     1|eagle-tv-plus|    3|
+-----+-------------------+-------+------+-------------+-----+
|glass|               null|  eagle|     c|      lion-pc|    4|
+-----+-------------------+-------+------+-------------+-----+

cols.rename(columns_old_new=None, func=None)
----------------------------------------------

Changes the name of a column(s) dataFrame.

.. code-block:: python

    df.cols.rename('num','number').table()

+-------------------+------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|              words|number|animals|thing|  two strings|filter|num 2|        col_array|  col_int|new_col_1|
+-------------------+------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|  I like     fish  |     1|    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|
+-------------------+------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|            zombies|     2|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|
+-------------------+------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|simpsons   cat lady|     2|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|
+-------------------+------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|               null|     3|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|
+-------------------+------+-------+-----+-------------+------+-----+-----------------+---------+---------+

.. code-block:: python

    df.cols.rename([('num','number'),("animals","gods")], str.upper).table()

+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|              WORDS|NUM|ANIMALS|THING|  TWO STRINGS|FILTER|NUM 2|        COL_ARRAY|  COL_INT|NEW_COL_1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|  I like     fish  |  1|    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|            zombies|  2|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|simpsons   cat lady|  2|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|               null|  3|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+

.. code-block:: python

    df.cols.rename(str.lower).table()

+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|              words|num|animals|thing|  two strings|filter|num 2|        col_array|  col_int|new_col_1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|  I like     fish  |  1|    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|            zombies|  2|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|simpsons   cat lady|  2|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+
|               null|  3|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|
+-------------------+---+-------+-----+-------------+------+-----+-----------------+---------+---------+

cols.cast()
-----------------

Cast multiple columns to a specific datatype.

List of tuples of column names and types to be casted. This variable should have the following structure:

colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

The first parameter in each tuple is the column name, the second is the final datatype of column after
the transformation is made.

.. code-block:: python

    df.cols.cast([("num", "string"),("num 2", "integer")]).dtypes

     [('words', 'string'),
     ('num', 'string'),
     ('animals', 'string'),
     ('thing', 'string'),
     ('two strings', 'string'),
     ('filter', 'string'),
     ('num 2', 'int'),
     ('col_array', 'array<string>'),
     ('col_int', 'array<int>'),
     ('new_col_1', 'int')]

You can cast all columns to a specific type too.

.. code-block:: python

    df.cols.cast("*", "string").dtypes

    [('words', 'string'),
     ('num', 'string'),
     ('animals', 'string'),
     ('thing', 'string'),
     ('two strings', 'string'),
     ('filter', 'string'),
     ('num 2', 'string'),
     ('col_array', 'string'),
     ('col_int', 'string'),
     ('new_col_1', 'string')]


keep(columns=None, regex=None)
---------------------------------------

Only keep the columns specified.

.. code-block:: python
    df.cols.keep("num").table()

+---+
|num|
+---+
|  1|
+---+
|  2|
+---+
|  2|
+---+
|  3|
+---+

cols.move(column, position, ref_col)
--------------------------------------

Move a column to specific position

.. code-block:: python

df.cols.move("words", "after", "thing").table()

+---+-------+-----+-------------------+-------------+------+-----+-----------------+---------+---------+
|num|animals|thing|              words|  two strings|filter|num 2|        col_array|  col_int|new_col_1|
+---+-------+-----+-------------------+-------------+------+-----+-----------------+---------+---------+
|  1|    dog|housé|  I like     fish  |      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|
+---+-------+-----+-------------------+-------------+------+-----+-----------------+---------+---------+
|  2|    cat|   tv|            zombies|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|
+---+-------+-----+-------------------+-------------+------+-----+-----------------+---------+---------+
|  2|   frog|table|simpsons   cat lady|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|
+---+-------+-----+-------------------+-------------+------+-----+-----------------+---------+---------+
|  3|  eagle|glass|               null|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|
+---+-------+-----+-------------------+-------------+------+-----+-----------------+---------+---------+

sort(order="asc")
------------------------------

Sort dataframes columns asc or desc

.. code-block:: python

    df.cols.sort().table()

+-------+-----------------+---------+------+---------+---+-----+-----+-------------+-------------------+
|animals|        col_array|  col_int|filter|new_col_1|num|num 2|thing|  two strings|              words|
+-------+-----------------+---------+------+---------+---+-----+-----+-------------+-------------------+
|    dog|    [baby, sorry]|[1, 2, 3]|     a|        1|  1|    1|housé|      cat-car|  I like     fish  |
+-------+-----------------+---------+------+---------+---+-----+-----+-------------+-------------------+
|    cat|[baby 1, sorry 1]|   [3, 4]|     b|        1|  2|    2|   tv|       dog-tv|            zombies|
+-------+-----------------+---------+------+---------+---+-----+-----+-------------+-------------------+
|   frog|[baby 2, sorry 2]|[5, 6, 7]|     1|        1|  2|    3|table|eagle-tv-plus|simpsons   cat lady|
+-------+-----------------+---------+------+---------+---+-----+-----+-------------+-------------------+
|  eagle|[baby 3, sorry 3]|   [7, 8]|     c|        1|  3|    4|glass|      lion-pc|               null|
+-------+-----------------+---------+------+---------+---+-----+-----+-------------+-------------------+

.. code-block:: python

    df.cols.sort(order = "desc").table()

+-------------------+-------------+-----+-----+---+---------+------+---------+-----------------+-------+
|              words|  two strings|thing|num 2|num|new_col_1|filter|  col_int|        col_array|animals|
+-------------------+-------------+-----+-----+---+---------+------+---------+-----------------+-------+
|  I like     fish  |      cat-car|housé|    1|  1|        1|     a|[1, 2, 3]|    [baby, sorry]|    dog|
+-------------------+-------------+-----+-----+---+---------+------+---------+-----------------+-------+
|            zombies|       dog-tv|   tv|    2|  2|        1|     b|   [3, 4]|[baby 1, sorry 1]|    cat|
+-------------------+-------------+-----+-----+---+---------+------+---------+-----------------+-------+
|simpsons   cat lady|eagle-tv-plus|table|    3|  2|        1|     1|[5, 6, 7]|[baby 2, sorry 2]|   frog|
+-------------------+-------------+-----+-----+---+---------+------+---------+-----------------+-------+
|               null|      lion-pc|glass|    4|  3|        1|     c|   [7, 8]|[baby 3, sorry 3]|  eagle|
+-------------------+-------------+-----+-----+---+---------+------+---------+-----------------+-------+

drop()
---------------------------

Drops a list of columns

.. code-block:: python
    df2 = df.cols.drop("num")
    df2.table()

+-------------------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|              words|animals|thing|  two strings|filter|num 2|        col_array|  col_int|new_col_1|
+-------------------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|  I like     fish  |    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|
+-------------------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|            zombies|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|
+-------------------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|simpsons   cat lady|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|
+-------------------+-------+-----+-------------+------+-----+-----------------+---------+---------+
|               null|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|
+-------------------+-------+-----+-------------+------+-----+-----------------+---------+---------+

.. code-block:: python
    df2 = df.cols.drop(["num","words"])
    df2.table()


+-------+-----+-------------+------+-----+-----------------+---------+---------+
|animals|thing|  two strings|filter|num 2|        col_array|  col_int|new_col_1|
+-------+-----+-------------+------+-----+-----------------+---------+---------+
|    dog|housé|      cat-car|     a|    1|    [baby, sorry]|[1, 2, 3]|        1|
+-------+-----+-------------+------+-----+-----------------+---------+---------+
|    cat|   tv|       dog-tv|     b|    2|[baby 1, sorry 1]|   [3, 4]|        1|
+-------+-----+-------------+------+-----+-----------------+---------+---------+
|   frog|table|eagle-tv-plus|     1|    3|[baby 2, sorry 2]|[5, 6, 7]|        1|
+-------+-----+-------------+------+-----+-----------------+---------+---------+
|  eagle|glass|      lion-pc|     c|    4|[baby 3, sorry 3]|   [7, 8]|        1|
+-------+-----+-------------+------+-----+-----------------+---------+---------+

chaining
----------------------------------------------

The past transformations were done step by step, but this can be achieved by chaining all operations into one line of code, like the cell below. This way is much more efficient and scalable because it uses all optimization issues from the lazy evaluation approach.

.. code-block:: python

    df\
    .cols.rename([('num','number')])\
    .cols.drop(["number","words"])\
    .withColumn("new_col_2", lit("spongebob"))\
    .cols.append("new_col_1", 1)\
    .cols.sort(order= "desc")\
    .rows.drop(df["num 2"] == 3)\
    .table()

+-----------+-----+-----+---------+---------+------+---------+-----------------+-------+
|two strings|thing|num 2|new_col_2|new_col_1|filter|  col_int|        col_array|animals|
+-----------+-----+-----+---------+---------+------+---------+-----------------+-------+
|    cat-car|housé|    1|spongebob|        1|     a|[1, 2, 3]|    [baby, sorry]|    dog|
+-----------+-----+-----+---------+---------+------+---------+-----------------+-------+
|     dog-tv|   tv|    2|spongebob|        1|     b|   [3, 4]|[baby 1, sorry 1]|    cat|
+-----------+-----+-----+---------+---------+------+---------+-----------------+-------+
|    lion-pc|glass|    4|spongebob|        1|     c|   [7, 8]|[baby 3, sorry 3]|  eagle|
+-----------+-----+-----+---------+---------+------+---------+-----------------+-------+