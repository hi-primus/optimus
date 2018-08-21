Row Operation
======================

Here you will see a detailed overview of all the row operations available in Optimus.
You can access the operations via ``df.rows``

Let's create a sample dataframe to start working.

.. code-block:: python
    # Import Optimus
    from optimus import Optimus
    # Create Optimus instance
    op = Optimus()

    df = op.create.df([
                ("words", "str", True),
                ("num", "int", True),
                ("animals", "str", True),
                ("thing", StringType(), True),
                ("second", "int", True),
                ("filter", StringType(), True)
            ],
            [
                ("  I like     fish  ", 1, "dog dog", "housé", 5 , "a"),
                ("    zombies", 2, "cat", "tv", 6, "b"),
                ("simpsons   cat lady", 2, "frog", "table", 7, "1"),
                (None, 3, "eagle", "glass", 8, "c")

            ])

    df.table()

+-------------------+---+-------+-----+------+------+
|              words|num|animals|thing|second|filter|
+-------------------+---+-------+-----+------+------+
|  I like     fish  |  1|dog dog|housé|     5|     a|
+-------------------+---+-------+-----+------+------+
|            zombies|  2|    cat|   tv|     6|     b|
+-------------------+---+-------+-----+------+------+
|simpsons   cat lady|  2|   frog|table|     7|     1|
+-------------------+---+-------+-----+------+------+
|               null|  3|  eagle|glass|     8|     c|
+-------------------+---+-------+-----+------+------+

rows.append(row)
-------------------

Append a row at the end of a dataframe

.. code-block:: python
    df.rows.append(["this is a word",2, "this is an animal", "this is a thing", 64, "this is a filter"]).table()

+-------------------+---+-----------------+---------------+------+----------------+
|              words|num|          animals|          thing|second|          filter|
+-------------------+---+-----------------+---------------+------+----------------+
|  I like     fish  |  1|          dog dog|          housé|     5|               a|
+-------------------+---+-----------------+---------------+------+----------------+
|            zombies|  2|              cat|             tv|     6|               b|
+-------------------+---+-----------------+---------------+------+----------------+
|simpsons   cat lady|  2|             frog|          table|     7|               1|
+-------------------+---+-----------------+---------------+------+----------------+
|               null|  3|            eagle|          glass|     8|               c|
+-------------------+---+-----------------+---------------+------+----------------+
|     this is a word|  2|this is an animal|this is a thing|    64|this is a filter|
+-------------------+---+-----------------+---------------+------+----------------+

rows.sort()
---------------

Sort the columns by rows or multiple conditions.

.. code-block:: python
    df.rows.sort("animals").table()

+-------------------+---+-------+-----+------+------+
|              words|num|animals|thing|second|filter|
+-------------------+---+-------+-----+------+------+
|simpsons   cat lady|  2|   frog|table|     7|     1|
+-------------------+---+-------+-----+------+------+
|               null|  3|  eagle|glass|     8|     c|
+-------------------+---+-------+-----+------+------+
|  I like     fish  |  1|dog dog|housé|     5|     a|
+-------------------+---+-------+-----+------+------+
|            zombies|  2|    cat|   tv|     6|     b|
+-------------------+---+-------+-----+------+------+

.. code-block:: python
    df.rows.sort("animals", "desc").table()

+-------------------+---+-------+-----+------+------+
|              words|num|animals|thing|second|filter|
+-------------------+---+-------+-----+------+------+
|simpsons   cat lady|  2|   frog|table|     7|     1|
+-------------------+---+-------+-----+------+------+
|               null|  3|  eagle|glass|     8|     c|
+-------------------+---+-------+-----+------+------+
|  I like     fish  |  1|dog dog|housé|     5|     a|
+-------------------+---+-------+-----+------+------+
|            zombies|  2|    cat|   tv|     6|     b|
+-------------------+---+-------+-----+------+------+

.. code-block:: python
    df.rows.sort([("animals","desc"),("thing","asc")]).table()

+-------------------+---+-------+-----+------+------+
|              words|num|animals|thing|second|filter|
+-------------------+---+-------+-----+------+------+
|simpsons   cat lady|  2|   frog|table|     7|     1|
+-------------------+---+-------+-----+------+------+
|               null|  3|  eagle|glass|     8|     c|
+-------------------+---+-------+-----+------+------+
|  I like     fish  |  1|dog dog|housé|     5|     a|
+-------------------+---+-------+-----+------+------+
|            zombies|  2|    cat|   tv|     6|     b|
+-------------------+---+-------+-----+------+------+

rows.select(*args, **kwargs)
----------------------------

Alias of Spark filter function. Return rows that match a expression.

.. code-block:: python
    df.rows.select(df["num"]==1).table()

+-------------------+---+-------+-----+------+------+
|              words|num|animals|thing|second|filter|
+-------------------+---+-------+-----+------+------+
|  I like     fish  |  1|dog dog|housé|     5|     a|
+-------------------+---+-------+-----+------+------+

rows.select_by_dtypes(col_name, data_type=None)
-------------------------------------------------

This function has built in order to filter some type of row depending of the var type detected by python

.. code-block:: python
    df.rows.select_by_dtypes("filter", "integer").table()

+-------------------+---+-------+-----+------+------+
|              words|num|animals|thing|second|filter|
+-------------------+---+-------+-----+------+------+
|simpsons   cat lady|  2|   frog|table|     7|     1|
+-------------------+---+-------+-----+------+------+

rows.drop(where=None)
------------------------

Drop a row depending on a dataframe expression

.. code-block:: python
    df.rows.drop((df["num"]==2) | (df["second"]==5)).table()

+-----+---+-------+-----+------+------+
|words|num|animals|thing|second|filter|
+-----+---+-------+-----+------+------+
| null|  3|  eagle|glass|     8|     c|
+-----+---+-------+-----+------+------+

rows.drop_by_dtypes(col_name, data_type=None)
---------------------------------------------

Drop rows by cell data type

.. code-block:: python
    df.rows.drop_by_dtypes("filter", "int").table()

+-------------------+---+-------+-----+------+------+
|              words|num|animals|thing|second|filter|
+-------------------+---+-------+-----+------+------+
|  I like     fish  |  1|dog dog|housé|     5|     a|
+-------------------+---+-------+-----+------+------+
|            zombies|  2|    cat|   tv|     6|     b|
+-------------------+---+-------+-----+------+------+
|               null|  3|  eagle|glass|     8|     c|
+-------------------+---+-------+-----+------+------+

Drop using an abstract UDF
--------------------------------

.. code-block:: python
    from optimus.functions import abstract_udf as audf

    def func_data_type(value, attr):
        return value >1


    df.rows.drop(audf("num", func_data_type, "boolean")).table()

+-------------------+---+-------+-----+------+------+
|              words|num|animals|thing|second|filter|
+-------------------+---+-------+-----+------+------+
|  I like     fish  |  1|dog dog|housé|     5|     a|
+-------------------+---+-------+-----+------+------+
