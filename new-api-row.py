from optimus import *

from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

sc = SparkSession.builder.getOrCreate()

# Create optimus
op = Optimus(sc)

# Thanks Mr Powers
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

df.show()

df.rows().append(["this is a word",2, "this is an animal", "this is a thing", 64, "this is a filter"]).show()

df.rows().drop(1).show()

df.rows().filter_by_type("filter", type = "integer").show()

df.rows().lookup("animals", ["dog", "cat", "eagle"], "just animals").show()

def func(value): 
    return str(int(value) + 1 )

df.rows().apply_by_type([('num', 'integer', func)]).show()
