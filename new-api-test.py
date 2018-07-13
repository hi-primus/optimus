
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# In[2]:


from optimus import *

from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

sc = SparkSession.builder.getOrCreate()


# In[3]:


# Create optimus
op = Optimus(sc)


# ## Create dataframe
# ### Spark
# 
# This is ugly:
# 
# ```
# val someData = Seq(
#   Row(8, "bat"),
#   Row(64, "mouse"),
#   Row(-27, "horse")
# )
# 
# val someSchema = List(
#   StructField("number", IntegerType, true),
#   StructField("word", StringType, true)
# )
# 
# val someDF = spark.createDataFrame(
#   spark.sparkContext.parallelize(someData),
#   StructType(someSchema)
# )```

# In[4]:


# Thanks Mr Powers
df = op.create.df(
    [
                ("words", "str", True),
                ("num", "int", True),
                ("animals", "str", True),
                ("thing", StringType(), True),
                ("two strings", StringType(), True),
                ("filter", StringType(), True),
                ("num 2", "string", True),
                ("date", "string", True),
                ("num 3", "string", True)
                
            ],[
                ("  I like     fish  ", 1, "dog", "&^%$#housé", "cat-car", "a","1", "20150510", "3"),
                ("    zombies", 2, "cat", "tv", "dog-tv", "b","2", "20160510", "3"),
                ("simpsons   cat lady", 2, "frog", "table","eagle-tv-plus","1","3", "20170510", "4"),
                (None, 3, "eagle", "glass", "lion-pc", "c","4", "20180510", "5"),
    
            ]
            )

df.show()


# In[5]:


from pyspark.sql.functions import udf

# Use udf to define a row-at-a-time udf
@udf('double')
# Input/output are both a single double value
def plus_one(v):
      return v + 1

df.withColumn('v2', plus_one(df.v))


# In[11]:


from pyspark.sql.functions import pandas_udf, PandasUDFType

# Use pandas_udf to define a Pandas UDF
@pandas_udf('double', PandasUDFType.SCALAR)
# Input/output are both a pandas.Series of doubles

def pandas_plus_one(v):
    return v + 1

df.withColumn('num', pandas_plus_one(df.num)).show()


# In[7]:


get_ipython().system('pip install pyarrow==0.9.*')

