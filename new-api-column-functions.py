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

# In[188]:


# Thanks Mr Powers
df = op.create.df([
                ("  I like     fish  ", 1, "dog", "&^%$#housé", "cat-car", "a","1", "20150510", "3"),
                ("    zombies", 2, "cat", "tv", "dog-tv", "b","2", "20160510", "3"),
                ("simpsons   cat lady", 2, "frog", "table","eagle-tv-plus","1","3", "20170510", "4"),
                (None, 3, "eagle", "glass", "lion-pc", "c","4", "20180510", "5"),
    
            ],
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
                
            ])

df.show()


# In[5]:


df.cols().date_transform("date", "yyyyMMdd", "dd-MM-YYYY").show()


# In[101]:


df.cols().age_from_date("clientAge", "yyyyMMdd", "date").show()


# In[191]:


df.cols().remove_accents("thing").show()


# In[190]:


df.cols().remove_special_chars("thing").show()

