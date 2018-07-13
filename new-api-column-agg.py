

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
df = op.create.df([
                ("  I like     fish  ", 1, "dog", "housé", 5 ),
                ("    zombies", 2, "cat", "tv", 6),
                ("simpsons   cat lady", 2, "frog", "table", 7),
                (None, 3, "eagle", "glass", 8)
            ],
            [
                ("words", "str", True),
                ("num", "int", True),
                ("animals", "str", True),
                ("thing", StringType(), True),
                ("second", "int", True)
            ])

df.show()


# ### Math Operations

# In[5]:


print(df.cols().min("num"))
print(df.cols().max("num"))
print(df.cols().range(["num","second"]))
print(df.cols().median(["num","second"]))

print(df.cols().stddev("num"))
print(df.cols().kurt("num"))
print(df.cols().mean("num"))
print(df.cols().skewness("num"))
print(df.cols().sum("num"))
print(df.cols().variance("num"))


# In[11]:


print(df.cols().range(["num","second"]))


# ### String Operations

# In[29]:


df.show()


# In[32]:


df    .cols().trim("words")    .show()


# In[34]:


df    .cols().trim("words")    .cols().lower("words")    .cols().upper(["animals", "thing"])    .cols().reverse("thing")    .show()

