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

# In[8]:


# Thanks Mr Powers
df = op.create.df(
            [
                ("words", "str", True),
                ("num", "int", True),
                ("animals", "str", True),
                ("thing", StringType(), True),
                ("two strings", StringType(), True),
                ("filter", StringType(), True),
                ("num 2", "string", True)

            ]
,
[
                ("  I like     fish  ", 1, "dog", "housé", "cat-car", "a","1"),
                ("    zombies", 2, "cat", "tv", "dog-tv", "b","2"),
                ("simpsons   cat lady", 2, "frog", "table","eagle-tv-plus","1","3"),
                (None, 3, "eagle", "glass", "lion-pc", "c","4")
            ])

df.show()


# ## Create Columns
# ### Spark
# * You can not create multiple columns at the same time
# * You need to use the lit function. lit???
# 
# ### Pandas
# * Similiar behavior
# 

# In[5]:


df = df.cols().create("new_col_1", 1)
df.show()


# In[6]:


from pyspark.sql.functions import *

sf = df.cols().create([
    ("new_col_2", 2.22),
    ("new_col_3", lit(3)),
    ("new_col_4", "test"),
    ("new_col_5", df['num']*2)
    ])

df.show()


# ## Select columns
# ### Spark
# * You can not select columns by string and index at the same time
# 
# ### Pandas
# * You can not select columns by string and index at the same time

# In[7]:


columns = ["words", 1, "animals", 3]
df.cols().select(columns).show()


# In[8]:


df.cols().select(regex = "n.*").show()


# ## Rename Column
# ### Spark
# You can not rename multiple columns using Spark Vanilla API
# 
# 
# ### Pandas
# * Almost the same behavior https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.rename.html

# In[24]:


df.cols().rename([('num','number')]).show()


# In[26]:


df.cols().rename(func = str.lower).show()


# In[11]:


df.cols().rename(func = str.upper).show()


# ## Cast a columns
# 
# ### Spark
# * Can not cast multiple columns
# 
# ### Pandas
# This is a opinionated way to handle column casting. 
# One of the first thing that every data cleaning process need to acomplish is define a data dictionary.
# Because of that we prefer to create a tuple like this:
# 
# df.cols().cast(
# [("words","str"),
# ("num","int"),
# ("animals","float"),
# ("thing","str")]
# )
# 
# instead of pandas
# 
# pd.Series([1], dtype='int32')
# pd.Series([2], dtype='string')
# 
# https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.astype.html

# In[31]:


df.cols().cast([("num", "string"),("num 2", "integer")])


# ## Keep columns
# ### Spark
# * You can not remove multiple columns
# 
# ### Pandas
# * Handle in pandas with drop
# 

# In[23]:


from pyspark.sql.functions import *
df.withColumn("num", col("num").cast(StringType()))


# In[16]:


df.cols().keep("num").show()


# ## Move columns
# ### Spark
# Do not exist in spark
# 
# ### Pandas
# Do not exist in pandas

# In[17]:


df.cols().move("words", "thing", "after").show()


# ## Sorting Columns
# ### Spark
# You can not sort columns using Spark Vanilla API 
# 
# ### Pandas
# Similar to pandas
# http://pandas.pydata.org/pandas-docs/version/0.19/generated/pandas.DataFrame.sort_values.html#pandas.DataFrame.sort_values

# In[18]:


df.cols().sort().show()


# In[19]:


df.cols().sort(reverse = True).show()


# ## Drop columns
# ### Spark 
# * You can not delete multiple colums
# 
# ### Pandas
# * Almost the same as pandas
# https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.drop.html

# In[20]:


df2 = df.cols().drop("num")
df2 = df.cols().drop(["num","words"])
df2.show()


# ## Chaining
# 
# cols y rows functions are used to organize and encapsulate optimus' functionality apart of Apache Spark Dataframe API. This have a disadvantage at chaining time because we need to user invoke cols or rows in every step.
# 
# At the same time it can be helpfull when you look at the code because every line is self explained.

# In[21]:


df    .cols().rename([('num','number')])    .cols().drop(["number","words"])    .withColumn("new_col_2", lit("spongebob"))    .cols().create("new_col_1", 1)    .cols().sort(reverse= True)    .show()


# ## Split Columns
# ### Spark
# 
# ### Pandas

# In[22]:


df.cols().split("two strings","-", n=3).show()


# In[23]:


df.cols().split("two strings","-", get = 1).show()


# ## Impute
# ### Spark

# In[16]:


df.cols().impute(["num","num 2"], ["out_a","out_B"], strategy="mean")


# In[ ]:


df


# ## Pandas comparision
# Pandas vs Spark
# https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/
