

from optimus import *

from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType

sc = SparkSession.builder.getOrCreate()


# In[3]:


# Create optimus
op = Optimus(sc)


# In[4]:


csv =op.load.url("https://raw.githubusercontent.com/ironmussa/Optimus/master/examples/foo.csv")
csv.show()


# In[5]:


json =op.load.url("https://api.github.com/users/mralexgray/repos", "json")
json.show()


# In[6]:


op.save.csv(csv, "test.csv")


# In[7]:


op.save.json(json, "test.json")


# In[8]:


op.save.parquet(csv, "test.parquet")

