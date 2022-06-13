#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


# In[2]:


spark = SparkSession.builder.appName("EventsSplit").getOrCreate()


# In[3]:


userSchema = StructType().add("timestamp", "string").add("visitorid", "string").add("event", "string").add("itemid", "string").add("transactionid", "string")


# In[4]:


dfCSV = spark.readStream.option("sep", ",").option("header", "false").schema(userSchema).csv("file:///C:/tmp/text")


# In[5]:


dfCSV.createOrReplaceTempView("events")



totalSalary = spark.sql("select * from events where event='transaction'")


query = totalSalary.writeStream.format("console").start()

query.awaitTermination()




