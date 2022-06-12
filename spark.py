#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import SparkSession
# import pyspark.implicits._


spark = SparkSession.builder.master("local").appName("Test spark").getOrCreate()
sc = spark.sparkContext
print("a")
userSchema = StructType().add("name", "string").add("age", "integer")
print("b")
csvDF = spark.readStream.option("sep", ";").option("header", "false").schema(userSchema).csv("C:\\tmp\\text\\file.csv")
print("c")

csvDF.createOrReplaceTempView("age")
totalSalary = spark.sql("select name,sum(age) from age group by name")
query = totalSalary.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()





