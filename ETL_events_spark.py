#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession.builder.appName("EventsSplit").getOrCreate()

userSchema = StructType().add("timestamp", "string").add("visitorid", "string").add("event", "string").add("itemid", "string").add("transactionid", "string")


dfCSV = spark.readStream.option("sep", ",").option("header", "false").schema(userSchema).csv("file:///C:/tmp/text")

dfCSV.createOrReplaceTempView("events")

transaction = spark.sql("select * from events where event='transaction'")
events = spark.sql("select * from events where event<>'transaction'")

query1 = transaction.writeStream.format("console").start()
query2 = events.writeStream.format("console").start()

query1.awaitTermination()
query2.awaitTermination()




