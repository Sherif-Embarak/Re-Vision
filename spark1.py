print("aaa")
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

#%%
## SCRIPT
print("aa")
# Instantiate the new spark session
spark = SparkSession.builder.appName("StreamingDemo").getOrCreate()
print("a")
# * Call stream with sending messages to local host with standard port "9999"
# --> loads socket
# NOTE on hosthame: use "localhost" being the standard term for working locally on your private computer ("local" apparently doesn't work correctly)
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Create dataframe
# NOTE on several methods employed here (hover over them for docs)
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Create word counter
# NOTE: store in a new dataframe
wordCounts = words.groupBy("word").count()

# * Print results
print(wordCounts.printSchema())
