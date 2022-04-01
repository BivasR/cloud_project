from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
spark = SparkSession \
        .builder \
        .appName("Demo") \
        .master("local[4]") \
        .getOrCreate()
schema_1=StructType([StructField('Date',DateType(),True),
                     StructField('Open',FloatType(),True),
                      StructField('High',FloatType(),True),
                     StructField('Low',FloatType(),True),
                     StructField('Close',FloatType(),True),
                     StructField('Adj Close',IntegerType(),True),
                     StructField('volume',IntegerType(),True)])
raw_df = spark.readStream \
        .format("csv") \
        .schema(schema_1)\
        .option("header",True)\
        .option("maxFilesPerTrigger", 1) \
        .load(r"./data/10/*.csv")
print(raw_df.isStreaming)
print(raw_df.printSchema())
avg_y=raw_df.groupBy("Date").agg((avg("High").alias("HIGH_1"))).sort(desc("HIGH_1"))
query=avg_y.writeStream.format("console").outputMode("complete").start()

query.awaitTermination()
