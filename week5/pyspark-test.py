import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master('local[*]') \
        .appName('pyspark-test') \
        .getOrCreate()

df = spark.read \
        .option("header", "true") \
        .csv('../week4/seeds/taxi_zone_lookup.csv')

df.show()
df.write.parquet('taxi_zone_lookup')
