from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DF Example").getOrCreate()

df = spark.read.json("data/persons.json")

df.groupBy("company").count().show()