from pyspark import SparkContext
import json

sc = SparkContext("local", "RDD Example")

# load data
data = sc.textFile("data/persons.json")

# parse JSON
rdd = data.map(lambda x: json.loads(x))

# extract (company, 1)
mapped = rdd.map(lambda x: (x["company"], 1))

# reduce
result = mapped.reduceByKey(lambda a, b: a + b)

print(result.collect())