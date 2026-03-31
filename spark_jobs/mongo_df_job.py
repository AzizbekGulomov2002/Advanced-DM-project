from pyspark.sql import SparkSession
from pymongo import MongoClient

# Spark start
spark = SparkSession.builder.appName("Mongo Spark DF").getOrCreate()

# Mongo connect
client = MongoClient("mongodb://localhost:27017/")
db = client["company_project"]

# load data
persons = list(db.persons.find({}, {"_id":0}))
companies = list(db.companies.find({}, {"_id":0}))
works = list(db.works_in.find({}, {"_id":0}))

# convert to DataFrame
df_persons = spark.createDataFrame(persons)
df_companies = spark.createDataFrame(companies)
df_works = spark.createDataFrame(works)

# JOIN logic
result = df_works \
    .join(df_persons, "person_id") \
    .join(df_companies, "company_id")

result.show()