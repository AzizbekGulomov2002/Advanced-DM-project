from pyspark.sql import SparkSession
from pymongo import MongoClient
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import MONGO_DB, MONGO_URI

# Spark start
spark = SparkSession.builder.appName("Mongo Spark DF").getOrCreate()

# Mongo connect
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

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