from pyspark.sql import SparkSession
from pymongo import MongoClient
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import MONGO_DB, MONGO_URI

# Spark
spark = SparkSession.builder.appName("Mongo Spark SQL").getOrCreate()

# Mongo
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# Load data
persons = list(db.persons.find({}, {"_id":0}))
companies = list(db.companies.find({}, {"_id":0}))
works = list(db.works_in.find({}, {"_id":0}))

# DataFrames
df_persons = spark.createDataFrame(persons)
df_companies = spark.createDataFrame(companies)
df_works = spark.createDataFrame(works)

# Register as tables
df_persons.createOrReplaceTempView("persons")
df_companies.createOrReplaceTempView("companies")
df_works.createOrReplaceTempView("works")

# SQL JOIN
result = spark.sql("""
SELECT p.name AS person, c.name AS company
FROM works w
JOIN persons p ON w.person_id = p.person_id
JOIN companies c ON w.company_id = c.company_id
""")

result.show()