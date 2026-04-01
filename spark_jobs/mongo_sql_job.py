"""
Slide_ADM(3).pdf — Part 1, Tasks 5–8 (SparkSQL variant): same join semantics as mongo_df_job.py.

Why a second file? The assignment allows DataFrame API and SparkSQL (including both styles).
This script mirrors mongo_df_job.py in SQL strings for the report / oral comparison.
"""
from pyspark.sql import SparkSession
from pymongo import MongoClient
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import MONGO_DB, MONGO_URI

spark = SparkSession.builder.appName("Mongo Spark SQL").getOrCreate()

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

persons = list(db.persons.find({}, {"_id": 0}))
companies = list(db.companies.find({}, {"_id": 0}))
works = list(db.works_in.find({}, {"_id": 0}))

df_persons = spark.createDataFrame(persons)
df_companies = spark.createDataFrame(companies)
df_works = spark.createDataFrame(works)

df_persons.createOrReplaceTempView("persons")
df_companies.createOrReplaceTempView("companies")
df_works.createOrReplaceTempView("works")

# Task 5 — INNER (SparkSQL)
print("\n===== SparkSQL Task 5: INNER JOIN =====")
spark.sql("""
SELECT p.name AS person_name, p.surname, c.name AS company_name
FROM works w
JOIN persons p ON w.person_id = p.person_id
JOIN companies c ON w.company_id = c.company_id
""").show()

# Task 6 — LEFT (all persons)
print("\n===== SparkSQL Task 6: LEFT JOIN =====")
spark.sql("""
SELECT p.name AS person_name, p.surname, c.name AS company_name
FROM persons p
LEFT JOIN works w ON p.person_id = w.person_id
LEFT JOIN companies c ON w.company_id = c.company_id
""").show()

# Task 7 — RIGHT semantics: all companies + persons if any
print("\n===== SparkSQL Task 7: RIGHT JOIN (company-based) =====")
spark.sql("""
SELECT p.name AS person_name, p.surname, c.name AS company_name
FROM companies c
LEFT JOIN works w ON c.company_id = w.company_id
LEFT JOIN persons p ON w.person_id = p.person_id
""").show()

# Task 8 — FULL semantics (aligned with SQL CTE: employed rows + orphan companies)
print("\n===== SparkSQL Task 8: FULL (UNION ALL) =====")
spark.sql("""
SELECT p.name AS person_name, p.surname, c.name AS company_name
FROM persons p
LEFT JOIN works w ON p.person_id = w.person_id
LEFT JOIN companies c ON w.company_id = c.company_id
UNION ALL
SELECT CAST(NULL AS STRING) AS person_name, CAST(NULL AS STRING) AS surname, c.name AS company_name
FROM companies c
LEFT JOIN works w ON c.company_id = w.company_id
WHERE w.person_id IS NULL
""").show()

spark.stop()
