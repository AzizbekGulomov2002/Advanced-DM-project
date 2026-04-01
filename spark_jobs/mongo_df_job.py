"""
Slide_ADM(3).pdf — Part 1, Tasks 5–8: Spark DataFrame API joins (inner, left, right, full)
on MongoDB collections (persons, companies, works_in). Sister script: mongo_sql_job.py
(same logical joins expressed in SparkSQL).
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pymongo import MongoClient
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import MONGO_DB, MONGO_URI

# ----------------------------------------
# Spark start
# ----------------------------------------
spark = SparkSession.builder.appName("Mongo DF Jobs").getOrCreate()

# ----------------------------------------
# MongoDB connect
# ----------------------------------------
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# ----------------------------------------
# Load data from MongoDB
# ----------------------------------------
persons = list(db.persons.find({}, {"_id": 0}))
companies = list(db.companies.find({}, {"_id": 0}))
works = list(db.works_in.find({}, {"_id": 0}))

# ----------------------------------------
# DataFrames with clear column names (avoid "name" clash)
# ----------------------------------------
df_persons = spark.createDataFrame(persons).withColumnRenamed("name", "person_name")
df_companies = spark.createDataFrame(companies).withColumnRenamed("name", "company_name")
df_works = spark.createDataFrame(works)

# ----------------------------------------
# Task 5 — INNER JOIN (employed persons + company)
# ----------------------------------------
inner = (
    df_works.join(df_persons, "person_id", "inner")
    .join(df_companies, "company_id", "inner")
    .select("person_name", "surname", "company_name")
)

print("\n===== Task 5: INNER JOIN (employed persons) =====")
inner.show()

# ----------------------------------------
# Task 6 — LEFT JOIN (all persons, company if any)
# ----------------------------------------
left = (
    df_persons.join(df_works, "person_id", "left")
    .join(df_companies, "company_id", "left")
    .select("person_name", "surname", "company_name")
)

print("\n===== Task 6: LEFT JOIN (all persons) =====")
left.show()

# ----------------------------------------
# Task 7 — RIGHT JOIN (all companies, persons if any)
# Base: companies, not persons.
# ----------------------------------------
right = (
    df_companies.join(df_works, "company_id", "left")
    .join(df_persons, "person_id", "left")
    .select("person_name", "surname", "company_name")
)

print("\n===== Task 7: RIGHT JOIN (all companies) =====")
right.show()

# ----------------------------------------
# Task 8 — FULL semantics aligned with SQL CTE (employed rows + orphan companies)
# Not chained FULL OUTER joins (avoids edge cases with null company_id on second join).
# ----------------------------------------
employed_side = (
    df_persons.join(df_works, "person_id", "left")
    .join(df_companies, "company_id", "left")
    .select("person_name", "surname", "company_name")
)

used_company_ids = df_works.select("company_id").distinct()
orphan_companies = df_companies.join(used_company_ids, "company_id", "left_anti")
orphan_side = orphan_companies.select(
    lit(None).cast("string").alias("person_name"),
    lit(None).cast("string").alias("surname"),
    col("company_name"),
)

full = employed_side.unionByName(orphan_side)

print("\n===== Task 8: FULL (same idea as SQL WITH + UNION ALL) =====")
full.show()

spark.stop()
