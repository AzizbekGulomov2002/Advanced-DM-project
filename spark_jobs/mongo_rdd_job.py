"""
Slide_ADM(3).pdf — Part 1, Tasks 9–12: same join shapes as Tasks 5–8 but implemented with
Spark RDD operations (restricted RDD API on the slide), reading from MongoDB.
"""
from pyspark.sql import SparkSession
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
spark = SparkSession.builder.appName("Mongo RDD").getOrCreate()
sc = spark.sparkContext

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
# Convert to RDD
# ----------------------------------------
rdd_persons = sc.parallelize(persons)
rdd_companies = sc.parallelize(companies)
rdd_works = sc.parallelize(works)

# ----------------------------------------
# Build lookup dictionaries (collected once)
# ----------------------------------------
p_dict = dict(rdd_persons.map(lambda x: (x["person_id"], x["name"])).collect())
c_dict = dict(rdd_companies.map(lambda x: (x["company_id"], x["name"])).collect())

# works as (person_id, company_id)
emp = rdd_works.map(lambda x: (x["person_id"], x["company_id"]))

# ----------------------------------------
# Task 9 — INNER JOIN
# ----------------------------------------
inner = emp.map(
    lambda x: {
        "person": p_dict.get(x[0]),
        "company": c_dict.get(x[1]),
    }
).filter(lambda x: x["person"] is not None and x["company"] is not None)

print("\n===== Task 9: INNER JOIN (RDD) =====")
for row in inner.collect():
    print(row)

# ----------------------------------------
# Task 10 — LEFT JOIN (all persons; multiple companies => multiple rows)
# groupByKey keeps all employments per person (no dict overwrite)
# ----------------------------------------
emp_by_person = (
    emp.map(lambda x: (x[0], c_dict.get(x[1])))
    .groupByKey()
    .map(lambda x: (x[0], list(x[1])))
)
emp_by_person_dict = dict(emp_by_person.collect())

left = rdd_persons.flatMap(
    lambda x: (
        [{"person": x["name"], "company": c} for c in emp_by_person_dict[x["person_id"]]]
        if x["person_id"] in emp_by_person_dict
        else [{"person": x["name"], "company": None}]
    )
)

print("\n===== Task 10: LEFT JOIN (RDD, multi-company safe) =====")
for row in left.collect():
    print(row)

# ----------------------------------------
# Task 11 — RIGHT JOIN (all companies, persons if any)
# ----------------------------------------
company_to_persons = (
    emp.map(lambda x: (x[1], p_dict.get(x[0])))
    .groupByKey()
    .map(lambda x: (x[0], list(x[1])))
)
company_to_persons_dict = dict(company_to_persons.collect())

right = rdd_companies.flatMap(
    lambda x: (
        [{"company": x["name"], "person": p} for p in company_to_persons_dict[x["company_id"]]]
        if x["company_id"] in company_to_persons_dict
        else [{"company": x["name"], "person": None}]
    )
)

print("\n===== Task 11: RIGHT JOIN (RDD) =====")
for row in right.collect():
    print(row)

# ----------------------------------------
# Task 12 — FULL OUTER JOIN
# Rows from left (person, company) + companies with zero employees
# ----------------------------------------
employed_company_ids = set(emp.map(lambda x: x[1]).distinct().collect())

left_for_full = left.map(lambda x: (x["person"], x["company"]))

orphan_companies = rdd_companies.filter(
    lambda x: x["company_id"] not in employed_company_ids
).map(lambda x: (None, x["name"]))

full = left_for_full.union(orphan_companies)

print("\n===== Task 12: FULL OUTER JOIN (RDD) =====")
for row in full.collect():
    print({"person": row[0], "company": row[1]})

spark.stop()
