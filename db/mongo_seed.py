"""
Slide_ADM(3).pdf — PART 1, document (DDBMS) model.

Loads sample PERSON / COMPANY / WORK_IN-style collections into MongoDB.
Same conceptual data as the relational Part 1 side (postgres.sql), stored as documents.

Used by:
  - Part 1, Slide Tasks 5–8 — Spark jobs reading from Mongo (mongo_df_job, mongo_sql_job).
  - Part 1, Slide Tasks 9–12 — Spark RDD jobs (mongo_rdd_job).
  - services/mongo_service.py — API join-style aggregations (mirror Tasks 5–8 semantics).
"""
from pymongo import MongoClient
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import MONGO_DB, MONGO_URI

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# Clean old data
db.persons.delete_many({})
db.companies.delete_many({})
db.works_in.delete_many({})

persons = [
    {"person_id": 1, "name": "Mark",   "surname": "Zuckerberg"},
    {"person_id": 2, "name": "Pavel",  "surname": "Durov"},
    {"person_id": 3, "name": "Sam",    "surname": "Altman"},
    {"person_id": 4, "name": "Elon",   "surname": "Musk"},
    {"person_id": 5, "name": "Larry",  "surname": "Page"},
    {"person_id": 6, "name": "Sergey", "surname": "Brin"},
]
db.persons.insert_many(persons)

companies = [
    {"company_id": 1, "name": "Google",  "sector": "Tech"},
    {"company_id": 2, "name": "Amazon",  "sector": "E-commerce"},
    {"company_id": 3, "name": "Meta",    "sector": "Social"},
    {"company_id": 4, "name": "Tesla",   "sector": "Automotive"},
    {"company_id": 5, "name": "OpenAI",  "sector": "AI"},
]
db.companies.insert_many(companies)

works_in = [
    {"person_id": 1, "company_id": 3},
    {"person_id": 2, "company_id": 3},
    {"person_id": 3, "company_id": 5},
    {"person_id": 3, "company_id": 1},
    {"person_id": 4, "company_id": 4},
    {"person_id": 5, "company_id": 1},
    {"person_id": 6, "company_id": 1},
]
db.works_in.insert_many(works_in)

print("MongoDB seed successfully inserted")