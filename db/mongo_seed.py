from pymongo import MongoClient
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import MONGO_DB, MONGO_URI

# Connect
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

# Clean old data (IMPORTANT)
db.persons.delete_many({})
db.companies.delete_many({})
db.works_in.delete_many({})

# Persons
persons = [
    {"person_id": 1, "name": "Mark", "surname": "Zuck"},
    {"person_id": 2, "name": "Pavel", "surname": "Durov"},
    {"person_id": 3, "name": "Sam", "surname": "Altman"},
    {"person_id": 4, "name": "Elon", "surname": "Musk"}
]
db.persons.insert_many(persons)

# Companies
companies = [
    {"company_id": 1, "name": "Google"},
    {"company_id": 2, "name": "Amazon"},
    {"company_id": 3, "name": "Meta"},
    {"company_id": 4, "name": "Tesla"}
]
db.companies.insert_many(companies)

# Works_in (RELATION COLLECTION)
works_in = [
    {"person_id": 1, "company_id": 3},
    {"person_id": 2, "company_id": 3},
    {"person_id": 3, "company_id": 1},
    {"person_id": 4, "company_id": 4}
]
db.works_in.insert_many(works_in)

print("✅ MongoDB seed successfully inserted")