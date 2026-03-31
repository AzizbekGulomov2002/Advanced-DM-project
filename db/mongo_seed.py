from pymongo import MongoClient

# Connect
client = MongoClient("mongodb://localhost:27017/")
db = client["company_project"]

# Clean old datas
db.persons.delete_many({})
db.companies.delete_many({})
db.works_in.delete_many({})

# Create persons DML
persons = [
    {"person_id":1, "name":"Mark","surname":"Zuck"},
    {"person_id":2, "name":"Pavel","surname":"Durov"},
    {"person_id":3, "name":"Sam","surname":"Altman"},
    {"person_id":4, "name":"Elon","surname":"Musk"}
]
db.persons.insert_many(persons)

# Create companies DML
companies = [
    {"company_id":1, "name":"Google"},
    {"company_id":2, "name":"Amazon"},
    {"company_id":3, "name":"Meta"},
    {"company_id":4, "name":"Tesla"},
]
db.companies.insert_many(companies)

# Relation companies to persons
works = [
    {"person_id": 1, "company_id": 3},
    {"person_id": 2, "company_id": 3},
    {"person_id": 3, "company_id": 1},
    {"person_id": 4, "company_id": 4},
]
db.works.insert_many(works)
print("MongoDB seed successfully")

