from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client["company_project"]
def get_person_company():
    result = db.works_in.insert_many([
        {
            "$lookup":{
                "from":"persons",
                "localField":"person_id",
                "foreignField":"person_id",
                "as":"person"
            }
        },
        {"$unwind":"$person"},
        {
            "$lookup":{
                "from":"companies",
                "localField":"company_id",
                "foreignField":"company_id",
                "as":"comapny"
            }
        },
        {"$unwind":"$company"},
        {
            "$project":{
                "_id":0,
                "person":"$person.name",
                "company":"$company.name"
            }
        }
    ])
    return list(result)


# checking query : db.persons.find()
# db.companies.find()
# db.works_in.find()

def mongo_inner_join():
    return list(db.works_in.aggregate([
        {
            "$lookup": {
                "from": "persons",
                "localField": "person_id",
                "foreignField": "person_id",
                "as": "person"
            }
        },
        {"$unwind": "$person"},
        {
            "$lookup": {
                "from": "companies",
                "localField": "company_id",
                "foreignField": "company_id",
                "as": "company"
            }
        },
        {"$unwind": "$company"},
        {
            "$project": {
                "_id": 0,
                "person": "$person.name",
                "company": "$company.name"
            }
        }
    ]))

def mongo_left_join():
    return list(db.persons.aggregate([
        {
            "$lookup":{
                "from":"works_in",
                "localField":"person_id",
                "foreignField":"person_id",
                "as":"work"
            }
        },
        {
            "$lookup":{
                "from":"companies",
                "localField":"company_id",
                "foreignField":"company_id",
                "as":"company"
            }
        },
        {
            "$project":{
                "_id":0,
                "person":"$name",
                "company":{
                    "$ifNull":[{"$arrayElemAt":["$company.name",0]}, None]
                }
            }
        }
    ]))

def mongo_right_join():
    return list(db.companies.aggregate([
        {
            "$lookup":{
                "from":"works_in",
                "localField":"company_id",
                "foreignField":"company_id",
                "as":"work"
            }
        },
        {
            "$lookup":{
                "from":"persons",
                "localField":"work.person_id",
                "foreignField":"person_id",
                "as":"person"
            }
        },
        {
            "$project":{
                "_id":0,
                "company":"$name",
                "person":{
                    "$ifNull":[{"$arrayElemAt": ["$person.name",0]}, None]
                }
            }
        }
    ]))
    

def mongo_full_join():
    left = mongo_left_join()
    right = mongo_right_join()
    return left+right