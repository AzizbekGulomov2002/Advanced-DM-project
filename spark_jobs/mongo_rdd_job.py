from pyspark.sql import SparkSession
from pymongo import MongoClient

# Spark start
spark = SparkSession.builder.appName("Mongo RDD").getOrCreate()
sc = spark.sparkContext

# Mongo connect
client = MongoClient("mongodb://localhost:27017/")
db = client["company_project"]

# Load data
persons = list(db.persons.find({}, {"_id":0}))
companies = list(db.companies.find({}, {"_id":0}))
works = list(db.works_in.find({}, {"_id":0}))

# Convert to RDD
rdd_persons = sc.parallelize(persons)
rdd_companies = sc.parallelize(companies)
rdd_works = sc.parallelize(works)

# Map step (key-value)
persons_kv = rdd_persons.map(lambda x: (x["person_id"], x["name"]))
companies_kv = rdd_companies.map(lambda x: (x["company_id"], x["name"]))
works_kv = rdd_works.map(lambda x: (x["person_id"], x["company_id"]))

# JOIN step 1 (person ↔ works)
join1 = works_kv.join(persons_kv)
# (person_id, (company_id, person_name))

# Prepare for next join
step2 = join1.map(lambda x: (x[1][0], x[1][1]))
# (company_id, person_name)

# JOIN step 2 (company ↔ person)
final = step2.join(companies_kv)
# (company_id, (person_name, company_name))

# Final format
result = final.map(lambda x: (x[1][0], x[1][1]))

print(result.collect())