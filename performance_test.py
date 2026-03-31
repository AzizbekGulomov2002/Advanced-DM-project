import time
import psycopg2
from pymongo import MongoClient
from pyspark.sql import SparkSession

# ---------------- POSTGRES ----------------
def test_postgres():
    start = time.time()

    conn = psycopg2.connect(
        dbname="company_project",
        user="postgres",
        password="@Azibek1py",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
    SELECT p.name, c.name
    FROM person p
    JOIN works_in w ON p.id = w.person_id
    JOIN company c ON w.company_id = c.id;
    """)

    cur.fetchall()
    conn.close()

    return time.time() - start


# ---------------- MONGO ----------------
def test_mongo():
    start = time.time()

    client = MongoClient("mongodb://localhost:27017/")
    db = client["company_project"]

    list(db.works_in.aggregate([
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
        {"$unwind": "$company"}
    ]))

    return time.time() - start


# ---------------- SPARK ----------------
def test_spark():
    start = time.time()

    spark = SparkSession.builder.appName("PerfTest").getOrCreate()

    client = MongoClient("mongodb://localhost:27017/")
    db = client["company_project"]

    persons = list(db.persons.find({}, {"_id":0}))
    companies = list(db.companies.find({}, {"_id":0}))
    works = list(db.works_in.find({}, {"_id":0}))

    df_persons = spark.createDataFrame(persons)
    df_companies = spark.createDataFrame(companies)
    df_works = spark.createDataFrame(works)

    df_works.join(df_persons, "person_id").join(df_companies, "company_id").collect()

    spark.stop()

    return time.time() - start


# ---------------- RUN ----------------
if __name__ == "__main__":
    pg = test_postgres()
    mg = test_mongo()
    sp = test_spark()

    print("\n--- PERFORMANCE ---")
    print(f"Postgres: {pg:.4f} sec")
    print(f"MongoDB: {mg:.4f} sec")
    print(f"Spark:    {sp:.4f} sec")