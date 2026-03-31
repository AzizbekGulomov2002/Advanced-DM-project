import psycopg2
from fastapi import FastAPI
from neo4j import GraphDatabase
from config.settings import (
    NEO4J_PASSWORD,
    NEO4J_URI,
    NEO4J_USERNAME,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from services.mongo_service import (
    get_person_company,
    mongo_full_join,
    mongo_inner_join,
    mongo_left_join,
    mongo_right_join
)
app = FastAPI()
# Postgres DB APIS



def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

@app.get("/postgres/join")
def postgres_join():
    conn = get_pg_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT p.name, c.name
        FROM person p
        JOIN works_in w ON p.id = w.person_id
        JOIN company c ON w.company_id = c.id
    """)

    result = cur.fetchall()
    conn.close()

    return result

# Mongo DB APIS


@app.get("/mongo/join")
def mongo_join():
    return get_person_company()

@app.get("/mongo/inner")
def mongo_inner():
    return mongo_inner_join()

@app.get("/mongo/full")
def mongo_inner():
    return mongo_full_join()

@app.get("/mongo/right")
def mongo_right():
    return mongo_right_join()

@app.get("/mongo/left")
def mongo_left():
    return mongo_left_join()

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
)

@app.get("/neo4j/follows")
def neo4j_query():
    with driver.session() as session:
        result = session.run("""
            MATCH (a:User {name:"Mark"})-[:FOLLOWS*]->(u)
            RETURN u.name AS name
        """)
        return [r["name"] for r in result]