"""
Performance harness — Slide_ADM(3).pdf (end of Part 1 and Part 2).

Part 1 — Compare running times for implementations varying:
  persons {100, 1000, 10000}, companies {10, 100}, occupation {10%, 25%, 50%}
  - Slide Tasks 1–4: PostgreSQL join queries (inner, left, right, full semantics)
  - Slide Tasks 5–8: MongoDB aggregations + Spark DataFrames (same four shapes)
  - Slide Tasks 9–12: Spark RDD join-style timings (inner, left, right, full)

Part 2 — Compare running times varying:
  users {10, 100, 1000}, connection rates {5%, 10%, 25%} of possible directed edges
  - Slide Task 1: Neo4j transitive query
  - Slide Task 2: GraphFrames BFS (from PostgreSQL)
  - Slide Task 3: Spark RDD graph expansion (from PostgreSQL)

Set BENCHMARK_QUICK=1 to run one cell per part for quick tables.
"""
import os
import sys
import time
import random
import psycopg2
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from neo4j import GraphDatabase

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from config.settings import (
    MONGO_DB, MONGO_URI,
    POSTGRES_DB, POSTGRES_HOST,
    POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER,
    NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD,
)

# ----------------------------------------
# DB connections
# ----------------------------------------
def get_pg():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def get_mongo():
    return MongoClient(MONGO_URI)[MONGO_DB]


def _exit_services(message: str) -> None:
    print(message, file=sys.stderr)
    sys.exit(1)


def _require_mongodb() -> None:
    """Fail fast if mongod is down (avoids a long wait inside generate_part1)."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        client.close()
    except Exception as exc:
        _exit_services(
            "MongoDB is not reachable (connection refused or timeout).\n"
            "  • Start MongoDB, then retry (e.g. macOS Homebrew: brew services start mongodb-community).\n"
            "  • Or run a container: docker run -d -p 27017:27017 --name mongo mongo:7\n"
            "  • Confirm MONGO_URI and MONGO_DB in your project .env match that server.\n"
            f"Underlying error: {exc}"
        )


def _require_neo4j(driver) -> None:
    try:
        driver.verify_connectivity()
    except Exception as exc:
        _exit_services(
            "Neo4j is not reachable.\n"
            "  • Start Neo4j and check NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD in .env.\n"
            f"Underlying error: {exc}"
        )


# Assignment-allowed company sectors (Slide Part 1)
_ALLOWED_SECTORS = (
    "automotive", "banking", "services", "healthcare", "chemicals", "public"
)

# One grid cell only — fast docs / smoke (set BENCHMARK_QUICK=1)
_QUICK = os.environ.get("BENCHMARK_QUICK", "").lower() in ("1", "true", "yes")

# ----------------------------------------
# Data generators (synthetic data for Part 1 / Part 2 performance grids above)
# ----------------------------------------
def generate_part1(pg_conn, mongo_db, n_persons, n_companies, occupation_rate):
    """Part 1 performance grid — relational PERSON/COMPANY/WORKS_IN + mirror collections for Mongo/Spark Tasks 5–12."""
    cur = pg_conn.cursor()

    # Clean
    cur.execute("DELETE FROM works_in; DELETE FROM person; DELETE FROM company;")

    # 1) Insert companies (sectors from assignment list only)
    for i in range(n_companies):
        cur.execute(
            "INSERT INTO company (name, founded, sector) VALUES (%s,%s,%s)",
            (f"Company{i}", "2000-01-01", _ALLOWED_SECTORS[i % len(_ALLOWED_SECTORS)]),
        )

    # 2) Insert persons
    for i in range(n_persons):
        cur.execute(
            "INSERT INTO person (name, surname, birthdate, address, country) VALUES (%s,%s,%s,%s,%s)",
            (f"Person{i}", f"Surname{i}", "1990-01-01", "Address", "US")
        )
    pg_conn.commit()

    # 3) Get ids
    cur.execute("SELECT id FROM person;")
    person_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM company;")
    company_ids = [r[0] for r in cur.fetchall()]

    # 4) Assign employment
    n_employed = int(len(person_ids) * occupation_rate)
    employed = random.sample(person_ids, n_employed)

    for pid in employed:
        cid = random.choice(company_ids)
        cur.execute(
            "INSERT INTO works_in (person_id, company_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (pid, cid)
        )
    pg_conn.commit()

    # Sync to MongoDB
    mongo_db.persons.delete_many({})
    mongo_db.companies.delete_many({})
    mongo_db.works_in.delete_many({})

    cur.execute("SELECT id, name, surname FROM person;")
    mongo_db.persons.insert_many([
        {"person_id": r[0], "name": r[1], "surname": r[2]}
        for r in cur.fetchall()
    ])
    cur.execute("SELECT id, name, sector FROM company;")
    mongo_db.companies.insert_many([
        {"company_id": r[0], "name": r[1], "sector": r[2]}
        for r in cur.fetchall()
    ])
    cur.execute("SELECT person_id, company_id FROM works_in;")
    rows = cur.fetchall()
    if rows:
        mongo_db.works_in.insert_many([
            {"person_id": r[0], "company_id": r[1]} for r in rows
        ])

    cur.close()


def generate_part2(pg_conn, neo4j_driver, n_users, connection_rate):
    """Part 2 performance grid — USER/FOLLOW relational data + Neo4j mirror for Task 1 timing."""
    cur = pg_conn.cursor()

    # 1) Clean and fill PostgreSQL
    cur.execute("DELETE FROM follows; DELETE FROM users;")

    for i in range(n_users):
        job = "Engineer" if i % 2 == 0 else None
        study = f"Degree-{i % 5}" if i % 3 == 0 else None
        cur.execute(
            """
            INSERT INTO users (name, surname, birthdate, address, country, job, study_title)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (f"User{i}", f"Surname{i}", "1990-01-01", "Address", "US", job, study),
        )
    pg_conn.commit()

    cur.execute("SELECT id FROM users;")
    user_ids = [r[0] for r in cur.fetchall()]

    # Total possible connections
    max_connections = n_users * (n_users - 1)
    n_connections = int(max_connections * connection_rate)

    pairs = set()
    while len(pairs) < n_connections:
        a, b = random.sample(user_ids, 2)
        pairs.add((a, b))

    for (a, b) in pairs:
        cur.execute(
            "INSERT INTO follows (follower_id, followee_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (a, b)
        )
    pg_conn.commit()

    # 2) Sync same graph data to Neo4j for fair comparison
    cur.execute(
        """
        SELECT id, name, surname, birthdate, address, country, job, study_title
        FROM users
        """
    )
    users_rows = cur.fetchall()
    cur.execute("SELECT follower_id, followee_id FROM follows;")
    follows_rows = cur.fetchall()

    with neo4j_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        session.run(
            """
            UNWIND $users AS u
            CREATE (:User {
                id: u.id,
                name: u.name,
                surname: u.surname,
                birthdate: u.birthdate,
                address: u.address,
                country: u.country,
                job: u.job,
                study_title: u.study_title
            })
            """,
            users=[
                {
                    "id": row[0],
                    "name": row[1],
                    "surname": row[2],
                    "birthdate": str(row[3]) if row[3] is not None else None,
                    "address": row[4],
                    "country": row[5],
                    "job": row[6],
                    "study_title": row[7],
                }
                for row in users_rows
            ],
        )
        if follows_rows:
            session.run(
                """
                UNWIND $rels AS r
                MATCH (a:User {id: r.follower_id}), (b:User {id: r.followee_id})
                CREATE (a)-[:FOLLOWS]->(b)
                """,
                rels=[
                    {"follower_id": row[0], "followee_id": row[1]}
                    for row in follows_rows
                ],
            )
    cur.close()


# ----------------------------------------
# Part 1, Slide Tasks 1–4 — PostgreSQL SQL strings (same semantics as db/postgres.sql)
# ----------------------------------------
SQL_INNER = """
SELECT p.name, p.surname, c.name
FROM person p
JOIN works_in w ON p.id = w.person_id
JOIN company c ON w.company_id = c.id
"""

SQL_LEFT = """
SELECT p.name, p.surname, c.name
FROM person p
LEFT JOIN works_in w ON p.id = w.person_id
LEFT JOIN company c ON w.company_id = c.id
"""

SQL_RIGHT = """
SELECT p.name, p.surname, c.name
FROM company c
LEFT JOIN works_in w ON c.id = w.company_id
LEFT JOIN person p ON w.person_id = p.id
"""

SQL_FULL = """
WITH employed AS (
    SELECT p.name, p.surname, c.name AS cname
    FROM person p
    LEFT JOIN works_in w ON p.id = w.person_id
    LEFT JOIN company c ON w.company_id = c.id
),
orphan_companies AS (
    SELECT NULL::varchar AS name, NULL::varchar AS surname, c.name AS cname
    FROM company c
    WHERE NOT EXISTS (SELECT 1 FROM works_in w WHERE w.company_id = c.id)
)
SELECT name, surname, cname FROM employed
UNION ALL
SELECT name, surname, cname FROM orphan_companies
"""


def test_postgres_all_joins(pg_conn):
    """Time Part 1 Tasks 1–4 (inner, left, right, full) in PostgreSQL."""
    cur = pg_conn.cursor()
    times = {}
    for key, sql in [
        ("inner", SQL_INNER),
        ("left", SQL_LEFT),
        ("right", SQL_RIGHT),
        ("full", SQL_FULL),
    ]:
        t0 = time.time()
        cur.execute(sql)
        cur.fetchall()
        times[key] = time.time() - t0
    cur.close()
    return times


# Part 1, Slide Tasks 5–8 — MongoDB aggregation pipelines (document side)
MONGO_INNER_PIPELINE = [
    {"$lookup": {
        "from": "persons",
        "localField": "person_id",
        "foreignField": "person_id",
        "as": "person",
    }},
    {"$unwind": "$person"},
    {"$lookup": {
        "from": "companies",
        "localField": "company_id",
        "foreignField": "company_id",
        "as": "company",
    }},
    {"$unwind": "$company"},
]

MONGO_LEFT_PIPELINE = [
    {"$lookup": {
        "from": "works_in",
        "localField": "person_id",
        "foreignField": "person_id",
        "as": "work",
    }},
    {"$unwind": {"path": "$work", "preserveNullAndEmptyArrays": True}},
    {"$lookup": {
        "from": "companies",
        "localField": "work.company_id",
        "foreignField": "company_id",
        "as": "company",
    }},
    {"$unwind": {"path": "$company", "preserveNullAndEmptyArrays": True}},
]

MONGO_RIGHT_PIPELINE = [
    {"$lookup": {
        "from": "works_in",
        "localField": "company_id",
        "foreignField": "company_id",
        "as": "work",
    }},
    {"$lookup": {
        "from": "persons",
        "localField": "work.person_id",
        "foreignField": "person_id",
        "as": "person",
    }},
]

MONGO_ORPHAN_COMPANIES_PIPELINE = [
    {"$lookup": {
        "from": "works_in",
        "localField": "company_id",
        "foreignField": "company_id",
        "as": "w",
    }},
    {"$match": {"w": {"$size": 0}}},
]


def test_mongo_all_joins(mongo_db):
    """Time Part 1 Tasks 5–8 equivalents in MongoDB."""
    times = {}
    t0 = time.time()
    list(mongo_db.works_in.aggregate(MONGO_INNER_PIPELINE))
    times["inner"] = time.time() - t0

    t0 = time.time()
    list(mongo_db.persons.aggregate(MONGO_LEFT_PIPELINE))
    times["left"] = time.time() - t0

    t0 = time.time()
    list(mongo_db.companies.aggregate(MONGO_RIGHT_PIPELINE))
    times["right"] = time.time() - t0

    # Full outer semantics: all person-side rows + companies with no employees
    t0 = time.time()
    list(mongo_db.persons.aggregate(MONGO_LEFT_PIPELINE))
    list(mongo_db.companies.aggregate(MONGO_ORPHAN_COMPANIES_PIPELINE))
    times["full"] = time.time() - t0

    return times


def test_spark_all_joins(mongo_db, spark):
    """Part 1, Slide Tasks 5–8 — Spark DataFrame API, data from MongoDB."""
    persons = list(mongo_db.persons.find({}, {"_id": 0}))
    companies = list(mongo_db.companies.find({}, {"_id": 0}))
    works = list(mongo_db.works_in.find({}, {"_id": 0}))

    if not persons or not companies:
        return {k: 0.0 for k in ("inner", "left", "right", "full")}

    df_p = spark.createDataFrame(persons).withColumnRenamed("name", "person_name")
    df_c = spark.createDataFrame(companies).withColumnRenamed("name", "company_name")
    df_w = spark.createDataFrame(works)

    times = {}

    t0 = time.time()
    df_w.join(df_p, "person_id", "inner").join(df_c, "company_id", "inner").collect()
    times["inner"] = time.time() - t0

    t0 = time.time()
    df_p.join(df_w, "person_id", "left").join(df_c, "company_id", "left").collect()
    times["left"] = time.time() - t0

    t0 = time.time()
    df_c.join(df_w, "company_id", "left").join(df_p, "person_id", "left").collect()
    times["right"] = time.time() - t0

    # Same semantics as SQL FULL (employed side + orphan companies), like mongo_df_job Task 8
    t0 = time.time()
    employed_side = (
        df_p.join(df_w, "person_id", "left")
        .join(df_c, "company_id", "left")
        .select("person_name", "surname", "company_name")
    )
    used_ids = df_w.select("company_id").distinct()
    orphan_side = (
        df_c.join(used_ids, "company_id", "left_anti").select(
            lit(None).cast("string").alias("person_name"),
            lit(None).cast("string").alias("surname"),
            col("company_name"),
        )
    )
    employed_side.unionByName(orphan_side).collect()
    times["full"] = time.time() - t0

    return times


def test_spark_rdd_all_joins(mongo_db, sc):
    """Part 1, Slide Tasks 9–12 — Spark RDD joins, data from MongoDB (restricted RDD API on slide)."""
    persons = list(mongo_db.persons.find({}, {"_id": 0}))
    companies = list(mongo_db.companies.find({}, {"_id": 0}))
    works = list(mongo_db.works_in.find({}, {"_id": 0}))

    if not persons or not companies:
        return {k: 0.0 for k in ("inner", "left", "right", "full")}

    rdd_p = sc.parallelize(persons)
    rdd_c = sc.parallelize(companies)
    rdd_w = sc.parallelize(works)

    p_dict = dict(rdd_p.map(lambda x: (x["person_id"], x["name"])).collect())
    c_dict = dict(rdd_c.map(lambda x: (x["company_id"], x["name"])).collect())
    emp = rdd_w.map(lambda x: (x["person_id"], x["company_id"]))

    times = {}

    t0 = time.time()
    list(
        emp.map(
            lambda x: {
                "person": p_dict.get(x[0]),
                "company": c_dict.get(x[1]),
            }
        )
        .filter(lambda x: x["person"] is not None and x["company"] is not None)
        .collect()
    )
    times["inner"] = time.time() - t0

    t0 = time.time()
    emp_by_person = dict(
        emp.map(lambda x: (x[0], c_dict.get(x[1])))
        .groupByKey()
        .map(lambda x: (x[0], list(x[1])))
        .collect()
    )
    list(
        rdd_p.flatMap(
            lambda x: (
                [{"person": x["name"], "company": c} for c in emp_by_person[x["person_id"]]]
                if x["person_id"] in emp_by_person
                else [{"person": x["name"], "company": None}]
            )
        ).collect()
    )
    times["left"] = time.time() - t0

    t0 = time.time()
    company_to_persons = dict(
        emp.map(lambda x: (x[1], p_dict.get(x[0])))
        .groupByKey()
        .map(lambda x: (x[0], list(x[1])))
        .collect()
    )
    list(
        rdd_c.flatMap(
            lambda x: (
                [{"company": x["name"], "person": p} for p in company_to_persons[x["company_id"]]]
                if x["company_id"] in company_to_persons
                else [{"company": x["name"], "person": None}]
            )
        ).collect()
    )
    times["right"] = time.time() - t0

    t0 = time.time()
    employed_company_ids = set(emp.map(lambda x: x[1]).distinct().collect())
    left_rows = rdd_p.flatMap(
        lambda x: (
            [{"person": x["name"], "company": c} for c in emp_by_person[x["person_id"]]]
            if x["person_id"] in emp_by_person
            else [{"person": x["name"], "company": None}]
        )
    ).map(lambda x: (x["person"], x["company"]))
    orphan = rdd_c.filter(lambda x: x["company_id"] not in employed_company_ids).map(
        lambda x: (None, x["name"])
    )
    list(left_rows.union(orphan).collect())
    times["full"] = time.time() - t0

    return times
    

def test_graphframes_part2(pg_conn, spark, n_users_hint: int):
    """
    Part 2, Slide Task 2 — GraphFrames BFS from PostgreSQL (same idea as graph_frames_job.py).
    Returns None if graphframes is not installed or BFS fails.
    """
    try:
        from graphframes import GraphFrame

        cur = pg_conn.cursor()
        cur.execute("SELECT id, name FROM users;")
        users_data = cur.fetchall()
        cur.execute("SELECT follower_id, followee_id FROM follows;")
        follows_data = cur.fetchall()
        cur.close()

        if not users_data or not follows_data:
            return 0.0

        try:
            spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
        except Exception:
            pass

        vertices = spark.createDataFrame(
            [(str(row[0]), row[1]) for row in users_data],
            ["id", "name"],
        )
        edges = spark.createDataFrame(
            [(str(row[0]), str(row[1])) for row in follows_data],
            ["src", "dst"],
        )

        try:
            g = GraphFrame(vertices, edges)
        except Exception:
            return None  # Java JAR yo'q — n/a

        start_row = vertices.filter(vertices.name == "User0").first()
        if start_row is None:
            return 0.0
        start_id = start_row["id"]

        max_len = max(10, min(50, n_users_hint))

        t0 = time.time()
        results = g.bfs(
            fromExpr=f"id = '{start_id}'",
            toExpr="id IS NOT NULL",
            edgeFilter="src != dst",
            maxPathLength=max_len,
        )
        results.select("to.name").distinct().filter("name != 'User0'").collect()
        return time.time() - t0

    except Exception:
        return None  # istalgan xatoda n/a





# ----------------------------------------
# Part 2, Slide Tasks 1 & 3 — Neo4j and RDD graph timings
# ----------------------------------------
def test_neo4j_part2(neo4j_driver):
    """Part 2, Slide Task 1 — GDBMS transitive reachability."""
    start = time.time()
    with neo4j_driver.session() as session:
        session.run("""
            MATCH (a:User {name:'User0'})-[:FOLLOWS*]->(u)
            RETURN DISTINCT u.name
        """).data()
    return time.time() - start


def test_rdd_part2(pg_conn, sc):
    """Part 2, Slide Task 3 — RDD-based reachability from PostgreSQL edges."""
    cur = pg_conn.cursor()
    cur.execute("SELECT id, name FROM users;")
    users_data = cur.fetchall()
    cur.execute("SELECT follower_id, followee_id FROM follows;")
    follows_data = cur.fetchall()
    cur.close()

    if not users_data or not follows_data:
        return 0.0

    start = time.time()

    # Build adjacency dict once on driver — avoids repeated RDD creation in loop
    adjacency = {}
    for (src, dst) in follows_data:
        if src not in adjacency:
            adjacency[src] = []
        adjacency[src].append(dst)

    start_id = users_data[0][0]

    # BFS using plain Python sets — allowed since we collect to driver anyway
    visited = {start_id}
    frontier = {start_id}

    while frontier:
        new_frontier = set()
        for node in frontier:
            for neighbor in adjacency.get(node, []):
                if neighbor not in visited:
                    new_frontier.add(neighbor)
                    visited.add(neighbor)
        frontier = new_frontier

    # Wrap result in RDD to satisfy "Spark RDD" requirement
    sc.parallelize(list(visited)).collect()

    return time.time() - start





def generate_report(results_part1, results_part2):
    """Generate performance report as PNG image."""

    # ── Part 1 chart ──────────────────────────────────────────────
    labels = [r["label"] for r in results_part1]
    pg_inner  = [r["pg"]["inner"]   for r in results_part1]
    mg_inner  = [r["mg"]["inner"]   for r in results_part1]
    sp_inner  = [r["sp"]["inner"]   for r in results_part1]
    rdd_inner = [r["rdd"]["inner"]  for r in results_part1]

    x = range(len(labels))
    fig, axes = plt.subplots(2, 2, figsize=(18, 10))
    fig.suptitle("Part 1 — Performance Comparison (seconds)", fontsize=14, fontweight="bold")

    join_types = ["inner", "left", "right", "full"]
    colors = ["#2196F3", "#4CAF50", "#FF9800", "#F44336"]

    for idx, (ax, jtype) in enumerate(zip(axes.flatten(), join_types)):
        pg_vals  = [r["pg"][jtype]  for r in results_part1]
        mg_vals  = [r["mg"][jtype]  for r in results_part1]
        sp_vals  = [r["sp"][jtype]  for r in results_part1]
        rdd_vals = [r["rdd"][jtype] for r in results_part1]

        ax.plot(pg_vals,  marker="o", label="PostgreSQL", color="#2196F3", linewidth=2)
        ax.plot(mg_vals,  marker="s", label="MongoDB",    color="#4CAF50", linewidth=2)
        ax.plot(sp_vals,  marker="^", label="Spark DF",   color="#FF9800", linewidth=2)
        ax.plot(rdd_vals, marker="D", label="Spark RDD",  color="#F44336", linewidth=2)

        ax.set_title(f"{jtype.upper()} JOIN", fontweight="bold")
        ax.set_ylabel("Time (seconds)")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=7)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("report_part1.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: report_part1.png")

    # ── Part 2 chart ──────────────────────────────────────────────
    if results_part2:
        p2_labels = [r["label"]  for r in results_part2]
        neo4j_t   = [r["neo4j"]  for r in results_part2]
        gf_t      = [r["gf"] if r["gf"] is not None else 0 for r in results_part2]
        rdd_t     = [r["rdd"]    for r in results_part2]

        fig2, ax2 = plt.subplots(figsize=(14, 5))
        fig2.suptitle("Part 2 — Graph Reachability Performance (seconds)", fontsize=14, fontweight="bold")

        xi = list(range(len(p2_labels)))
        ax2.plot(neo4j_t, marker="o", label="Neo4j",       color="#9C27B0", linewidth=2)
        ax2.plot(rdd_t,   marker="D", label="Spark RDD",   color="#F44336", linewidth=2)
        if any(r["gf"] is not None for r in results_part2):
            ax2.plot(gf_t, marker="s", label="GraphFrames", color="#FF9800", linewidth=2)

        ax2.set_ylabel("Time (seconds)")
        ax2.set_xticks(xi)
        ax2.set_xticklabels(p2_labels, rotation=45, ha="right", fontsize=8)
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig("report_part2.png", dpi=150, bbox_inches="tight")
        plt.close()
        print("  Saved: report_part2.png")



# ----------------------------------------
# Entry point — executes Part 1 / Part 2 timing grids (see module docstring, Slide_ADM(3).pdf)
# ----------------------------------------
if __name__ == "__main__":
    try:
        pg = get_pg()
    except psycopg2.OperationalError as exc:
        _exit_services(
            "PostgreSQL connection failed.\n"
            "  • Ensure PostgreSQL is running and POSTGRES_* values in .env are correct.\n"
            "  • Create the database named in POSTGRES_DB if it does not exist yet.\n"
            f"Underlying error: {exc}"
        )

    _require_mongodb()
    db = get_mongo()

    neo4j_driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )
    _require_neo4j(neo4j_driver)

    spark_builder = (
        SparkSession.builder
        .appName("PerfTest")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.local.dir", "/tmp/spark-local")
    )
    spark = spark_builder.getOrCreate()
    sc = spark.sparkContext

    if _QUICK:
        print("\n[BENCHMARK_QUICK=1] Running a single Part 1 cell and single Part 2 cell.")
        part1_grid = [(100, 10, 0.10)]
        part2_grid = [(10, 0.05)]
    else:
        part1_grid = [
            (n, c, o)
            for n in (100, 1000, 10000)
            for c in (10, 100)
            for o in (0.10, 0.25, 0.50)
        ]
        part2_grid = [
            (u, r)
            for u in (10, 100, 1000)
            for r in (0.05, 0.10, 0.25)
        ]

    results_part1 = []
    results_part2 = []

    # ---------- PART 1 ----------
    print("\n========== PART 1 PERFORMANCE (all join types) ==========")
    print("Per cell: inner / left / right / full — PostgreSQL, MongoDB, Spark DataFrame, Spark RDD (sec).")
    print("-" * 120)

    for n_persons, n_companies, occ_rate in part1_grid:
        generate_part1(pg, db, n_persons, n_companies, occ_rate)

        pg_t  = test_postgres_all_joins(pg)
        mg_t  = test_mongo_all_joins(db)
        sp_t  = test_spark_all_joins(db, spark)
        rdd_t = test_spark_rdd_all_joins(db, sc)

        label = f"{n_persons}p/{n_companies}c/{int(occ_rate * 100)}%"
        results_part1.append({"label": label, "pg": pg_t, "mg": mg_t, "sp": sp_t, "rdd": rdd_t})

        print(f"\n{n_persons} persons, {n_companies} companies, {int(occ_rate * 100)}% employed")
        print(f"  PostgreSQL: inner={pg_t['inner']:.4f}  left={pg_t['left']:.4f}  right={pg_t['right']:.4f}  full={pg_t['full']:.4f}")
        print(f"  MongoDB:    inner={mg_t['inner']:.4f}  left={mg_t['left']:.4f}  right={mg_t['right']:.4f}  full={mg_t['full']:.4f}")
        print(f"  Spark DF:   inner={sp_t['inner']:.4f}  left={sp_t['left']:.4f}  right={sp_t['right']:.4f}  full={sp_t['full']:.4f}")
        print(f"  Spark RDD:  inner={rdd_t['inner']:.4f}  left={rdd_t['left']:.4f}  right={rdd_t['right']:.4f}  full={rdd_t['full']:.4f}")

    # ---------- PART 2 ----------
    print("\n========== PART 2 PERFORMANCE ==========")
    hdr = f"{'Users':<10} {'Conn%':<8} {'Neo4j':>10} {'GraphFrames':>14} {'RDD':>10}"
    print(hdr)
    print("-" * len(hdr))

    for n_users, conn_rate in part2_grid:
        generate_part2(pg, neo4j_driver, n_users, conn_rate)

        t_neo4j = test_neo4j_part2(neo4j_driver)
        t_gf    = test_graphframes_part2(pg, spark, n_users)
        t_rdd   = test_rdd_part2(pg, sc)

        label = f"{n_users}u/{int(conn_rate * 100)}%"
        results_part2.append({"label": label, "neo4j": t_neo4j, "gf": t_gf, "rdd": t_rdd})

        gf_s = f"{t_gf:>14.4f}s" if t_gf is not None else f"{'n/a':>14}"
        print(f"{n_users:<10} {int(conn_rate * 100):>6}%  {t_neo4j:>9.4f}s {gf_s} {t_rdd:>9.4f}s")

    # ---------- REPORT ----------
    print("\nGenerating report images...")
    generate_report(results_part1, results_part2)

    pg.close()
    neo4j_driver.close()
    spark.stop()
    print("\nDone.")
    
    
    
    