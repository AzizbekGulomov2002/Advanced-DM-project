# Advanced Data Management Project

## Project Overview
This project is implemented for the **Advanced Data Management** course based on the task in `Slide_ADM(3).pdf`.

It demonstrates three data models:
- **Relational model** using PostgreSQL
- **Document model** using MongoDB (through PyMongo)
- **Graph model** using Neo4j

The main goal is to compare how relationships are modeled and queried across different database systems.

## What Is Implemented (Based on the Slide)

### Part 1: Relational vs Document Model
- SQL schema and sample data are implemented in `db/postgres.sql`:
  - `person`
  - `company`
  - `works_in`
- SQL query types included:
  - `INNER JOIN`
  - `LEFT JOIN`
  - `RIGHT JOIN`
  - `FULL OUTER JOIN`
- MongoDB seed script is implemented in `db/mongo_seed.py` using **PyMongo**.
- MongoDB join-like operations are implemented in `services/mongo_service.py` using aggregation stages such as `$lookup`, `$unwind`, and `$project`.
- API endpoints for MongoDB outputs:
  - `GET /mongo/inner`
  - `GET /mongo/left`
  - `GET /mongo/right`
  - `GET /mongo/full`
- API endpoints for PostgreSQL (same join ideas as `db/postgres.sql`):
  - `GET /postgres/inner`
  - `GET /postgres/left`
  - `GET /postgres/right`
  - `GET /postgres/full`

### Part 2: Relational vs Graph Model
- Graph dataset and relationships are implemented in `db/neo4j.cypher` with `User` nodes and `FOLLOWS` edges.
- Transitive reachability query is implemented with `[:FOLLOWS*]`.
- Neo4j service logic is implemented in `services/neo4j_service.py`.
- API endpoint:
  - `GET /neo4j/follows/{name}`

### Spark Jobs (`spark_jobs`)

| File | Role |
|------|------|
| `mongo_df_job.py` | Part 1 Tasks 5–8 — **DataFrame** API, data from MongoDB |
| `mongo_sql_job.py` | Same tasks — **SparkSQL** (see docstring: why both exist) |
| `mongo_rdd_job.py` | Part 1 Tasks 9–12 — **RDD** joins from MongoDB |
| `rdd_graph_job.py` | Part 2 Task 3 — graph reachability from PostgreSQL (**RDD join** on edges) |
| `graph_frames_job.py` | Part 2 Task 2 — **GraphFrames** BFS from PostgreSQL |

## Run Instructions

## 1) Install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 1.1) Configure environment variables
Create your local `.env` file from the template:
```bash
cp .env.example .env
```

Then set your real credentials in `.env`:
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `MONGO_URI`, `MONGO_DB`
- `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`

Security note:
- `.env` is ignored by git.
- `.env.example` is committed as a safe template without real secrets.

## 2) PostgreSQL (pgAdmin)
1. Open **pgAdmin** and create a database (example: `advanced_dm`).
2. Open Query Tool.
3. Execute the SQL script from `db/postgres.sql`.

If you already applied an older `postgres.sql` without `study_title`, add the column once:

```sql
ALTER TABLE users ADD COLUMN study_title VARCHAR(100);
```

Optional CLI alternative:
```bash
psql -U postgres -d advanced_dm -f db/postgres.sql
```

## 3) MongoDB with PyMongo
Start MongoDB server:
```bash
mongod --dbpath ~/mongodb-data
```

Seed MongoDB data using PyMongo:
```bash
python db/mongo_seed.py
```

## 4) Neo4j Graph DB
1. Start Neo4j Database (Neo4j Desktop or local service).
2. Open Neo4j Browser (`http://localhost:7474`).
3. Run the commands from `db/neo4j.cypher`.

The key query used for transitive traversal is:
```cypher
MATCH (a:User {name:"Mark"})-[:FOLLOWS*]->(u)
RETURN DISTINCT u.name;
```

## 5) Run FastAPI
```bash
uvicorn app.main:app --reload
```

## API Endpoints
- `GET /postgres/inner` | `/postgres/left` | `/postgres/right` | `/postgres/full`
- `GET /mongo/inner` | `/mongo/left` | `/mongo/right` | `/mongo/full`
- `GET /neo4j/follows/{name}`

## Run Spark Jobs
```bash
python spark_jobs/mongo_df_job.py
python spark_jobs/mongo_rdd_job.py
python spark_jobs/mongo_sql_job.py
python spark_jobs/rdd_graph_job.py
python spark_jobs/graph_frames_job.py
```

## Notes
- `mongo_df_job.py` and `mongo_sql_job.py` implement the **same** Part 1 join scenarios in two Spark styles (DataFrame vs SQL); see the docstring in `mongo_sql_job.py` for a short defense line for exams.
- `rdd_graph_job.py` expands the BFS frontier with an **RDD `join`** on `(follower_id, followee_id)` edges instead of a Python `dict` of the whole adjacency list.

# System Design Choices (Why These Technologies)

## Why PostgreSQL?
PostgreSQL was used because it is a relational database designed for structured data and supports efficient JOIN operations. It is ideal when the schema is fixed and relationships between entities are well-defined.

## Why MongoDB?
MongoDB was chosen because it is a document-oriented NoSQL database that supports flexible schemas. It is useful when working with semi-structured data. However, since MongoDB has limited join capabilities, we used aggregation pipelines and Spark to simulate joins.

## Why Neo4j?
Neo4j was used because it is a graph database optimized for relationship-based queries. It allows efficient traversal operations such as multi-hop connections (transitive relationships), which are difficult to implement in relational databases.

## Why Apache Spark?
Apache Spark was used as a distributed data processing engine. It allows handling large-scale data efficiently using parallel computation. Spark was used to simulate MapReduce operations and to perform joins across distributed datasets.

## Why RDD, DataFrame, and SparkSQL?

- RDD (Resilient Distributed Dataset):
  Used for low-level control and to manually implement MapReduce logic. It helps understand how distributed computation works internally.

- DataFrame:
  Provides a higher-level abstraction with optimizations. It is easier to use and more efficient than RDD for most operations.

- SparkSQL:
  Allows querying data using SQL syntax. It demonstrates how structured queries can be executed on distributed systems.

## Key Insight

Different systems are optimized for different tasks:

- PostgreSQL -> structured data and strong consistency
- MongoDB -> flexible schema and document storage
- Neo4j -> relationship traversal
- Spark -> large-scale distributed computation

This project demonstrates how these systems complement each other rather than compete.

# Performance Observation

Based on experiments:

- PostgreSQL and MongoDB are faster for small datasets due to lower overhead.
- Apache Spark has higher startup cost but becomes more efficient for large-scale data processing.
- Graph queries are most efficient in Neo4j due to native traversal optimization.

## Part 1 benchmark (`performance_test.py`)

The script runs the full assignment grid: persons in `{100, 1000, 10000}`, companies in `{10, 100}`, occupation `{10%, 25%, 50%}`.

For **each** cell it measures **four** join patterns (Tasks 1–4 style) on:

- **PostgreSQL** (four SQL queries)  
- **MongoDB** (matching aggregation pipelines)  
- **Spark DataFrame** (inner / left / company-based right / full = employed + orphan companies, same as SQL CTE)  
- **Spark RDD** (Tasks 9–12 style: inner, left with `groupByKey` for multi-company, right, full)

**Part 2** prints **Neo4j**, **GraphFrames BFS** (same idea as `graph_frames_job.py`; needs `graphframes` + JAR — see env vars below), and **Spark RDD** graph reachability.

### Quick run (one cell, for a sample table)

```bash
BENCHMARK_QUICK=1 python performance_test.py
```

Optional: disable automatic GraphFrames Ivy JAR download (GraphFrames column shows `n/a`):

```bash
ENABLE_GRAPHFRAMES_JAR=0 BENCHMARK_QUICK=1 python performance_test.py
```

### Example result row (100 persons, 10 companies, 10% employed)

After `BENCHMARK_QUICK=1`, copy the printed seconds into a table like this:

| Persons | Companies | Occ % | PG in | PG L | PG R | PG F | MG in | MG L | MG R | MG F | DF in | DF L | DF R | DF F | RDD in | RDD L | RDD R | RDD F |
|--------:|----------:|------:|------:|-----:|-----:|-----:|------:|-----:|-----:|-----:|------:|------:|------:|------:|-------:|------:|------:|------:|
| 100 | 10 | 10 | *from stdout* | … | … | … | … | … | … | … | … | … | … | … | … | … | … | … |

**Part 2 example row** (10 users, 5% connections):

| Users | Conn % | Neo4j (s) | GraphFrames (s) | RDD (s) |
|------:|-------:|----------:|----------------:|--------:|
| 10 | 5 | *stdout* | *stdout or n/a* | *stdout* |

Legacy single-query sample (inner only, old smoke test — not the full benchmark):

- Postgres: `0.2325 sec`
- MongoDB: `0.0404 sec`
- Spark: `8.2996 sec`

## Why This Matters
Most students can write code, but fewer can clearly explain design choices and trade-offs. This section makes the project academically stronger by connecting implementation decisions with system behavior and performance.

## Recent Fixes Applied
- Removed duplicate file `spark_jobs/graph_rdd_job.py`. The correct file is `spark_jobs/rdd_graph_job.py`.
- Fixed `generate_part1()` in `performance_test.py`:
  - now data is created in correct order: **company -> person -> works_in**
  - prevents empty `company_ids` and invalid `works_in` inserts
- Fixed Part 2 Neo4j benchmark path:
  - `generate_part2()` now writes generated users and follows relations to Neo4j
  - `test_neo4j_part2()` now runs on real generated graph data (including `User0`)
- **SQL Task 3 (RIGHT):** `db/postgres.sql` now starts from `company` with `LEFT JOIN` to `works_in` and `person` so companies with zero employees appear.
- **SQL Task 4 (FULL):** replaced chained `FULL OUTER JOIN` with a `WITH` + `UNION ALL` pattern (employed rows + orphan companies).
- **Spark DataFrame Task 8:** `mongo_df_job.py` full result = **left join chain + `left_anti` orphan companies + `unionByName`**, matching the SQL `WITH` / `UNION ALL` idea (not chained `FULL OUTER`).
- **Spark DataFrame Tasks 7–8:** Task 7 is company-based; columns `person_name`, `surname`, `company_name`.
- **SparkSQL Tasks 5–8:** `mongo_sql_job.py` now runs inner, left, right, and full queries.
- **Spark RDD Task 10:** `mongo_rdd_job.py` uses `groupByKey` so a person with multiple jobs keeps all companies.
- **Spark RDD Task 12:** full outer built from the corrected left side plus companies with no `works_in` rows.
- **Part 1 performance:** `performance_test.py` times all four join types for Postgres, MongoDB, Spark **DataFrame**, and Spark **RDD** (Tasks 9–12).
- **Part 2 performance:** Neo4j + **GraphFrames** + RDD graph timing in the same script.
- **Schema (Slide):** `users` has optional **`study_title`**; sample `company.sector` values use only **automotive, banking, services, healthcare, chemicals, public**.
- **Graph RDD job:** `rdd_graph_job.py` uses a Python `set` for reachable ids and drops unused RDD code.

---
Completed by Azizbek Gulomov.
