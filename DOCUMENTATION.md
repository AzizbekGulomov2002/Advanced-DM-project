# Advanced Data Management - Technical Documentation

## 1) Purpose
This document explains how the project was implemented according to the task in `Slide_ADM(3).pdf`, how each module works, and how to run each database component and Spark job.

## 2) Technology Stack
- Python
- FastAPI
- PostgreSQL (relational model)
- MongoDB with **PyMongo** (document model)
- Neo4j (graph model)
- PySpark (DataFrame and RDD jobs)

## 3) Implementation by Task Sections

## Part 1: Comparing Relational and Document Models

### 3.1 Relational model implementation (PostgreSQL)
File: `db/postgres.sql`

Implemented artifacts:
- Table `person`
- Table `company`
- Junction table `works_in` for many-to-many relationships
- Sample insert statements
- SQL query implementations for:
  - employed persons with their companies (`INNER JOIN`)
  - all persons and optional company info (`LEFT JOIN`)
  - all companies and optional person info: **`FROM company` + `LEFT JOIN works_in` + `LEFT JOIN person`** (correct “right side is companies” semantics)
  - full person-company view: **`WITH` employed rows `UNION ALL` companies with no employees** (avoids broken chained `FULL OUTER JOIN` null pairing)

This directly models the PERSON-COMPANY-WORKS_IN structure required in the assignment.

### 3.2 Document model implementation (MongoDB with PyMongo)
Files:
- `db/mongo_seed.py`
- `services/mongo_service.py`

`db/mongo_seed.py` responsibilities:
- Connects with `MongoClient("mongodb://localhost:27017/")`
- Clears previous data
- Inserts sample data into `persons`, `companies`, and relationship collection

`services/mongo_service.py` responsibilities:
- Implements join-like behavior via MongoDB Aggregation:
  - `mongo_inner_join()`
  - `mongo_left_join()`
  - `mongo_right_join()`
  - `mongo_full_join()`
- Uses stages such as `$lookup`, `$unwind`, `$project`, and `$ifNull`.

### 3.3 API exposure for Part 1
File: `app/main.py`

PostgreSQL endpoints (SQL matches `db/postgres.sql` join semantics, including company-based **right** and **full** via `WITH`/`UNION ALL`):
- `GET /postgres/inner`
- `GET /postgres/left`
- `GET /postgres/right`
- `GET /postgres/full`

MongoDB endpoints:
- `GET /mongo/inner`
- `GET /mongo/left`
- `GET /mongo/right`
- `GET /mongo/full`

These return JSON results to compare join behavior at application level.

## Part 2: Comparing Relational and Graph Models

### Users table (PostgreSQL, Part 2, slide schema)
- Columns: `name`, `surname`, `birthdate`, `address`, `country`, optional `job`, optional **`study_title`** (matches the assignment USER entity).

### 3.4 Graph model implementation (Neo4j)
Files:
- `db/neo4j.cypher`
- `services/neo4j_service.py`

`db/neo4j.cypher` responsibilities:
- Clears graph
- Creates `User` nodes
- Creates `FOLLOWS` relationships
- Provides transitive reachability query:
  - `MATCH (a:User {name:"Mark"})-[:FOLLOWS*]->(u) RETURN DISTINCT u.name;`

`services/neo4j_service.py` responsibilities:
- Connects to Neo4j via Bolt driver
- Executes:
  - `MATCH (a:User {name:$name})-[:FOLLOWS*]->(u) RETURN DISTINCT u.name AS name`
- Returns reachable users list to API caller.

### 3.5 API exposure for Part 2
File: `app/main.py`

Neo4j endpoint:
- `GET /neo4j/follows/{name}`

This endpoint returns all transitively reachable users for a given user.

## 4) Spark Jobs Explanation

## 4.0 Part 1 joins from MongoDB (Tasks 5–8 / 9–12)

### `spark_jobs/mongo_df_job.py` (DataFrame API)
- Task 5: inner join on `works_in` → `persons` → `companies`
- Task 6: left join from `persons`
- Task 7: **company-based** left join chain (all companies)
- Task 8: **not** chained `FULL OUTER`. Same as SQL: **person left joins** for all employment rows, **`left_anti`** companies with no `works_in`, then **`unionByName`** (employed side + orphan companies). Columns: `person_name`, `surname`, `company_name`.

### `spark_jobs/mongo_sql_job.py` (SparkSQL)
- Task 5–8: same four join patterns expressed as `spark.sql(...)` queries.
- Task 8 uses **`UNION ALL`**: employed rows from `persons` left joins, plus orphan companies (`companies` left join `works` where `w.person_id IS NULL`), matching the SQL CTE semantics.
- **Why not only `mongo_df_job.py`?** The assignment allows Spark joins using DataFrame/SQL APIs. Keeping both files shows the **same logic** in two Spark interfaces (oral defense: “not two different answers — one pipeline, two APIs”).

### `spark_jobs/mongo_rdd_job.py` (RDD, allowed primitives)
- Task 9: inner join style mapping from `works_in`
- Task 10: left join for all persons; **`groupByKey` per person** so multiple employers are kept
- Task 11: right join from all companies
- Task 12: full outer style = left rows **union** companies with no employment edges

## 4.1 `spark_jobs/rdd_graph_job.py` (Part 2, RDD graph)
- Loads `users` and `follows` from PostgreSQL.
- Builds `rdd_edges` as `(follower_id, followee_id)`.
- BFS-style expansion: **`frontier.map((id,1)).join(rdd_edges)`** → followee ids. This uses Spark’s **distributed join** on the edge RDD (not a driver-side `dict` of the full adjacency list).
- Final step: join reachable ids to `rdd_users` to print names.

## 4.2 `spark_jobs/graph_frames_job.py` (Part 2, GraphFrames)
- Loads the same social graph from PostgreSQL into a **GraphFrame**.
- Runs **`bfs`** from a chosen user (`Mark`) for transitive reachability.

## 5) End-to-End Run Guide

## 5.1 Install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 5.1.1 Configure `.env` credentials
Create local environment file:
```bash
cp .env.example .env
```

Set actual values in `.env`:
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `MONGO_URI`, `MONGO_DB`
- `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`

Implementation note:
- Database connection credentials are loaded from `.env` through `config/settings.py`.
- `.env` is git-ignored, and `.env.example` is the tracked template.

## 5.2 PostgreSQL setup with pgAdmin
1. Open **pgAdmin**.
2. Create a database (example: `advanced_dm`).
3. Open Query Tool.
4. Run the full script from `db/postgres.sql`.

Optional command line alternative:
```bash
psql -U postgres -d advanced_dm -f db/postgres.sql
```

## 5.3 MongoDB setup using PyMongo
Start MongoDB server:
```bash
mongod --dbpath ~/mongodb-data
```

Seed database through PyMongo script:
```bash
python db/mongo_seed.py
```

## 5.4 Neo4j graph setup
1. Start Neo4j database (Neo4j Desktop or local service).
2. Open Neo4j Browser at `http://localhost:7474`.
3. Execute commands from `db/neo4j.cypher`.

If using `cypher-shell`, you can run:
```bash
cat db/neo4j.cypher | cypher-shell -u neo4j -p <your_password>
```

## 5.5 Run API service
```bash
uvicorn app.main:app --reload
```

## 5.6 Run Spark jobs
```bash
python spark_jobs/mongo_df_job.py
python spark_jobs/mongo_rdd_job.py
python spark_jobs/mongo_sql_job.py
python spark_jobs/rdd_graph_job.py
python spark_jobs/graph_frames_job.py
```

## 6) API Endpoints Summary
- `GET /postgres/inner` | `/postgres/left` | `/postgres/right` | `/postgres/full`
- `GET /mongo/inner`
- `GET /mongo/left`
- `GET /mongo/right`
- `GET /mongo/full`
- `GET /neo4j/follows/{name}`

## 7) Final Notes
- The project demonstrates relational, document, and graph querying approaches clearly.
- Spark jobs currently focus on company-based counting using:
  - DataFrame aggregate (`groupBy + count`)
  - RDD MapReduce (`map + reduceByKey`)

## 8) System Design Choices (Why These Technologies)

### Why PostgreSQL?
PostgreSQL was used because it is a relational database designed for structured data and supports efficient JOIN operations. It is ideal when the schema is fixed and relationships between entities are well-defined.

### Why MongoDB?
MongoDB was chosen because it is a document-oriented NoSQL database that supports flexible schemas. It is useful when working with semi-structured data. However, since MongoDB has limited join capabilities, aggregation pipelines and Spark-style processing are used where needed.

### Why Neo4j?
Neo4j was used because it is a graph database optimized for relationship-based queries. It allows efficient traversal operations such as multi-hop connections (transitive relationships), which are difficult to implement in relational databases.

### Why Apache Spark?
Apache Spark was used as a distributed data processing engine. It allows handling large-scale data efficiently using parallel computation. In this project, Spark demonstrates distributed aggregation and MapReduce-like processing.

### Why RDD vs DataFrame vs SparkSQL?
- **RDD (Resilient Distributed Dataset):**
  Used for low-level control and manual MapReduce implementation, showing internal distributed computation logic.
- **DataFrame:**
  Higher-level API with optimizer support, easier syntax, and better performance for most analytical operations.
- **SparkSQL:**
  SQL interface on distributed data, useful for expressing structured analytical queries with familiar SQL syntax.

### Key Insight
Different systems are optimized for different tasks:
- PostgreSQL -> structured data and strong consistency
- MongoDB -> flexible schema and document storage
- Neo4j -> relationship traversal
- Spark -> large-scale distributed computation

This project demonstrates how these systems complement each other rather than compete.

## 9) Performance Observation
Based on experiments:
- PostgreSQL and MongoDB are faster for small datasets due to lower overhead.
- Apache Spark has higher startup cost but becomes more efficient for large-scale data processing.
- Graph queries are most efficient in Neo4j due to native traversal optimization.

### Part 1: measuring all four join types
`performance_test.py` regenerates data for each grid point, then prints **inner / left / right / full** timings for:

- PostgreSQL (four SQL statements)
- MongoDB (matching aggregation pipelines; “full” = left-style persons pipeline + companies with empty `works_in` lookup)
- Spark DataFrames (including **full** = employed + orphan companies, same as `mongo_df_job.py`)
- **Spark RDD** (Tasks 9–12 style joins, timed separately)

Use **`BENCHMARK_QUICK=1`** for one Part 1 cell and one Part 2 cell to fill a sample report table quickly.

### Part 2: Neo4j + GraphFrames + RDD
For each Part 2 grid point the script prints:

- Neo4j transitive `FOLLOWS*` query time  
- **GraphFrames** `bfs` time (requires `graphframes` Python package and JAR; set `ENABLE_GRAPHFRAMES_JAR=0` to skip Ivy download — then timing shows `n/a` if GraphFrames is unavailable)  
- Spark RDD BFS-style reachability time  

Copy the printed lines into a table for your report. Example layouts are in `README.md`.

Legacy quick sample (inner join only, old smoke test):

- Postgres: `0.2325 sec`
- MongoDB: `0.0404 sec`
- Spark: `8.2996 sec`

## 10) Academic Value
Most students can write code, but fewer can clearly explain design choices and trade-offs. This documentation closes that gap by linking implementation decisions with scalability, query behavior, and performance implications.

## 11) Stability Fixes (Implemented from Feedback)

### 11.1 Duplicate graph RDD job removed
- Deleted `spark_jobs/graph_rdd_job.py` (old duplicate).
- Kept `spark_jobs/rdd_graph_job.py` as the valid script.

### 11.2 `generate_part1()` bug fixed in `performance_test.py`
- The insert order is now correct:
  1. insert companies
  2. insert persons
  3. insert `works_in` links
- This prevents `company_ids` from being empty and avoids relation insert failures.

### 11.3 Neo4j benchmark path fixed for Part 2
- `generate_part2()` previously filled only PostgreSQL.
- Now `generate_part2()` also syncs the same generated users and follows edges to Neo4j.
- `test_neo4j_part2()` now runs against actual generated graph data, so `User0` is available.

### 11.4 Semantics fixes (Part 1 joins)
- PostgreSQL **RIGHT** and **FULL** queries updated to match the assignment wording.
- Spark **DataFrame** right/full joins fixed (`mongo_df_job.py`).
- Spark **SQL** now implements all four joins (`mongo_sql_job.py`).
- Spark **RDD** left join fixed for multi-company persons; full join rebuilt (`mongo_rdd_job.py`).

### 11.5 Performance harness
- `performance_test.py` Part 1 now benchmarks **inner, left, right, and full** for Postgres, MongoDB, and Spark (not only inner).

### 11.6 `rdd_graph_job.py` cleanup
- Reachable ids collected into a **set** for faster lookup; removed unused intermediate RDD.

### 11.7 Slide-aligned schema and data
- **`users.study_title`** added in PostgreSQL; Neo4j `User` nodes include `study_title` where applicable.
- **`company.sector`** sample data uses only allowed sectors: automotive, banking, services, healthcare, chemicals, public.

### 11.8 Benchmark coverage (Tasks 1–12 + Part 2)
- Part 1: Spark **RDD** join timings added (Tasks 9–12).
- Part 2: **GraphFrames** BFS timing added next to Neo4j and RDD.
- **Task 8 DataFrame / SparkSQL:** full semantics aligned with SQL CTE (`UNION ALL` pattern).

---
Completed by Azizbek Gulomov.
