# Advanced Data Management Project

This repository is the implementation for the assignment in `Slide_ADM(3).pdf`.
It is organized so a professor can run it end-to-end with minimal setup.

## Assignment Coverage

### Part 1 — Relational vs Document
- **RDBMS (PostgreSQL)** schema + SQL queries (Tasks 1–4): `db/postgres.sql`
- **DDBMS (MongoDB)** seed data: `db/mongo_seed.py`
- **Spark DataFrame / SparkSQL** joins from Mongo (Tasks 5–8):
  - `spark_jobs/mongo_df_job.py`
  - `spark_jobs/mongo_sql_job.py`
- **Spark RDD** joins from Mongo with restricted RDD-style logic (Tasks 9–12):
  - `spark_jobs/mongo_rdd_job.py`

### Part 2 — Relational vs Graph
- **GDBMS (Neo4j)** graph model + transitive query (Task 1): `db/neo4j.cypher`
- **Spark GraphFrames** reachability from PostgreSQL (Task 2): `spark_jobs/graph_frames_job.py`
- **Spark RDD** reachability from PostgreSQL (Task 3): `spark_jobs/rdd_graph_job.py`

### Performance Comparison (Part 1 + Part 2)
- Full benchmark script: `performance_test.py`
- Outputs timing tables in terminal and saves plots:
  - `report_part1.png`
  - `report_part2.png`

---

## 1) Prerequisites

- Python 3.10+ (tested in local venv workflow)
- Java (required by PySpark)
- PostgreSQL running locally (or reachable remotely)
- MongoDB running locally (or reachable remotely)
- Neo4j running locally (or reachable remotely)

Optional but recommended:
- `psql` CLI for PostgreSQL
- Neo4j Browser (`http://localhost:7474`) or `cypher-shell`

---

## 2) Project Setup

### 2.1 Create venv and install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2.2 Configure environment variables
Create your local config file from template:
```bash
cp .env.example .env
```

Edit `.env` with real values:
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `MONGO_URI`, `MONGO_DB`
- `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`

Note:
- `config/settings.py` loads `.env` from project root automatically.
- `.env` is git-ignored; `.env.example` is safe template only.

---

## 3) Database Initialization

### 3.1 PostgreSQL (required for Part 1 and Part 2)
Create database if needed:
```bash
createdb company_project
```

Apply schema + sample data:
```bash
psql -U postgres -d company_project -f db/postgres.sql
```

If your `.env` uses another DB name, replace `company_project` accordingly.

### 3.2 MongoDB (required for Part 1 Spark jobs and benchmarks)
Start MongoDB, then seed collections:
```bash
python db/mongo_seed.py
```

Collections created:
- `persons`
- `companies`
- `works_in`

### 3.3 Neo4j (required for Part 2 and benchmarks)
Start Neo4j and execute:
- file: `db/neo4j.cypher`

In Neo4j Browser, run the full file contents, or with shell:
```bash
cat db/neo4j.cypher | cypher-shell -u neo4j -p <password>
```

---

## 4) Run Individual Assignment Components

### 4.1 Part 1 Spark programs
```bash
python spark_jobs/mongo_df_job.py
python spark_jobs/mongo_sql_job.py
python spark_jobs/mongo_rdd_job.py
```

### 4.2 Part 2 Spark programs
```bash
python spark_jobs/graph_frames_job.py
python spark_jobs/rdd_graph_job.py
```

Notes:
- `graph_frames_job.py` requires GraphFrames package/JAR resolution by Spark.
- `rdd_graph_job.py` uses RDD frontier expansion with RDD `join` on edges.

---

## 5) Run Full Performance Evaluation (Professor Run Path)

### 5.1 Quick smoke run (fast)
Runs one grid cell from each part:
```bash
BENCHMARK_QUICK=1 python performance_test.py
```

### 5.2 Full assignment run
Runs all required grids:
- Part 1: persons `{100,1000,10000}`, companies `{10,100}`, occupation `{10%,25%,50%}`
- Part 2: users `{10,100,1000}`, connection rates `{5%,10%,25%}`

```bash
python performance_test.py
```

### 5.3 Output artifacts
After run:
- terminal tables for Part 1 and Part 2 timings
- `report_part1.png`
- `report_part2.png`

---

## 6) Verification Checklist (Before Submission)

Run in this order:
1. `psql ... -f db/postgres.sql`
2. `python db/mongo_seed.py`
3. apply `db/neo4j.cypher` in Neo4j
4. `BENCHMARK_QUICK=1 python performance_test.py`
5. (optional) full `python performance_test.py`

Expected:
- no connection errors
- Part 1 and Part 2 timing tables printed
- report PNG files generated

---

## 7) Common Runtime Issues and Fixes

### PostgreSQL: `database "...\" does not exist`
- Create DB named in `POSTGRES_DB`, or update `.env` to an existing DB.

### MongoDB: `ServerSelectionTimeoutError ... connection refused`
- Start MongoDB service/container.
- Verify `MONGO_URI` and `MONGO_DB` in `.env`.

### Neo4j connectivity failure
- Start Neo4j service.
- Verify `NEO4J_URI`, username, password in `.env`.

### Spark/GraphFrames warnings
- Some startup warnings are normal on local macOS.
- If GraphFrames fails, ensure Java + internet access for package resolution.

---

## 8) Important Files

- `Slide_ADM(3).pdf` — assignment statement
- `ADM_Project.pdf` — report document
- `db/postgres.sql` — relational schema + SQL tasks
- `db/mongo_seed.py` — Mongo seed for Part 1
- `db/neo4j.cypher` — graph setup + transitive query
- `spark_jobs/*.py` — Spark implementations for Part 1/2
- `performance_test.py` — benchmark harness and report images
- `.env.example` — environment template

---

Prepared for reproducible evaluation and oral/project review.
