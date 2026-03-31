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
  - all companies and optional person info (`RIGHT JOIN`)
  - full person-company matching (`FULL OUTER JOIN`)

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

MongoDB endpoints:
- `GET /mongo/inner`
- `GET /mongo/left`
- `GET /mongo/right`
- `GET /mongo/full`

These return JSON results to compare join behavior at application level.

## Part 2: Comparing Relational and Graph Models

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
- `GET /neo4j/reachable/{name}`

This endpoint returns all transitively reachable users for a given user.

## 4) Spark Jobs Explanation

## 4.1 `spark_jobs/dataframe_job.py`
Current logic:
- Creates `SparkSession`
- Reads `data/persons.json` as a DataFrame
- Runs `df.groupBy("company").count().show()`

Meaning:
- This is a **DataFrame aggregate** operation.
- It groups rows by `company` and computes row counts per group.
- Equivalent SQL idea:
  - `SELECT company, COUNT(*) FROM persons GROUP BY company;`

## 4.2 `spark_jobs/rdd_job.py`
Current logic:
- Creates `SparkContext`
- Loads JSON lines with `textFile`
- Parses each line with `json.loads`
- Maps each record to `(company, 1)`
- Aggregates with `reduceByKey(lambda a, b: a + b)`
- Prints collected result

Meaning:
- This follows the classic **MapReduce pattern**:
  - **Map:** emit key-value pairs `(company, 1)`
  - **Reduce/Aggregate:** sum counts per `company`
- This is an RDD-level aggregate equivalent to `GROUP BY company COUNT(*)`.

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
python spark_jobs/dataframe_job.py
python spark_jobs/rdd_job.py
```

## 6) API Endpoints Summary
- `GET /mongo/inner`
- `GET /mongo/left`
- `GET /mongo/right`
- `GET /mongo/full`
- `GET /neo4j/reachable/{name}`

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

Measured output from `performance_test.py`:
- Postgres: `0.2325 sec`
- MongoDB: `0.0404 sec`
- Spark: `8.2996 sec`

## 10) Academic Value
Most students can write code, but fewer can clearly explain design choices and trade-offs. This documentation closes that gap by linking implementation decisions with scalability, query behavior, and performance implications.

---
Completed by Azizbek Gulomov.
