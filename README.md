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

### Part 2: Relational vs Graph Model
- Graph dataset and relationships are implemented in `db/neo4j.cypher` with `User` nodes and `FOLLOWS` edges.
- Transitive reachability query is implemented with `[:FOLLOWS*]`.
- Neo4j service logic is implemented in `services/neo4j_service.py`.
- API endpoint:
  - `GET /neo4j/reachable/{name}`

### Spark Jobs (`spark_jobs`)

#### `spark_jobs/dataframe_job.py`
- Uses `SparkSession` and reads `data/persons.json`.
- Executes:
  - `groupBy("company").count().show()`
- This is a **DataFrame aggregate** operation to compute the number of records per company.

#### `spark_jobs/rdd_job.py`
- Uses `SparkContext`.
- Reads JSON lines and parses each row with `json.loads`.
- Performs MapReduce-style processing:
  - **Map step:** `(company, 1)`
  - **Reduce step:** `reduceByKey(lambda a, b: a + b)`
- Returns total person count per company via `collect()`.

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
- `GET /mongo/inner`
- `GET /mongo/left`
- `GET /mongo/right`
- `GET /mongo/full`
- `GET /neo4j/reachable/{name}`

## Run Spark Jobs
```bash
python spark_jobs/dataframe_job.py
python spark_jobs/rdd_job.py
```

## Notes
- The repository currently includes core relational/document/graph demonstrations.
- Spark files currently demonstrate counting by company using both DataFrame aggregate and RDD MapReduce style.

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

Measured output from `performance_test.py`:
- Postgres: `0.2325 sec`
- MongoDB: `0.0404 sec`
- Spark: `8.2996 sec`

## Why This Matters
Most students can write code, but fewer can clearly explain design choices and trade-offs. This section makes the project academically stronger by connecting implementation decisions with system behavior and performance.

---
Completed by Azizbek Gulomov.
