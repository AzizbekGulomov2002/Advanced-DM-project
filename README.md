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

---
Completed by Azizbek Gulomov.
