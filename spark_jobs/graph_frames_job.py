"""
Slide_ADM(3).pdf — Part 2, Task 2: GraphFrames BFS / reachability on USER/FOLLOW data
loaded from PostgreSQL (relational source of truth for the graph benchmark).
"""
from pyspark.sql import SparkSession
from graphframes import GraphFrame
import psycopg2
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from config.settings import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

# ----------------------------------------
# Spark start
# ----------------------------------------
spark = SparkSession.builder \
    .appName("GraphFrames Reachability") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

# ----------------------------------------
# Load data from PostgreSQL
# ----------------------------------------
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cur = conn.cursor()

cur.execute("SELECT id, name FROM users;")
users_data = cur.fetchall()

cur.execute("SELECT follower_id, followee_id FROM follows;")
follows_data = cur.fetchall()

conn.close()

# ----------------------------------------
# Build GraphFrame
# vertices: must have column "id"
# edges:    must have columns "src" and "dst"
# ----------------------------------------
vertices = spark.createDataFrame(
    [(str(row[0]), row[1]) for row in users_data],
    ["id", "name"]
)

edges = spark.createDataFrame(
    [(str(row[0]), str(row[1])) for row in follows_data],
    ["src", "dst"]
)

g = GraphFrame(vertices, edges)

# ----------------------------------------
# Part 2 — Query 2
# Find all users reachable from a given user
# using BFS (transitive FOLLOWS traversal)
# ----------------------------------------
TARGET_USER = "Mark"

start_id = vertices.filter(vertices.name == TARGET_USER).first()["id"]

# BFS from start node — finds all reachable nodes at any depth
_max_len = max(10, min(99, len(users_data)))
results = g.bfs(
    fromExpr=f"id = '{start_id}'",
    toExpr="id IS NOT NULL",
    edgeFilter="src != dst",
    maxPathLength=_max_len
)

# Extract unique reachable user names (exclude the start user itself)
reachable = results.select("to.name") \
    .distinct() \
    .filter(f"name != '{TARGET_USER}'")

print(f"\nUsers reachable from '{TARGET_USER}':")
reachable.show()

spark.stop()