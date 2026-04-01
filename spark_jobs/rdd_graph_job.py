"""
Slide_ADM(3).pdf — Part 2, Task 3: RDD-based multi-hop reachability from PostgreSQL
USER/FOLLOW edges (allowed RDD operations; uses RDD join for expansion).
"""
from pyspark.sql import SparkSession
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

spark = SparkSession.builder.appName("RDD Graph Reachability").getOrCreate()
sc = spark.sparkContext

conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
)
cur = conn.cursor()
cur.execute("SELECT id, name FROM users;")
users_data = cur.fetchall()
cur.execute("SELECT follower_id, followee_id FROM follows;")
follows_data = cur.fetchall()
conn.close()

# Users: (user_id, name) — key will be used in join
rdd_users = sc.parallelize(users_data).map(lambda x: (x[0], x[1]))

# Edges: (follower_id, followee_id) — same key as frontier ids for join
rdd_edges = sc.parallelize(follows_data).map(lambda x: (x[0], x[1]))

TARGET_USER = "Mark"

start_id = (
    rdd_users.filter(lambda x: x[1] == TARGET_USER)
    .map(lambda x: x[0])
    .collect()[0]
)

visited = sc.parallelize([start_id])
frontier = sc.parallelize([start_id])

while True:
    # RDD join: each frontier id matches edge rows where follower_id == id.
    # This is a real distributed join (shuffle/hash on key), not a Python dict of the whole graph.
    frontier_keys = frontier.map(lambda fid: (fid, 1))
    new_nodes = frontier_keys.join(rdd_edges).map(lambda row: row[1][1]).distinct()

    truly_new = new_nodes.subtract(visited)
    if truly_new.isEmpty():
        break
    visited = visited.union(truly_new).distinct()
    frontier = truly_new

# Map reachable ids to names — again join users RDD on id
reachable_ids = visited.filter(lambda uid: uid != start_id).map(lambda uid: (uid, 1))
result = reachable_ids.join(rdd_users).map(lambda row: row[1][1])

print(f"\nUsers reachable from '{TARGET_USER}':")
for name in result.collect():
    print(name)

spark.stop()
