from neo4j import GraphDatabase
from config.settings import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USERNAME

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
)

def get_reachable_users(name):
    with driver.session() as session:
        result = session.run("""
            MATCH (a:User {name:$name})-[:FOLLOWS*]->(u)
            RETURN DISTINCT u.name AS name
        """, name=name)

        return [record["name"] for record in result]