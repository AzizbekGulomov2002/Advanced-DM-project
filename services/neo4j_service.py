from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "@Azizbek1py"   # sen qo‘ygan

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def get_reachable_users(name):
    with driver.session() as session:
        result = session.run("""
            MATCH (a:User {name:$name})-[:FOLLOWS*]->(u)
            RETURN DISTINCT u.name AS name
        """, name=name)

        return [record["name"] for record in result]