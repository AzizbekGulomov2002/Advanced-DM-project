// =====================
// CLEAN DATABASE
// =====================
MATCH (n) DETACH DELETE n;

// =====================
// CREATE USERS
// =====================
CREATE 
(:User {id:1, name:"Mark"}),
(:User {id:2, name:"Pavel"}),
(:User {id:3, name:"Sam"}),
(:User {id:4, name:"Elon"}),
(:User {id:5, name:"Larry"});

// =====================
// CREATE FOLLOWS RELATION
// =====================
MATCH (a:User {id:1}), (b:User {id:2})
CREATE (a)-[:FOLLOWS]->(b);

MATCH (a:User {id:2}), (b:User {id:3})
CREATE (a)-[:FOLLOWS]->(b);

MATCH (a:User {id:3}), (b:User {id:4})
CREATE (a)-[:FOLLOWS]->(b);

MATCH (a:User {id:4}), (b:User {id:5})
CREATE (a)-[:FOLLOWS]->(b);

// =====================
// TRANSITIVE QUERY
// =====================
MATCH (a:User {name:"Mark"})-[:FOLLOWS*]->(u)
RETURN DISTINCT u.name;