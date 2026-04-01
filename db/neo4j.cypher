// =============================================================================
// Advanced Data Management — Slide_ADM(3).pdf
// PART 2 — GRAPH (GDBMS) side: USER nodes + FOLLOWS relationships.
// Implements the slide USER model (optional job, study_title) and FOLLOW edges.
//
// Sample graph below supports:
//   Part 2, Slide Task 1 — transitive “who can see whose content” (example query at file end).
// Spark Part 2 Task 2 (GraphFrames) and Task 3 (RDD) load the same network from PostgreSQL
// in jobs; this file is the canonical Neo4j dataset for Task 1 + API demo.
// =============================================================================

// Clean all existing nodes and relationships
MATCH (n) DETACH DELETE n;

// Sample USER nodes (slide Part 2 conceptual model)
CREATE
(:User {
    id: 1,
    name: "Mark",
    surname: "Zuckerberg",
    birthdate: "1984-10-10",
    address: "Palo Alto, California",
    country: "US",
    job: "CEO",
    study_title: "Harvard dropout"
}),
(:User {
    id: 2,
    name: "Pavel",
    surname: "Durov",
    birthdate: "1984-10-10",
    address: "Dubai",
    country: "UAE",
    job: "CEO",
    study_title: "Saint Petersburg State"
}),
(:User {
    id: 3,
    name: "Sam",
    surname: "Altman",
    birthdate: "1985-04-22",
    address: "San Francisco, California",
    country: "US",
    job: "CEO",
    study_title: "Stanford"
}),
(:User {
    id: 4,
    name: "Elon",
    surname: "Musk",
    birthdate: "1971-06-28",
    address: "Austin, Texas",
    country: "US",
    job: "CEO",
    study_title: "Penn/Stanford"
}),
(:User {
    id: 5,
    name: "Larry",
    surname: "Page",
    birthdate: "1973-03-26",
    address: "Palo Alto, California",
    country: "US",
    study_title: "Stanford PhD"
}),
(:User {
    id: 6,
    name: "Sergey",
    surname: "Brin",
    birthdate: "1973-08-21",
    address: "Palo Alto, California",
    country: "US",
    study_title: "Stanford PhD"
}),
(:User {
    id: 7,
    name: "Jeff",
    surname: "Bezos",
    birthdate: "1964-01-12",
    address: "Seattle, Washington",
    country: "US",
    job: "Founder",
    study_title: "Princeton"
}),
(:User {
    id: 8,
    name: "Sundar",
    surname: "Pichai",
    birthdate: "1972-07-10",
    address: "Mountain View, California",
    country: "US",
    job: "CEO",
    study_title: "IIT Kharagpur"
});

MATCH (a:User {id:1}), (b:User {id:2}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id:2}), (b:User {id:3}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id:3}), (b:User {id:4}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id:4}), (b:User {id:5}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id:5}), (b:User {id:6}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id:1}), (b:User {id:7}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id:7}), (b:User {id:8}) CREATE (a)-[:FOLLOWS]->(b);
MATCH (a:User {id:6}), (b:User {id:8}) CREATE (a)-[:FOLLOWS]->(b);

// PART 2, Slide Task 1 — Given user "Mark", all users whose posts/content he can see transitively
MATCH (a:User {name:"Mark"})-[:FOLLOWS*]->(u)
RETURN DISTINCT u.name AS reachable_user;
