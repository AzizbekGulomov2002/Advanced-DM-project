-- =============================================================================
-- Advanced Data Management project (Slide_ADM(3).pdf)
-- This file implements the RELATIONAL (RDBMS) side of the assignment.
--
-- PART 1 — Comparing relational vs document models:
--   Schema + sample data: PERSON, COMPANY, WORK_IN (slide conceptual model).
--   Queries below map to Part 1 Tasks 1–4 (SQL join variants).
--   Extra aggregate queries at end of Part 1: supporting analytics only (not
--   separate numbered tasks on the slide).
--
-- PART 2 — Comparing relational vs graph models:
--   Schema + sample data: USER, FOLLOW (relational side of the social network).
--   Graph queries live in db/neo4j.cypher; Spark graph jobs read these tables.
-- =============================================================================

-- PART 1 — Relational schema: PERSON, COMPANY, WORK_IN (slide entity classes)
CREATE TABLE person(
	id SERIAL PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	surname VARCHAR(100) NOT NULL,
	birthdate DATE,
	address TEXT,
	country VARCHAR(100)
);

CREATE TABLE company(
	id SERIAL PRIMARY KEY,
	name VARCHAR(200) NOT NULL,
	founded DATE,
	sector VARCHAR(50)
);

CREATE TABLE works_in(
	person_id INTEGER REFERENCES person(id),
	company_id INTEGER REFERENCES company(id),
	PRIMARY KEY(person_id, company_id)
);

-- Sectors must match assignment: automotive, banking, services, healthcare, chemicals, public
INSERT INTO company (name, sector, founded)
VALUES
('Google',   'services',   '1998-09-04'),
('Amazon',   'services',   '1994-07-05'),
('Meta',     'services',   '2004-02-04'),
('Tesla',    'automotive', '2003-07-01'),
('OpenAI',   'services',   '2015-12-11');


INSERT INTO person (name, surname, birthdate, address, country)
VALUES
('Mark','Zuckerberg','1984-10-10','California','US'),
('Pavel','Durov','1983-10-10','Moscow','Russia'),
('Sam','Altman','1985-04-22','California','US'),
('Elon','Musk','1971-06-28','Texas','US'),
('Larry','Page','1973-03-26','California','US'),
('Sergey','Brin','1973-08-21','California','US');


INSERT INTO works_in (person_id, company_id)
VALUES
(1,3),  -- Mark → Meta
(2,3),  -- Pavel → Meta
(3,5),  -- Sam → OpenAI
(4,4),  -- Elon → Tesla
(5,1),  -- Larry → Google
(6,1),  -- Sergey → Google
(3,1);  -- Sam → Google (multi-company)

-- PART 1, Slide Task 1 — All employed persons + company they work in (INNER JOIN)
SELECT 
	p.name,
	p.surname,
	c.name AS company_name,
	c.sector
FROM person p
JOIN works_in w ON p.id = w.person_id
JOIN company c ON w.company_id = c.id;

-- PART 1, Slide Task 2 — All persons + company information (if any) (LEFT JOIN)
SELECT 
    p.name, 
    p.surname, 
    c.name AS company_name
FROM person p
LEFT JOIN works_in w ON p.id = w.person_id
LEFT JOIN company c ON w.company_id = c.id;

-- PART 1, Slide Task 3 — All companies + persons they employ (if any)
-- Base table must be company so employers with zero employees still appear.
SELECT
	p.name,
	p.surname,
	c.name AS company_name
FROM company c
LEFT JOIN works_in w ON c.id = w.company_id
LEFT JOIN person p ON w.person_id = p.id;


-- PART 1, Slide Task 4 — All persons and companies; match employed persons to companies
-- Chained FULL OUTER on person->works->company loses correct NULL pairing; use persons+orphan companies.
WITH employed AS (
	SELECT p.name, p.surname, c.name AS company_name
	FROM person p
	LEFT JOIN works_in w ON p.id = w.person_id
	LEFT JOIN company c ON w.company_id = c.id
),
orphan_companies AS (
	SELECT NULL::varchar AS name, NULL::varchar AS surname, c.name AS company_name
	FROM company c
	WHERE NOT EXISTS (SELECT 1 FROM works_in w WHERE w.company_id = c.id)
)
SELECT name, surname, company_name FROM employed
UNION ALL
SELECT name, surname, company_name FROM orphan_companies;

-- PART 1 — Supplementary (not a separate slide task): person ↔ company counts
-- All persons with number of companies
SELECT
	p.name,
	COUNT(w.company_id) AS companies_count
FROM person p
LEFT JOIN works_in w ON p.id = w.person_id
GROUP by p.name;

-- All companies with number of persons
SELECT 
	c.name,
	COUNT(w.company_id) AS employees_count
FROM company c
LEFT JOIN works_in w ON c.id = w.company_id
GROUP by c.name;



-- =============================================================================
-- PART 2 — Relational side: USER + FOLLOW (Slide_ADM(3).pdf Part 2)
-- Implements the slide USER entity (name, surname, birthdate, address, country,
-- optional job, optional study_title) and FOLLOW relationships.
-- GDBMS / Spark programs use Neo4j + Spark; they load from these tables where noted.
-- =============================================================================

CREATE TABLE users (
    id      SERIAL PRIMARY KEY,
    name    VARCHAR(100) NOT NULL,
    surname VARCHAR(100) NOT NULL,
    birthdate DATE,
    address TEXT,
    country VARCHAR(100),
    job     VARCHAR(100),
    study_title VARCHAR(100)
);

CREATE TABLE follows (
    follower_id INTEGER REFERENCES users(id),
    followee_id INTEGER REFERENCES users(id),
    PRIMARY KEY (follower_id, followee_id)
);

INSERT INTO users (name, surname, birthdate, address, country, job, study_title)
VALUES
('Mark','Zuckerberg','1984-10-10','Palo Alto, California','US','CEO','Harvard dropout'),
('Pavel','Durov','1984-10-10', 'Dubai','UAE', 'CEO','Saint Petersburg State'),
('Sam','Altman','1985-04-22', 'San Francisco, California','US','CEO','Stanford'),
('Elon','Musk','1971-06-28', 'Austin, Texas','US','CEO','Penn/Stanford'),
('Larry','Page','1973-03-26', 'Palo Alto, California','US',  NULL, 'Stanford PhD'),
('Sergey', 'Brin','1973-08-21', 'Palo Alto, California','US',NULL,'Stanford PhD'),
('Jeff','Bezos','1964-01-12', 'Seattle, Washington','US','Founder','Princeton'),
('Sundar','Pichai','1972-07-10','Mountain View, California','US','CEO','IIT Kharagpur');

INSERT INTO follows (follower_id, followee_id)
VALUES
(1, 2),
(2, 3),
(3, 4),
(4, 5),
(5, 6),
(1, 7),
(7, 8),
(6, 8);