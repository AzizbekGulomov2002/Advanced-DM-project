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

INSERT INTO company (name, sector, founded)
VALUES 
('Google','Tech','1998-09-04'),
('Amazon','E-commerce','1994-07-05'),
('Meta','Social','2004-02-04'),
('Tesla','Automotive','2003-07-01'),
('OpenAI','AI','2015-12-11');


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

-- INNER JOIN (All of Work in persons)
SELECT 
	p.name,
	p.surname,
	c.name AS company_name,
	c.sector
FROM person p
JOIN works_in w ON p.id = w.person_id
JOIN company c ON w.company_id = c.id;

-- LEFT JOIN (All of objects from left table: persons)
SELECT 
    p.name, 
    p.surname, 
    c.name AS company_name
FROM person p
LEFT JOIN works_in w ON p.id = w.person_id
LEFT JOIN company c ON w.company_id = c.id;

-- RIGHT JOIN (All of objects from right table: companies)
SELECT 
	p.name,
	p.surname,
	c.name AS company_name
FROM person p
RIGHT JOIN works_in w ON p.id = w.person_id
RIGHT JOIN company c ON w.company_id = c.id;


-- FULL OUTER JOIN (All of object from both side tables)
SELECT 
	p.name, 
	p.surname,
	c.name AS company_name
FROM person p
FULL OUTER JOIN works_in w ON p.id=w.person_id
FULL OUTER JOIN company c ON w.company_id = c.id;

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
RIGHT JOIN works_in w ON c.id = w.company_id
GROUP by c.name;
