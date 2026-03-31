# Advanced Data Management - Documentation

## 1. Maqsad
Bu hujjat `Slide_ADM(3).pdf` da berilgan project task bo'yicha loyihada qanday jarayonlar bajarilgani va tizim qanday ishlashini tushuntiradi.

## 2. Ishlatilgan texnologiyalar
- Python
- FastAPI
- PostgreSQL (Relational)
- MongoDB (Document)
- Neo4j (Graph)
- PySpark (dependency mavjud)

## 3. Task bo'yicha jarayonlar

## Part 1 - Relational va Document model solishtiruvi
### 3.1 Relational model (PostgreSQL)
`db/postgres.sql` faylida:
- `person` jadvali yaratildi
- `company` jadvali yaratildi
- `works_in` bog'lovchi jadvali yaratildi
- test ma'lumotlar kiritildi
- quyidagi query turlari yozildi:
  - `INNER JOIN`
  - `LEFT JOIN`
  - `RIGHT JOIN`
  - `FULL OUTER JOIN`

Bu qism orqali topshiriqdagi person-company bog'lanishlari relational modelda ifodalangan.

### 3.2 Document model (MongoDB)
`db/mongo_seed.py` faylida:
- `persons`, `companies`, `works` kolleksiyalariga seed ma'lumotlar yoziladi.

`services/mongo_service.py` faylida:
- `mongo_inner_join()`
- `mongo_left_join()`
- `mongo_right_join()`
- `mongo_full_join()`

Ushbu metodlar MongoDB aggregation pipeline (`$lookup`, `$unwind`, `$project`) orqali relational join mantiqlarini emulyatsiya qiladi.

### 3.3 API orqali natijani olish
`app/main.py` da endpointlar ochilgan:
- `/mongo/inner`
- `/mongo/left`
- `/mongo/right`
- `/mongo/full`

Foydalanuvchi API'ga so'rov yuborib join natijalarini JSON ko'rinishida oladi.

## Part 2 - Relational va Graph model solishtiruvi
### 3.4 Graph model (Neo4j)
`db/neo4j.cypher` faylida:
- `User` node lar yaratildi
- `FOLLOWS` relationship lar yaratildi
- transitive reachable query yozildi (`[:FOLLOWS*]`)

`services/neo4j_service.py` da:
- `get_reachable_users(name)` funksiyasi berilgan userdan tranzitiv tarzda yetib boriladigan userlarni topadi.

`app/main.py` endpoint:
- `/neo4j/reachable/{name}`

Bu qism topshiriqdagi "berilgan user ko'ra oladigan boshqa userlar kontenti" g'oyasini graph traversal orqali bajaradi.

## 4. Ishga tushirish ketma-ketligi
1. `pip install -r requirements.txt`
2. PostgreSQL'da `db/postgres.sql` ni ishga tushirish
3. MongoDB seed: `python db/mongo_seed.py`
4. Neo4j'da `db/neo4j.cypher` ni ishga tushirish
5. API start: `uvicorn app.main:app --reload`

## 5. Arxitektura qisqacha
- `db/`: data model va seed scriptlar
- `services/`: DB query/business logic
- `app/`: FastAPI endpointlar
- `spark_jobs/`: Spark bo'yicha joy ajratilgan modul

## 6. Taskga moslik bo'yicha eslatma
- Relational, Document va Graph bo'limlari bo'yicha asosiy query/endpointlar mavjud.
- Spark bo'yicha papka mavjud, ammo ushbu repoda Spark implementatsiya kodlari hali to'ldirilmagan.

## 7. Yakuniy izoh
Loyiha taskdagi asosiy maqsadni - turli ma'lumotlar modellarida bog'lanishlarni ifodalash va so'rovlarni solishtirishni - amaliy kod orqali ko'rsatadi.

---
Azizbek Gulomov tomonidan bajarilgan.
