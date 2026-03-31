# Advanced Data Management Project

## Loyiha haqida
Ushbu loyiha `Advanced Data Management` fani uchun tayyorlangan bo'lib, berilgan topshiriq (`Slide_ADM(3).pdf`) asosida uch xil ma'lumotlar modeli amaliy ko'rinishda ishlangan:

- `Relational model` - PostgreSQL
- `Document model` - MongoDB
- `Graph model` - Neo4j

Loyihaning asosiy maqsadi: bir xil yoki o'xshash bog'lanishli ma'lumotlar ustida turli DBMS yondashuvlarini solishtirish.

## Slide bo'yicha bajarilgan qismlar

### 1) Relational vs Document (Part 1)
- `db/postgres.sql` ichida `person`, `company`, `works_in` jadvallari yaratildi.
- SQL orqali `INNER`, `LEFT`, `RIGHT`, `FULL OUTER JOIN` so'rovlari yozildi.
- MongoDB uchun `db/mongo_seed.py` orqali ma'lumotlar seed qilinadi.
- `services/mongo_service.py` ichida join mantiqlari aggregation (`$lookup`) orqali yozilgan.
- FastAPI endpointlari orqali Mongo join natijalari olinadi:
  - `/mongo/inner`
  - `/mongo/left`
  - `/mongo/right`
  - `/mongo/full`

### 2) Relational vs Graph (Part 2)
- `db/neo4j.cypher` ichida `User` tugunlari va `FOLLOWS` bog'lanishlari yaratildi.
- Transitiv kuzatuv (`FOLLOWS*`) query'si berilgan.
- `services/neo4j_service.py` va `/neo4j/reachable/{name}` endpointi orqali berilgan user uchun reachable userlar qaytariladi.

### 3) Spark qismi
- `spark_jobs/` papkasi tayyorlangan (`dataframe_job.py`, `rdd_job.py`).
- Hozirgi repoda Spark implementatsiya fayllari bo'sh holatda.

## Loyihani ishga tushirish

## 1. Kutubxonalarni o'rnatish
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 2. PostgreSQL
- `db/postgres.sql` ni ishga tushiring.

## 3. MongoDB
```bash
python db/mongo_seed.py
```

## 4. Neo4j
- `db/neo4j.cypher` skriptini Neo4j Browser'da run qiling.

## 5. API
```bash
uvicorn app.main:app --reload
```

## Endpointlar
- `GET /mongo/inner`
- `GET /mongo/left`
- `GET /mongo/right`
- `GET /mongo/full`
- `GET /neo4j/reachable/{name}`

## Qisqa xulosa
Loyiha topshiriqdagi ma'lumotlar modellarini amaliy ko'rinishda ishlatadi va relational/document/graph yondashuvlarining query yozish uslubidagi farqlarini ko'rsatadi.

---
Azizbek Gulomov tomonidan bajarilgan.
