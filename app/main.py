from fastapi import FastAPI
from services.mongo_service import (
    get_person_company,
    mongo_full_join,
    mongo_inner_join,
    mongo_left_join,
    mongo_right_join
)
    

app = FastAPI()
@app.get("/mongo/join")
def mongo_join():
    return get_person_company

@app.get("/mongo/inner")
def mongo_inner():
    return mongo_inner_join()

@app.get("/mongo/full")
def mongo_inner():
    return mongo_full_join()

@app.get("/mongo/right")
def mongo_right():
    return mongo_right_join()

@app.get("/mongo/left")
def mongo_left():
    return mongo_left_join()



from services.neo4j_service import get_reachable_users

@app.get("/neo4j/reachable/{name}")
def reachable(name: str):
    return get_reachable_users(name)