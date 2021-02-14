from typing import List
from fastapi import FastAPI, status
from pydantic import BaseModel
import databases
import sqlalchemy
import datetime

DATABASE_URL = "postgresql://postgres:root@localhost:5432/User"
database = databases.Database(DATABASE_URL)


metadata = sqlalchemy.MetaData()

User = sqlalchemy.Table(
    "User",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.String, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
)


engine = sqlalchemy.create_engine(
    DATABASE_URL, pool_size=3, max_overflow=0
)
metadata.create_all(engine)


class UserIn(BaseModel):
    id: str
    name: str


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/users", response_model=List[UserIn], status_code=status.HTTP_200_OK)
async def get_users():
    query = User.select()
    return await database.fetch_all(query)


@app.get("/users/{user_id}", response_model=UserIn, status_code=status.HTTP_200_OK)
async def get_user(user_id: str):
    query = User.select().where(User.c.id == user_id)
    return await database.fetch_one(query)


@app.post("/users/", response_model=UserIn, status_code=status.HTTP_201_CREATED)
async def create_user(user: UserIn):
    now = datetime.datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    new_id = user.name + dt_string
    query = User.insert().values(id=new_id, name=user.name)
    last_record_id = await database.execute(query)
    return {**user.dict(), "id": last_record_id}


@app.put("/users/{user_id}/", response_model=UserIn, status_code=status.HTTP_200_OK)
async def update_user(user_id: str, payload: UserIn):
    query = User.update().where(User.c.id == user_id).values(name=payload.name)
    await database.execute(query)
    return {**payload.dict(), "id": user_id}


@app.delete("/users/{user_id}/")
async def delete_user(user_id: str):
    query = User.delete().where(User.c.id == user_id)
    rsp = await database.execute(query)
    return {"message": "User with id: {} deleted successfully!".format(user_id)}
