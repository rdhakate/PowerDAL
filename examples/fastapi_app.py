"""
PowerDAL + FastAPI example.

    pip install "powerdal[async]" fastapi uvicorn
    docker compose up -d          # local Postgres (see docker-compose.yml)
    DB=postgres:psycopg://pydal_test:pydal_test@localhost:5432/pydal_test \
        uvicorn examples.fastapi_app:app --reload

Then open http://127.0.0.1:8000/docs
"""

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException

from pydal import DAL, Field
from pydal.aio import asyncify, close_pools, pool_stats

DB_URI = os.environ.get(
    "DB", "postgres:psycopg://pydal_test:pydal_test@localhost:5432/pydal_test"
)

# Ordinary pyDAL setup, done once at import time (synchronously).
# pool_size > 0 lets the async pool keep connections between requests.
db = DAL(DB_URI, pool_size=20, folder=os.environ.get("DAL_FOLDER", "/tmp"))
db.define_table("author", Field("name", notnull=True))
db.define_table(
    "book",
    Field("title", notnull=True),
    Field("author", "reference author"),
    Field("pages", "integer"),
)
db.commit()


@asynccontextmanager
async def lifespan(app):
    yield
    await close_pools()  # close idle pooled connections on shutdown


app = FastAPI(title="PowerDAL example", lifespan=lifespan)


def page_size(limit: int = 20):
    return min(limit, 100)


@app.post("/authors/{name}")
@asyncify  # plain pyDAL code; commits on success, rolls back on error
def create_author(name: str):
    return {"id": int(db.author.insert(name=name))}


@app.post("/books")
@asyncify
def create_book(title: str, author_id: int, pages: int = 0):
    if db.author(author_id) is None:
        raise HTTPException(status_code=404, detail="author not found")
    return {"id": int(db.book.insert(title=title, author=author_id, pages=pages))}


@app.get("/books/{book_id}")
@asyncify
def get_book(book_id: int):
    book = db.book(book_id)
    if book is None:
        raise HTTPException(status_code=404)
    # book.author.name is a lazy reference: a hidden query, still non-blocking
    return {"title": book.title, "author": book.author.name, "pages": book.pages}


@app.get("/books")
@asyncify
def list_books(limit: int = Depends(page_size)):
    rows = db(db.book).select(orderby=~db.book.id, limitby=(0, limit))
    return rows.as_list()


@app.get("/stats")
async def stats():
    return pool_stats()
