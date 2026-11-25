"""Sync micro-benchmarks for pyDAL. Usage: python bench_sync.py <uri> <label> <pydal_path>"""
import datetime
import json
import sys
import time

uri, label, path = sys.argv[1], sys.argv[2], sys.argv[3]
sys.path.insert(0, path)
from pydal import DAL, Field  # noqa: E402

N = 20000
db = DAL(uri, folder="/tmp", pool_size=5, migrate=True)
db.define_table(
    "bench_thing",
    Field("name"),
    Field("qty", "integer"),
    Field("price", "double"),
    Field("created", "datetime"),
    Field("active", "boolean"),
)
db.define_table("bench_ins", Field("name"), Field("qty", "integer"))
db.bench_thing.truncate()
db.commit()
now = datetime.datetime(2026, 1, 1, 12, 0, 0)
rows = [dict(name="n%d" % i, qty=i, price=i * 1.5, created=now, active=bool(i % 2)) for i in range(N)]
db.bench_thing.bulk_insert(rows)
db.commit()


def best(f, n=5):
    out = []
    for _ in range(n):
        t = time.perf_counter()
        f()
        out.append(time.perf_counter() - t)
    return min(out)


res = {}
res["select_20k_ms"] = best(lambda: db(db.bench_thing).select()) * 1e3
res["select_20k_2cols_ms"] = best(lambda: db(db.bench_thing).select(db.bench_thing.id, db.bench_thing.name)) * 1e3
sql = db(db.bench_thing)._select()
res["executesql_20k_ms"] = best(lambda: db.executesql(sql)) * 1e3


def bulk():
    db.bench_ins.truncate()
    db.bench_ins.bulk_insert([dict(name="x%d" % i, qty=i) for i in range(N)])
    db.commit()


res["bulk_insert_20k_ms"] = best(bulk, 3) * 1e3

K = 2000
t = time.perf_counter()
for i in range(K):
    db(db.bench_thing.id == 77).select().first()
res["single_row_select_us"] = (time.perf_counter() - t) / K * 1e6

t = time.perf_counter()
for i in range(K):
    db((db.bench_thing.qty > 5) & (db.bench_thing.active == True))._select(  # noqa: E712
        db.bench_thing.id, db.bench_thing.name, orderby=db.bench_thing.name, limitby=(0, 10)
    )
res["sql_generation_us"] = (time.perf_counter() - t) / K * 1e6

# request cycle: checkout from pool, one query, commit, return to pool
db._adapter.close()
K2 = 1000
t = time.perf_counter()
for i in range(K2):
    db(db.bench_thing.id == 5).select().first()
    db.commit()
    db._adapter.close()
res["request_cycle_us"] = (time.perf_counter() - t) / K2 * 1e6

# correctness fingerprint so before/after can be compared
r = db(db.bench_thing.id <= 3).select(orderby=db.bench_thing.id)
res["fingerprint"] = repr([(x.id, x.name, x.qty, x.price, x.created, x.active) for x in r])
db.bench_thing.drop()
db.bench_ins.drop()
db.commit()
db.close()
print(json.dumps({label: res}))
