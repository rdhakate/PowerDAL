"""Async smoke benchmark. Usage: python benchmarks/bench_async_smoke.py postgres:psycopg://user:pw@host/db"""
import asyncio, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pydal import DAL, Field
from pydal.aio import run, asyncify, pool_stats, close_pools, MissingBridgeError

URI = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DB", "postgres:psycopg://postgres:@127.0.0.1/pydal")
db = DAL(URI, pool_size=50, folder="/tmp")
db.define_table("smoke", Field("name"), Field("n", "integer"))
db.smoke.truncate(); db.commit()

def sleeper(i):
    db.executesql("SELECT pg_sleep(0.2)")
    return i

def writer(i):
    db.smoke.insert(name="w%d" % i, n=i)
    if i % 2:
        raise ValueError("boom %d" % i)
    return db(db.smoke.name == "w%d" % i).count()

@asyncify
def read_all():
    return [r.n for r in db(db.smoke).select(orderby=db.smoke.n)]

async def main():
    t = time.perf_counter()
    res = await asyncio.gather(*(run(sleeper, i) for i in range(50)))
    el = time.perf_counter() - t
    print("50 x pg_sleep(0.2) concurrently: %.2fs (sequential would be 10s)" % el, res == list(range(50)))
    # event loop stays responsive while queries run
    ticks = 0
    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01); ticks += 1
    tk = asyncio.create_task(ticker())
    await run(sleeper, 0)
    tk.cancel()
    print("loop ticks during a 0.2s query:", ticks, "(blocked loop would give 0)")
    out = await asyncio.gather(*(run(writer, i) for i in range(20)), return_exceptions=True)
    print("writes ok:", sum(1 for o in out if o == 1), "rolled back:", sum(1 for o in out if isinstance(o, ValueError)))
    print("rows committed:", await read_all())
    print("pool:", pool_stats())
    try:
        from pydal.aio import await_
        await_(asyncio.sleep(0))
    except MissingBridgeError as e:
        print("outside bridge ->", type(e).__name__)
    await close_pools()

asyncio.run(main())
print("sync still works:", db(db.smoke).count())
db.smoke.drop(); db.commit()
