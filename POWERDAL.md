# PowerDAL

**PowerDAL is a drop-in fork of [pyDAL](https://github.com/web2py/pydal)
that adds a non-blocking asyncio mode for FastAPI and other async
servers, plus speed optimisations. Existing pyDAL code keeps working
unchanged.**

Published by **Gentlemans Digital Products** — *Gentleman's ebooks &
digital products* — <https://gentlemansebooks.com>.
Based on pyDAL `20261107.0` by Massimo Di Pierro and contributors
(BSD 3-clause; see `LICENSE.txt` and `NOTICE.md`).

---

## What changed, in one table

| Area | Change | Default behaviour |
|---|---|---|
| Async (`pydal.aio`) | Run ordinary pyDAL code without blocking the event loop, via a greenlet bridge and psycopg 3's async connection | Opt-in: only code wrapped with `asyncify` / `await run(...)` |
| psycopg 3 | New `postgres:psycopg`, `postgres2:psycopg`, `postgres3:psycopg` adapters (sync and async) | psycopg2 stays preferred when both drivers are installed |
| Row parser | Per-query precompiled column converters; identical `Row`/`Rows` output | On; automatic fallback to the original parser for unusual cases |
| `bulk_insert` | Multi-row `INSERT ... RETURNING id` in batches (Postgres, SQLite ≥ 3.35) | On; same return value and callbacks; row-by-row fallback |
| Pool checkout | Optional idle threshold to skip the per-checkout `SELECT 1` + `COMMIT` | Off (`pool_check_idle=0`), so classic behaviour is kept |
| Thread-local state | `THREAD_LOCAL` is per-greenlet when `greenlet` is installed | Identical to `threading.local` for threaded code |

The import name is still **`pydal`**, so `from pydal import DAL, Field`
works as before. Because the import name is shared, install PowerDAL
*instead of* `pydal`, not next to it (`pip uninstall pydal` first).
`pydal.__version__` still reports the upstream pyDAL release
(`20261107.0`) so frameworks that check it keep working;
`pydal.__powerdal_version__` gives the PowerDAL release (`1.0.0`).

---

## Installation

```bash
pip uninstall -y pydal            # the import name is shared
pip install "powerdal[async]"     # sync + async (greenlet + psycopg 3)
# or
pip install powerdal              # sync only, no extra dependencies
```

Until it is published on PyPI, install from GitHub:

```bash
pip install "powerdal[async] @ git+https://github.com/rdhakate/PowerDAL.git@main"
```

| Extra | Installs | For |
|---|---|---|
| *(none)* | nothing | sync pyDAL, exactly as before |
| `async` | `greenlet>=3.0`, `psycopg[binary]>=3.1` | `pydal.aio` with FastAPI/Starlette/Quart |
| `psycopg` | `psycopg[binary]>=3.1` | psycopg 3 for sync use |
| `test` | the above plus `psycopg2-binary`, `fastapi`, `legacy-cgi` | running the test suite |

Requirements: Python ≥ 3.10. The async mode needs PostgreSQL (any
supported version) and psycopg 3.

---

## Using it with FastAPI

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydal import DAL, Field
from pydal.aio import asyncify, close_pools

db = DAL("postgres:psycopg://user:pw@localhost/app", pool_size=20)
db.define_table("thing", Field("name"))        # at import time, as usual

@asynccontextmanager
async def lifespan(app):
    yield
    await close_pools()

app = FastAPI(lifespan=lifespan)

@app.get("/things/{thing_id}")
@asyncify                                      # body is plain pyDAL code
def get_thing(thing_id: int):
    row = db.thing(thing_id)
    if row is None:
        raise HTTPException(404)
    return row.as_dict()
```

A complete app is in `examples/fastapi_app.py`.

### Rules of the async mode

* **One unit of work per call.** Each `asyncify`-wrapped call (or
  `await pydal.aio.run(fn, ...)`) gets its own connection from a
  per-event-loop pool. It commits when the function returns and rolls
  back if it raises. `@asyncify(commit=False)` or `run(fn, commit=False)`
  rolls back unless the code calls `db.commit()` itself.
* **Everything in pyDAL works inside**: queries, `db.table[id]`, lazy
  references (`row.author.name`), `update_record`, validators, callbacks,
  `bulk_insert`, `executesql`.
* **Set `pool_size`.** As with sync pyDAL, connections are only kept for
  reuse when `pool_size > 0`. `pool_size` is also the cap on simultaneous
  connections per event loop (default 10 when `pool_size=0`).
* **Outside the bridge, the same `db` is plain sync pyDAL**: scripts,
  migrations, sync `def` endpoints in FastAPI's threadpool, and tests.
* **Don't call pyDAL directly inside `async def`** without `asyncify` or
  `run`. It would block the loop and share one connection between all
  tasks, so PowerDAL emits a one-time `RuntimeWarning` when it detects
  this.
* **Run migrations at start-up**, not inside requests: they write
  `.table` files with blocking file I/O. Use `migrate=False` in
  production servers if you can.

### `pydal.aio` API

| Name | What it does |
|---|---|
| `asyncify(fn)` / `asyncify(commit=False)` | Decorator: sync function → coroutine function; keeps the signature for FastAPI |
| `await run(fn, *args, commit=True, **kw)` | Run sync pyDAL code as one non-blocking unit of work |
| `await_(awaitable)` | From sync code inside the bridge, wait for any awaitable |
| `in_bridge()` | True inside `asyncify`/`run` |
| `await close_pools()` | Close the running loop's idle pooled connections (shutdown) |
| `pool_stats()` | `{uri: {size, in_use, idle, opened}}` for the running loop |
| `PoolTimeout`, `MissingBridgeError` | Exceptions |

### Async pool settings (`adapter_args`)

| Key | Default | Meaning |
|---|---|---|
| `async_pool_size` | `pool_size` or 10 | Max simultaneous connections per event loop |
| `async_pool_timeout` | 30 s | Wait this long for a free connection, then raise `PoolTimeout` |
| `async_pool_check_idle` | 30 s | Ping a pooled connection before reuse if idle longer than this |

Example: `DAL(uri, pool_size=20, adapter_args={"async_pool_timeout": 5})`.

---

## psycopg 3 adapter

* `postgres:psycopg://user:pw@host/db` selects it explicitly. A plain
  `postgres://` URI keeps using psycopg2 when it is installed, and falls
  back to psycopg 3 when it is the only driver.
* The flavours `postgres2:psycopg` (native arrays) and
  `postgres3:psycopg` (native arrays and booleans) are also available.
* Automatic server-side prepared statements are **off** by default, like
  psycopg2. They break after `ALTER TABLE` ("cached plan must not change
  result type") and behind PgBouncer in transaction mode. Opt in with
  `driver_args={"prepare_threshold": 5}`.

---

## Performance

Measured on one machine against a local PostgreSQL 16 (no network
latency) and SQLite. Each figure is the best of 5 runs, for the same
data before and after. Results files are in `benchmarks/`.

| Operation | DB | pyDAL 20261107.0 | PowerDAL 1.0.0 | Change |
|---|---|---|---|---|
| `select()` 20,000 rows × 6 columns | SQLite | 200.8 ms | 78.2 ms | 2.6x faster |
| `select()` 20,000 rows × 6 columns | Postgres (psycopg2) | 192.0 ms | 96.9 ms | 2.0x faster |
| `select()` 20,000 rows × 2 columns | Postgres (psycopg2) | 120.7 ms | 52.1 ms | 2.3x faster |
| `bulk_insert` 20,000 rows | Postgres (psycopg2) | 2,434 ms | 478 ms | 5.1x faster |
| `bulk_insert` 20,000 rows | SQLite | 453 ms | 407 ms | 1.1x faster |
| single-row select | Postgres (psycopg2) | ~180 µs | ~170 µs | same (within noise) |

* The `bulk_insert` gain grows with network latency, because rows go
  in a few statements instead of one round trip each.
* The async bridge adds about **0.75 µs per database call** and about
  **6 µs per unit of work**, against a typical database round trip of
  hundreds of microseconds.
* **Concurrency:** 50 concurrent requests each running
  `SELECT pg_sleep(0.2)` finish in about 0.3 s through `pydal.aio`
  (sequential: 10 s), and the event loop keeps serving other tasks
  meanwhile.
* Async mode does not make an individual query faster. It lets one
  process serve many requests that are waiting on the database.

Re-run the benchmarks with:

```bash
python benchmarks/bench_sync.py "sqlite:///tmp/b.sqlite" label .
python benchmarks/bench_sync.py "postgres://user:pw@localhost/db" label .
python benchmarks/bench_async_smoke.py "postgres:psycopg://user:pw@localhost/db"
```

---

## Compatibility

* **pyDAL's own test suite passes unchanged** in four configurations:
  SQLite, PostgreSQL with psycopg2, PostgreSQL with psycopg 3 (sync),
  and the *entire suite run through the async bridge* (every statement
  sent over psycopg's `AsyncConnection`).
* The one upstream test not run through the bridge
  (`tests.sql.TestConnection.testRun`) inspects the sync pool's internal
  `POOLS` dictionary, which async mode does not use. The async pool has
  its own tests in `tests/powerdal.py`.
* The fast parser is used only when every column is a plain table field
  and no adapter, parser or `Row` hook has been overridden. Otherwise
  the original parser runs, so custom adapters and subclasses behave
  exactly as before.
* `bulk_insert` falls back to row-by-row for keyed tables
  (`primarykey=`), tables with `_on_insert_error`, items that set `id`
  explicitly, and statements that are not fully parameterised.
  Execution hooks and `db._timings` see one statement per batch, not one
  per row.
* `THREAD_LOCAL` is greenlet-local when `greenlet` is installed. For
  threaded code, and for gevent (which already patches
  `threading.local`), this is the same as before.

### Known limits

* Only the psycopg 3 PostgreSQL adapters are asynchronous. SQLite,
  MySQL, MSSQL, Oracle and the rest work synchronously as before.
* CPU work (building SQL, parsing rows) still runs on the event-loop
  thread. Very large result sets will hold the loop for their parse time
  (now about 2.5x shorter).
* psycopg2 users get the parser and `bulk_insert` speed-ups but not the
  async mode. Async needs psycopg 3.

---

## Running the tests

```bash
git clone https://github.com/rdhakate/PowerDAL.git && cd PowerDAL
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -e ".[test]"
docker compose up -d                                  # local PostgreSQL 16

python -m unittest tests                                                          # SQLite
DB=postgres://pydal_test:pydal_test@localhost:5432/pydal_test python -m unittest tests         # psycopg2
DB=postgres:psycopg://pydal_test:pydal_test@localhost:5432/pydal_test python -m unittest tests # psycopg 3
DB=postgres:psycopg://pydal_test:pydal_test@localhost:5432/pydal_test python tools/run_suite_in_bridge.py  # async bridge
```

The new features' tests alone: `python -m unittest tests.powerdal`.
GitHub Actions runs all of the above on Python 3.10–3.13
(`.github/workflows/powerdal.yml`).

---

## Files added or changed

| File | What |
|---|---|
| `pydal/aio.py` | **New.** Greenlet bridge, DB-API shims over psycopg's async API, per-loop async pool, `asyncify`/`run` |
| `pydal/_globals.py` | `GreenletLocal`; `THREAD_LOCAL` uses it when greenlet is installed |
| `pydal/base.py` | `_aliased_tables` uses the same local |
| `pydal/drivers.py` | Registers the `psycopg` (v3) driver |
| `pydal/backends/postgres.py` | psycopg 3 adapters, psycopg2-compatible `adapt`, async hooks, `bulk_insert_returning` |
| `pydal/backends/sqlite.py` | Multi-row `bulk_insert` on SQLite ≥ 3.35 |
| `pydal/compilers/postgres.py` | Parameterised compiler for the psycopg 3 adapters |
| `pydal/backend_base.py` | Fast row parser, multi-row `bulk_insert` |
| `pydal/connection.py` | Optional `pool_check_idle` threshold |
| `pydal/__init__.py` | `__powerdal_version__` |
| `tests/powerdal.py` | **New.** 20 tests: local, parser equivalence, bulk insert, pool, async bridge, FastAPI |
| `tools/run_suite_in_bridge.py` | **New.** Runs the whole suite through the async bridge |
| `benchmarks/` | **New.** Benchmark scripts and results |
| `examples/fastapi_app.py` | **New.** Example FastAPI app |
| `docker-compose.yml` | **New.** Local PostgreSQL for tests |
| `.github/workflows/powerdal.yml` | **New.** CI for all test configurations |
| `pyproject.toml` | Distribution renamed `powerdal`; `async`, `psycopg`, `test` extras |
| `uv.lock` | Removed (it pinned the old `pydal` name; regenerate with `uv lock`) |
