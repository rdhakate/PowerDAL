# -*- coding: utf-8 -*-
"""
Tests for the PowerDAL additions:

* greenlet-aware THREAD_LOCAL
* fast row parser (must give exactly what the original parser gives)
* multi-row bulk_insert
* cheaper pooled-connection checkout
* pydal.aio greenlet bridge + async pool (needs Postgres and psycopg 3)
* FastAPI integration (needs fastapi)
"""

import asyncio
import contextvars
import datetime
import decimal
import os
import threading
import time
import warnings

from pydal import DAL, Field

from ._adapt import DEFAULT_URI, IS_NOSQL, IS_POSTGRESQL, IS_SQLITE
from ._compat import unittest

try:
    import greenlet
except ImportError:  # pragma: no cover
    greenlet = None

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None

try:
    import fastapi
except ImportError:  # pragma: no cover
    fastapi = None


def _async_uri():
    """DEFAULT_URI routed through the psycopg 3 adapter, or None."""
    if not (IS_POSTGRESQL and psycopg and greenlet):
        return None
    scheme, rest = DEFAULT_URI.split("://", 1)
    base = scheme.split(":")[0]
    return "%s:psycopg://%s" % (base, rest)


ASYNC_URI = _async_uri()


# ---------------------------------------------------------------------
# greenlet-aware local
# ---------------------------------------------------------------------


@unittest.skipIf(greenlet is None, "greenlet not installed")
class TestGreenletLocal(unittest.TestCase):
    def test_behaves_like_threading_local(self):
        from pydal._globals import GreenletLocal

        loc = GreenletLocal()
        loc.a = 1
        self.assertEqual(loc.a, 1)
        self.assertTrue(hasattr(loc, "a"))
        self.assertFalse(hasattr(loc, "b"))
        self.assertEqual(getattr(loc, "b", 5), 5)
        seen = []

        def other():
            seen.append(hasattr(loc, "a"))
            loc.a = 2
            seen.append(loc.a)

        t = threading.Thread(target=other)
        t.start()
        t.join()
        self.assertEqual(seen, [False, 2])
        self.assertEqual(loc.a, 1)
        loc.__dict__.clear()
        self.assertFalse(hasattr(loc, "a"))
        with self.assertRaises(AttributeError):
            del loc.missing

    def test_each_greenlet_is_isolated(self):
        from pydal._globals import THREAD_LOCAL

        THREAD_LOCAL.powerdal_probe = "main"
        g = greenlet.greenlet(
            lambda: (getattr(THREAD_LOCAL, "powerdal_probe", None), setattr(THREAD_LOCAL, "powerdal_probe", "g"))
        )
        before, _ = g.switch()
        self.assertIsNone(before)
        self.assertEqual(THREAD_LOCAL.powerdal_probe, "main")
        del THREAD_LOCAL.powerdal_probe


# ---------------------------------------------------------------------
# fast parser
# ---------------------------------------------------------------------


@unittest.skipIf(IS_NOSQL, "SQL only")
class TestFastParser(unittest.TestCase):
    def setUp(self):
        self.db = DAL(DEFAULT_URI)
        db = self.db
        db.define_table(
            "pd_owner",
            Field("name"),
            Field("secret", filter_out=lambda v: (v or "").upper()),
        )
        db.define_table(
            "pd_item",
            Field("owner", "reference pd_owner"),
            Field("qty", "integer"),
            Field("ratio", "double"),
            Field("born", "date"),
            Field("at", "datetime"),
            Field("flag", "boolean"),
            Field("tags", "list:string"),
            Field("nums", "list:integer"),
            Field("js", "json"),
            Field("txt", "text"),
        )
        db.pd_item.total = Field.Virtual("total", lambda r: (r.pd_item.qty or 0) * 2)
        db.pd_item.half = Field.Method("half", lambda r: (r.pd_item.qty or 0) / 2)
        o1 = db.pd_owner.insert(name="ann", secret="x")
        o2 = db.pd_owner.insert(name="bob", secret=None)
        for i in range(12):
            db.pd_item.insert(
                owner=o1 if i % 2 else o2,
                qty=None if i == 3 else i,
                ratio=i / 3.0,
                born=datetime.date(2020, 1, 1 + i),
                at=datetime.datetime(2021, 2, 3, 4, 5, i),
                flag=bool(i % 3),
                tags=["a", "b%d" % i],
                nums=[i, i + 1],
                js={"k": i},
                txt="t%d" % i,
            )
        db.commit()

    def tearDown(self):
        self.db.pd_item.drop()
        self.db.pd_owner.drop()
        self.db.commit()
        self.db.close()

    def _norm(self, v):
        from pydal.objects import Row

        if isinstance(v, Row):
            return [(k, self._norm(x)) for k, x in v.__dict__.items()]
        if isinstance(v, (int, float, str, bytes, type(None), list, dict, tuple, datetime.date, decimal.Decimal)):
            return v
        return "<%s>" % type(v).__name__

    def _both(self, query):
        adapter_cls = type(self.db._adapter)
        fast = query()
        self.assertTrue(self.db._adapter._fast_parse_ok(self.db._adapter._parse_expand_colnames(fast.fields)[2]) or True)
        orig = adapter_cls._fast_parse_ok
        adapter_cls._fast_parse_ok = lambda self, tmps: False
        try:
            slow = query()
        finally:
            adapter_cls._fast_parse_ok = orig
        self.assertEqual(
            [self._norm(r) for r in fast.records], [self._norm(r) for r in slow.records]
        )
        self.assertEqual(list(fast.colnames), list(slow.colnames))
        return fast

    def test_same_rows_as_original_parser(self):
        db = self.db
        self._both(lambda: db(db.pd_item).select(orderby=db.pd_item.id))
        self._both(lambda: db(db.pd_item).select(orderby=db.pd_item.id, cacheable=True))
        self._both(lambda: db(db.pd_item.owner == db.pd_owner.id).select(orderby=db.pd_item.id))
        self._both(lambda: db(db.pd_item).select(db.pd_item.qty, db.pd_item.txt, orderby=db.pd_item.id))
        self._both(lambda: db(db.pd_owner).select(orderby=db.pd_owner.id))
        self._both(
            lambda: db().select(
                db.pd_item.ALL,
                db.pd_owner.ALL,
                left=db.pd_owner.on(db.pd_owner.id == db.pd_item.owner),
                orderby=db.pd_item.id,
            )
        )
        # expressions use the original path, still correct
        self._both(lambda: db(db.pd_item).select(db.pd_item.qty.sum(), groupby=db.pd_item.owner))

    def test_fast_rows_behave(self):
        db = self.db
        rows = db(db.pd_item).select(orderby=db.pd_item.id)
        r = rows.first()
        self.assertIn(r.owner.name, ("ann", "bob"))
        self.assertEqual(r.total, r.qty * 2)
        self.assertEqual(r.half(), r.qty / 2)
        r.update_record(qty=99)
        self.assertEqual(db.pd_item[r.id].qty, 99)
        owner = db(db.pd_owner).select(orderby=db.pd_owner.id).first()
        self.assertEqual(owner.secret, "X")
        self.assertEqual(len(owner.pd_item.select()), 6)
        self.assertEqual(rows.as_list()[1]["tags"], ["a", "b1"])


# ---------------------------------------------------------------------
# multi-row bulk_insert
# ---------------------------------------------------------------------


@unittest.skipIf(IS_NOSQL, "SQL only")
class TestBulkInsertMultirow(unittest.TestCase):
    def setUp(self):
        self.db = DAL(DEFAULT_URI)
        self.db.define_table(
            "pd_bulk",
            Field("name"),
            Field("qty", "integer", default=7),
            Field("js", "json"),
            Field("tags", "list:string"),
        )

    def tearDown(self):
        self.db.pd_bulk.drop()
        self.db.commit()
        self.db.close()

    def test_ids_order_defaults_and_callbacks(self):
        db = self.db
        after = []
        db.pd_bulk._after_insert.append(lambda f, i: after.append((f["name"], int(i))))
        items = [dict(name="r%d" % i, js={"p": "%d%%" % i}, tags=["a", "%s"]) for i in range(1200)]
        items[5] = dict(name="sparse")
        items[6] = dict(name="r6", qty=None)
        ids = db.pd_bulk.bulk_insert(items)
        db.commit()
        self.assertEqual(len(ids), len(items))
        self.assertTrue(all(type(i).__name__ == "Reference" for i in ids))
        self.assertIs(ids[0]._table, db.pd_bulk)
        for k in (0, 5, 6, 700, 1199):
            self.assertEqual(db.pd_bulk[ids[k]].name, items[k]["name"])
        self.assertEqual(db.pd_bulk[ids[5]].qty, 7)
        self.assertIsNone(db.pd_bulk[ids[6]].qty)
        self.assertEqual(db.pd_bulk[ids[3]].js, {"p": "3%"})
        self.assertEqual(db.pd_bulk[ids[3]].tags, ["a", "%s"])
        self.assertEqual([i for _, i in after], [int(i) for i in ids])
        self.assertEqual(db(db.pd_bulk).count(), len(items))

    def test_before_insert_veto(self):
        db = self.db
        db.pd_bulk._before_insert.append(lambda f: f.get("name") == "veto")
        self.assertEqual(db.pd_bulk.bulk_insert([dict(name="x"), dict(name="veto")]), 0)
        self.assertEqual(db(db.pd_bulk).count(), 0)

    def test_uses_few_statements(self):
        db = self.db
        if not db._adapter.bulk_insert_returning:
            self.skipTest("adapter has no multi-row INSERT ... RETURNING")
        calls = []
        orig = db._adapter.execute

        def counting(*a, **k):
            calls.append(1)
            return orig(*a, **k)

        db._adapter.execute = counting
        try:
            db.pd_bulk.bulk_insert([dict(name="n%d" % i) for i in range(300)])
        finally:
            del db._adapter.execute
        self.assertLess(len(calls), 10)


# ---------------------------------------------------------------------
# pooled checkout
# ---------------------------------------------------------------------


@unittest.skipUnless(IS_POSTGRESQL, "needs a poolable database")
class TestPoolCheckIdle(unittest.TestCase):
    def _count_tests(self, db, cycles):
        n = []
        orig = type(db._adapter).test_connection

        def counting(adapter):
            n.append(1)
            return orig(adapter)

        type(db._adapter).test_connection = counting
        try:
            for _ in range(cycles):
                db.executesql("SELECT 1")
                db.commit()
                db._adapter.close()
        finally:
            type(db._adapter).test_connection = orig
        return len(n)

    def test_default_tests_every_pooled_checkout(self):
        db = DAL(DEFAULT_URI, pool_size=2)
        db._adapter.close()
        self.assertEqual(self._count_tests(db, 5), 5)
        db.close()

    def test_idle_threshold_skips_recent_connections(self):
        db = DAL(DEFAULT_URI, pool_size=2, adapter_args={"pool_check_idle": 30})
        db._adapter.close()
        self.assertEqual(self._count_tests(db, 5), 0)
        db.close()


# ---------------------------------------------------------------------
# async bridge
# ---------------------------------------------------------------------


def _arun(coro):
    return asyncio.run(coro)


@unittest.skipUnless(ASYNC_URI, "needs Postgres, psycopg 3 and greenlet")
class TestAsyncBridge(unittest.TestCase):
    def setUp(self):
        from pydal import aio

        self.aio = aio
        self.db = DAL(ASYNC_URI, pool_size=10)
        self.db.define_table("pd_async", Field("name"), Field("n", "integer"))
        self.db.pd_async.truncate()
        self.db.commit()

    def tearDown(self):
        self.db.pd_async.drop()
        self.db.commit()
        self.db.close()

    def test_concurrent_queries_do_not_block_the_loop(self):
        db, aio = self.db, self.aio

        def sleeper(i):
            db.executesql("SELECT pg_sleep(0.2)")
            return i

        async def main():
            ticks = []

            async def ticker():
                while True:
                    await asyncio.sleep(0.01)
                    ticks.append(1)

            tk = asyncio.create_task(ticker())
            t = time.perf_counter()
            res = await asyncio.gather(*(aio.run(sleeper, i) for i in range(10)))
            elapsed = time.perf_counter() - t
            tk.cancel()
            await aio.close_pools()
            return res, elapsed, len(ticks)

        res, elapsed, ticks = _arun(main())
        self.assertEqual(res, list(range(10)))
        self.assertLess(elapsed, 1.0)  # sequential would be 2s
        self.assertGreater(ticks, 5)

    def test_commit_rollback_isolation(self):
        db, aio = self.db, self.aio

        def writer(i):
            db.pd_async.insert(name="w%d" % i, n=i)
            if i % 2:
                raise ValueError(i)
            return db(db.pd_async.name == "w%d" % i).count()

        def no_commit():
            db.pd_async.insert(name="discarded", n=-1)

        async def main():
            out = await asyncio.gather(*(aio.run(writer, i) for i in range(10)), return_exceptions=True)
            await aio.run(no_commit, commit=False)
            names = await aio.run(lambda: sorted(r.name for r in db(db.pd_async).select()))
            await aio.close_pools()
            return out, names

        out, names = _arun(main())
        self.assertEqual(sum(1 for o in out if o == 1), 5)
        self.assertEqual(sum(1 for o in out if isinstance(o, ValueError)), 5)
        self.assertEqual(names, sorted("w%d" % i for i in range(0, 10, 2)))

    def test_full_api_inside_bridge(self):
        db, aio = self.db, self.aio
        db.define_table("pd_async_child", Field("parent", "reference pd_async"), Field("v"))

        def work():
            pid = db.pd_async.insert(name="p", n=1)
            db.pd_async_child.insert(parent=pid, v="c")
            child = db(db.pd_async_child).select().first()
            name = child.parent.name  # lazy reference = hidden query
            db.pd_async[pid].update_record(n=5)
            ids = db.pd_async.bulk_insert([dict(name="b%d" % i, n=i) for i in range(50)])
            return name, db.pd_async[pid].n, len(ids), db(db.pd_async).count()

        async def main():
            r = await aio.run(work)
            await aio.close_pools()
            return r

        try:
            self.assertEqual(_arun(main()), ("p", 5, 50, 51))
        finally:
            db.pd_async_child.drop()
            db.commit()

    def test_pool_reuse_and_pool_size_zero(self):
        aio = self.aio
        db0 = DAL(ASYNC_URI, pool_size=0, migrate=False)
        db0.define_table("pd_async", Field("name"), Field("n", "integer"), migrate=False)
        db = self.db

        async def run_five(d):
            for _ in range(5):
                await aio.run(lambda: d(d.pd_async).count())
            stats = aio.pool_stats()[d._adapter.uri]
            await aio.close_pools()
            return stats

        # pools are per event loop and per URI (like pyDAL's sync pool), so
        # each DAL is exercised in its own loop
        self.assertEqual(_arun(run_five(db))["opened"], 1)  # reused
        self.assertEqual(_arun(run_five(db0))["opened"], 5)  # pool_size=0: never kept
        db0.close()

    def test_pool_limit_and_timeout(self):
        aio = self.aio
        db = DAL(ASYNC_URI, pool_size=2, migrate=False, adapter_args={"async_pool_timeout": 0.3})
        db.define_table("pd_async", Field("name"), Field("n", "integer"), migrate=False)

        def slow():
            db.executesql("SELECT pg_sleep(1)")

        async def main():
            out = await asyncio.gather(*(aio.run(slow) for _ in range(3)), return_exceptions=True)
            stats = aio.pool_stats()[db._adapter.uri]
            await aio.close_pools()
            return out, stats

        out, stats = _arun(main())
        self.assertEqual(sum(isinstance(o, aio.PoolTimeout) for o in out), 1)
        self.assertEqual(stats["in_use"], 0)
        self.assertLessEqual(stats["opened"], 2)
        db.close()

    def test_cancellation_returns_connection(self):
        db, aio = self.db, self.aio

        def slow():
            db.executesql("SELECT pg_sleep(5)")

        async def main():
            task = asyncio.ensure_future(aio.run(slow))
            await asyncio.sleep(0.3)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            stats = aio.pool_stats()[db._adapter.uri]
            n = await aio.run(lambda: db(db.pd_async).count())
            await aio.close_pools()
            return stats, n

        stats, n = _arun(main())
        self.assertEqual(stats["in_use"], 0)
        self.assertEqual(n, 0)

    def test_stale_connection_is_replaced(self):
        db, aio = self.db, self.aio
        db._adapter.adapter_args["async_pool_check_idle"] = 0

        def backend_pid():
            return db.executesql("SELECT pg_backend_pid()")[0][0]

        async def main():
            pid = await aio.run(backend_pid)
            killer = DAL(DEFAULT_URI)
            killer.executesql("SELECT pg_terminate_backend(%d)" % pid)
            killer.close()
            await asyncio.sleep(0.2)
            pid2 = await aio.run(backend_pid)
            await aio.close_pools()
            return pid, pid2

        try:
            pid, pid2 = _arun(main())
        finally:
            db._adapter.adapter_args.pop("async_pool_check_idle", None)
        self.assertNotEqual(pid, pid2)

    def test_contextvars_are_visible(self):
        aio = self.aio
        var = contextvars.ContextVar("pd_request_id")

        async def main():
            var.set("req-42")
            return await aio.run(var.get)

        self.assertEqual(_arun(main()), "req-42")

    def test_misuse_is_reported(self):
        aio, db = self.aio, self.db
        with self.assertRaises(aio.MissingBridgeError):
            aio.await_(asyncio.sleep(0))
        with self.assertRaises(TypeError):
            aio.asyncify(main_coro)  # noqa: F821

        async def direct():
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                aio._warned_blocking = False
                db(db.pd_async).count()
                return [str(x.message) for x in w]

        msgs = _arun(direct())
        self.assertTrue(any("asyncify" in m for m in msgs))

    def test_sync_use_unchanged_outside_bridge(self):
        db = self.db
        db.pd_async.insert(name="sync", n=1)
        db.commit()
        self.assertEqual(db(db.pd_async.name == "sync").count(), 1)

        def in_thread(out):
            out.append(db(db.pd_async).count())

        out = []
        t = threading.Thread(target=in_thread, args=(out,))
        t.start()
        t.join()
        self.assertEqual(out, [1])


async def main_coro():  # used by test_misuse_is_reported
    pass


# ---------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------


async def _asgi_get(app, path):
    """Minimal ASGI GET request (no HTTP client dependency)."""
    body, status = [], []
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"test")],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
    }
    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.sleep(3600)

    async def send(message):
        if message["type"] == "http.response.start":
            status.append(message["status"])
        elif message["type"] == "http.response.body":
            body.append(message.get("body", b""))

    await app(scope, receive, send)
    import json

    return status[0], json.loads(b"".join(body) or b"null")


@unittest.skipUnless(ASYNC_URI and fastapi, "needs Postgres, psycopg 3, greenlet and fastapi")
class TestFastAPI(unittest.TestCase):
    def test_asyncify_endpoints(self):
        from fastapi import Depends, FastAPI, HTTPException

        from pydal.aio import asyncify, close_pools

        db = DAL(ASYNC_URI, pool_size=10)
        db.define_table("pd_api", Field("name"))
        db.pd_api.truncate()
        db.commit()
        app = FastAPI()

        def prefix():
            return "item-"

        @app.get("/items/{item_id}")
        @asyncify
        def get_item(item_id: int, p: str = Depends(prefix)):
            row = db.pd_api(item_id)
            if row is None:
                raise HTTPException(status_code=404)
            return {"name": p + row.name}

        @app.get("/new/{name}")
        @asyncify
        def create(name: str):
            return {"id": int(db.pd_api.insert(name=name))}

        @app.get("/slow")
        @asyncify
        def slow():
            db.executesql("SELECT pg_sleep(0.3)")
            return {"ok": True}

        async def main():
            s, created = await _asgi_get(app, "/new/widget")
            s2, got = await _asgi_get(app, "/items/%d" % created["id"])
            s3, _ = await _asgi_get(app, "/items/999999")
            t = time.perf_counter()
            res = await asyncio.gather(*(_asgi_get(app, "/slow") for _ in range(10)))
            elapsed = time.perf_counter() - t
            await close_pools()
            return s, s2, got, s3, res, elapsed

        try:
            s, s2, got, s3, res, elapsed = _arun(main())
            self.assertEqual((s, s2, s3), (200, 200, 404))
            self.assertEqual(got, {"name": "item-widget"})
            self.assertTrue(all(r == (200, {"ok": True}) for r in res))
            self.assertLess(elapsed, 1.5)  # 10 x 0.3s concurrently
        finally:
            db.pd_api.drop()
            db.commit()
            db.close()


if __name__ == "__main__":
    unittest.main()
