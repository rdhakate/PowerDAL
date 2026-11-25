"""
Non-blocking pyDAL for asyncio servers (FastAPI, Starlette, Quart, ...).

pyDAL's API stays exactly as it is. Ordinary, synchronous pyDAL code is
run inside a *greenlet*; whenever that code reaches the database driver,
the greenlet hands the I/O to the event loop as a coroutine and is
resumed when the result is ready. Lazy references, validators,
callbacks, ``db.table[id]``, ``row.update_record()`` - everything keeps
working, and the event loop is never blocked by the database.

This is the same technique SQLAlchemy uses for its asyncio extension.
It needs the ``greenlet`` package and the ``psycopg`` (v3) driver::

    pip install "pydal-async[async]"

FastAPI example::

    from fastapi import FastAPI
    from pydal import DAL, Field
    from pydal.aio import asyncify, close_pools

    db = DAL("postgres:psycopg://user:pw@localhost/app", pool_size=20,
             migrate=False)
    db.define_table("thing", Field("name"))

    app = FastAPI()

    @app.get("/things/{thing_id}")
    @asyncify                       # the body is plain pyDAL code
    def get_thing(thing_id: int):
        return db.thing[thing_id].as_dict()

Each call wrapped by ``asyncify`` (or awaited through ``run``) is one
unit of work: it gets its own connection from a per-event-loop pool,
commits when the function returns, rolls back if it raises, and gives
the connection back to the pool. As with sync pyDAL, connections are
only kept for reuse when the DAL has ``pool_size > 0``; set it (for
example to 20) in async servers.

Outside ``asyncify``/``run`` the same DAL object works synchronously,
exactly like stock pyDAL (scripts, migrations, sync endpoints, tests).

Limits:

* Only the ``psycopg`` (v3) Postgres adapters are asynchronous today;
  other databases keep working synchronously.
* CPU work (building SQL, parsing rows) still runs on the event-loop
  thread. Keep very large result sets out of latency-sensitive paths.
* Run migrations (``migrate=True``) at start-up, not inside requests:
  they write ``.table`` files with blocking file I/O.
"""

import asyncio
import contextvars
import functools
import time
import warnings
import weakref

from greenlet import getcurrent, greenlet

from ._globals import THREAD_LOCAL

__all__ = [
    "asyncify",
    "run",
    "await_",
    "in_bridge",
    "close_pools",
    "pool_stats",
    "PoolTimeout",
    "MissingBridgeError",
]

# Defaults for the per-event-loop async connection pool. Each can be
# overridden per DAL through ``adapter_args`` (same keys, without the
# leading underscore), e.g. DAL(uri, adapter_args={"async_pool_size": 50}).
DEFAULT_POOL_SIZE = 10  # used when the DAL was created with pool_size=0
DEFAULT_POOL_TIMEOUT = 30.0  # seconds to wait for a free connection
DEFAULT_POOL_CHECK_IDLE = 30.0  # ping connections idle longer than this


class MissingBridgeError(RuntimeError):
    """Async database I/O was attempted outside ``pydal.aio.run``/``asyncify``."""


class PoolTimeout(RuntimeError):
    """No connection became free within the pool timeout."""


# ---------------------------------------------------------------------
# The greenlet bridge
# ---------------------------------------------------------------------


class _BridgeGreenlet(greenlet):
    """A greenlet running sync pyDAL code on behalf of an event-loop task."""

    __slots__ = ()

    def __init__(self, run, parent):
        greenlet.__init__(self, run, parent)
        # adapters holding a pooled connection inside this greenlet
        self._pydal_adapters = []


def in_bridge():
    """True when the caller runs inside ``run``/``asyncify``."""
    return isinstance(getcurrent(), _BridgeGreenlet)


def await_(awaitable):
    """
    Wait for ``awaitable`` from synchronous code running inside the bridge.

    The current greenlet is suspended and the event loop is free to run
    other tasks until the awaitable completes; its result (or exception)
    is then returned (or raised) here, in the synchronous code.
    """
    current = getcurrent()
    if not isinstance(current, _BridgeGreenlet):
        if hasattr(awaitable, "close"):
            awaitable.close()  # avoid "coroutine was never awaited"
        raise MissingBridgeError(
            "This call needs the event loop: run the pyDAL code through "
            "pydal.aio.asyncify (decorator) or 'await pydal.aio.run(fn)'."
        )
    return current.parent.switch(awaitable)


async def _drive(fn, args, kwargs, commit):
    g = _BridgeGreenlet(_entry, getcurrent())
    # sync code sees the task's contextvars (request ids, tracing, ...)
    g.gr_context = contextvars.copy_context()
    result = g.switch(fn, args, kwargs, commit)
    while not g.dead:
        try:
            value = await result
        except BaseException as exc:  # incl. CancelledError: unwind the sync code
            result = g.throw(type(exc), exc, exc.__traceback__)
        else:
            result = g.switch(value)
    return result


def _entry(fn, args, kwargs, commit):
    try:
        result = fn(*args, **kwargs)
    except BaseException:
        _finish(getcurrent(), ok=False, commit=False)
        raise
    _finish(getcurrent(), ok=True, commit=commit)
    return result


def _finish(g, ok, commit):
    """End the unit of work: commit/rollback and return connections to the pool."""
    error = None
    for adapter in list(g._pydal_adapters):
        if ok and commit and error is None:
            try:
                adapter.commit()
            except BaseException as exc:
                # a failed COMMIT must not look like success
                error = exc
        if not (ok and commit) or error is not None:
            try:
                adapter.rollback()
            except BaseException:
                pass
        adapter.close(action=None)
    if error is not None:
        raise error


async def run(fn, *args, commit=True, **kwargs):
    """
    Run synchronous pyDAL code ``fn(*args, **kwargs)`` without blocking the loop.

    On success the work is committed (``commit=False`` rolls it back
    instead, unless ``fn`` called ``db.commit()`` itself); on an
    exception it is rolled back and the exception re-raised. Pooled
    connections are always returned when ``fn`` ends.
    """
    return await _drive(fn, args, kwargs, commit)


def asyncify(fn=None, *, commit=True):
    """
    Decorator turning a synchronous function that uses pyDAL into a
    coroutine function suitable for ``async`` frameworks.

    The wrapper keeps the original signature (FastAPI reads it for
    parameters and dependencies). Use ``@asyncify`` or
    ``@asyncify(commit=False)``.
    """
    if fn is None:
        return lambda f: asyncify(f, commit=commit)
    if asyncio.iscoroutinefunction(fn):
        raise TypeError("asyncify expects a regular (sync) function, got a coroutine function")

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        return await _drive(fn, args, kwargs, commit)

    return wrapper


# ---------------------------------------------------------------------
# DB-API shims over psycopg's AsyncConnection / AsyncCursor
# ---------------------------------------------------------------------


class AsyncCursorProxy(object):
    """Looks like a DB-API cursor to pyDAL; awaits psycopg's AsyncCursor."""

    __slots__ = ("_cur",)

    def __init__(self, cur):
        self._cur = cur

    def execute(self, query, params=None, **kwargs):
        await_(self._cur.execute(query, params, **kwargs))
        return self

    def executemany(self, query, params_seq, **kwargs):
        await_(self._cur.executemany(query, params_seq, **kwargs))
        return self

    def fetchone(self):
        return await_(self._cur.fetchone())

    def fetchmany(self, size=0):
        return await_(self._cur.fetchmany(size))

    def fetchall(self):
        return await_(self._cur.fetchall())

    def close(self):
        return await_(self._cur.close())

    def __iter__(self):
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    def __getattr__(self, name):
        return getattr(self._cur, name)


class AsyncConnectionProxy(object):
    """Looks like a DB-API connection to pyDAL; awaits psycopg's AsyncConnection."""

    __slots__ = ("_conn", "__weakref__")

    def __init__(self, conn):
        self._conn = conn

    def cursor(self, *args, **kwargs):
        return AsyncCursorProxy(self._conn.cursor(*args, **kwargs))

    def commit(self):
        return await_(self._conn.commit())

    def rollback(self):
        return await_(self._conn.rollback())

    def close(self):
        return await_(self._conn.close())

    @property
    def closed(self):
        return self._conn.closed

    @property
    def info(self):
        return self._conn.info

    def __getattr__(self, name):
        return getattr(self._conn, name)


# ---------------------------------------------------------------------
# Per-event-loop async connection pool
# ---------------------------------------------------------------------


class AsyncPool(object):
    """
    Small bounded pool of psycopg AsyncConnections (held as
    ``AsyncConnectionProxy`` objects) for one event loop.

    * at most ``max_size`` connections exist at once; extra requests wait
      (up to ``timeout`` seconds) instead of opening more,
    * idle connections are reused last-in-first-out,
    * a connection idle for more than ``check_idle`` seconds is pinged
      before reuse, so stale connections are dropped without paying a
      round trip on every request.
    """

    def __init__(self, connect, max_size, timeout, check_idle):
        self._connect = connect
        self.max_size = int(max_size)
        self.timeout = timeout
        self.check_idle = check_idle
        self._sem = asyncio.Semaphore(self.max_size)
        self._idle = []
        self.opened = 0

    @property
    def in_use(self):
        return self.max_size - self._sem._value

    async def acquire(self):
        if self._sem.locked():
            try:
                await asyncio.wait_for(self._sem.acquire(), self.timeout)
            except asyncio.TimeoutError:
                raise PoolTimeout(
                    "No database connection free after %ss (pool size %s)"
                    % (self.timeout, self.max_size)
                ) from None
        else:
            await self._sem.acquire()
        try:
            while self._idle:
                conn, since = self._idle.pop()
                if conn.closed:
                    continue
                if self.check_idle is not None and time.monotonic() - since > self.check_idle:
                    try:
                        await conn._conn.execute("SELECT 1")
                        await conn._conn.rollback()
                    except Exception:
                        await _quiet_close(conn)
                        continue
                return conn, False
            conn = await self._connect()
            self.opened += 1
            return conn, True
        except BaseException:
            self._sem.release()
            raise

    async def release(self, conn, reuse=True):
        try:
            if reuse and not conn.closed:
                if conn.info.transaction_status != _IDLE:
                    try:
                        await conn._conn.rollback()
                    except Exception:
                        reuse = False
                if reuse and conn.info.transaction_status == _IDLE:
                    self._idle.append((conn, time.monotonic()))
                    return
            await _quiet_close(conn)
        finally:
            self._sem.release()

    async def close(self):
        idle, self._idle = self._idle, []
        for conn, _ in idle:
            await _quiet_close(conn)


try:
    from psycopg.pq import TransactionStatus as _TS

    _IDLE = _TS.IDLE
except ImportError:  # pragma: no cover
    _IDLE = 0


async def _quiet_close(conn):
    """Close an AsyncConnectionProxy's connection, ignoring errors."""
    try:
        await conn._conn.close()
    except Exception:
        pass


# loop -> {uri: AsyncPool}; entries disappear with their event loop
_POOLS = weakref.WeakKeyDictionary()


def _pool_for(adapter):
    loop = asyncio.get_running_loop()
    pools = _POOLS.get(loop)
    if pools is None:
        pools = _POOLS[loop] = {}
    pool = pools.get(adapter.uri)
    if pool is None:
        args = adapter.adapter_args
        # upper bound on simultaneous connections; with pool_size=0 they are
        # still capped (DEFAULT_POOL_SIZE) but closed after each use
        size = args.get("async_pool_size") or adapter.pool_size or DEFAULT_POOL_SIZE
        timeout = args.get("async_pool_timeout", DEFAULT_POOL_TIMEOUT)
        check_idle = args.get("async_pool_check_idle", DEFAULT_POOL_CHECK_IDLE)
        driver = adapter.driver
        driver_args = dict(adapter.driver_args)

        async def connect():
            return AsyncConnectionProxy(await driver.AsyncConnection.connect(**driver_args))

        pool = pools[adapter.uri] = AsyncPool(connect, size, timeout, check_idle)
    return pool


async def close_pools():
    """Close every idle pooled connection of the running loop (call on shutdown)."""
    pools = _POOLS.pop(asyncio.get_running_loop(), {})
    for pool in pools.values():
        await pool.close()


def pool_stats():
    """{uri: {"size", "in_use", "idle", "opened"}} for the running loop's pools."""
    pools = _POOLS.get(asyncio.get_running_loop(), {})
    return {
        uri: {
            "size": p.max_size,
            "in_use": p.in_use,
            "idle": len(p._idle),
            "opened": p.opened,
        }
        for uri, p in pools.items()
    }


# ---------------------------------------------------------------------
# Hooks called by the async-capable adapters (see backends/postgres.py)
# ---------------------------------------------------------------------


def bridge_get_connection(adapter):
    """Adapter ``get_connection`` inside the bridge: check out from the async pool."""
    uname = adapter._connection_uname_
    conn = getattr(THREAD_LOCAL, uname, None)
    if conn is not None:
        return conn
    pool = _pool_for(adapter)
    # the pool hands out the same proxy object for the same connection
    conn, is_new = await_(pool.acquire())
    setattr(THREAD_LOCAL, uname, conn)
    setattr(THREAD_LOCAL, adapter._cursors_uname_, conn.cursor())
    getcurrent()._pydal_adapters.append(adapter)
    if is_new:
        try:
            adapter.after_connection_hook()
            conn.commit()
        except BaseException:
            bridge_close(adapter, None, True, reuse=False)
            raise
    return conn


def bridge_close(adapter, action="commit", really=True, reuse=True):
    """Adapter ``close`` inside the bridge: run ``action``, return connection to the pool."""
    uname = adapter._connection_uname_
    conn = getattr(THREAD_LOCAL, uname, None)
    if conn is None:
        return
    if action:
        try:
            if callable(action):
                action(adapter)
            else:
                getattr(adapter, action)()
        except Exception:
            reuse = False
    cursor = getattr(THREAD_LOCAL, adapter._cursors_uname_, None)
    if cursor is not None:
        try:
            cursor.close()
        except Exception:
            reuse = False
    setattr(THREAD_LOCAL, uname, None)
    setattr(THREAD_LOCAL, adapter._cursors_uname_, None)
    g = getcurrent()
    try:
        g._pydal_adapters.remove(adapter)
    except ValueError:
        pass
    # same rule as pyDAL's sync pool: pool_size=0 means "do not keep connections"
    if not adapter.pool_size:
        reuse = False
    await_(_pool_for(adapter).release(conn, reuse=reuse))


_warned_blocking = False


def warn_if_blocking():
    """Warn (once) when a sync DB call is made directly on a running event loop."""
    global _warned_blocking
    if _warned_blocking:
        return
    if asyncio._get_running_loop() is not None:
        _warned_blocking = True
        warnings.warn(
            "pyDAL was called directly from a running event loop: the call "
            "blocks the loop and shares one connection between all tasks. "
            "Wrap the code with pydal.aio.asyncify or 'await pydal.aio.run(fn)'.",
            RuntimeWarning,
            stacklevel=4,
        )
