"""Run pyDAL's whole unittest suite inside the pydal.aio greenlet bridge,
so every database call of every test goes through psycopg's AsyncConnection.

Usage (from the repository root):
    DB=postgres:psycopg://user:pw@localhost/pydal_test python tools/run_suite_in_bridge.py
"""
import asyncio, os, sys, unittest
sys.path.insert(0, os.getcwd())
from pydal import aio

# inspects the *sync* pool's internal POOLS dict, which async mode does not use;
# the async pool has its own tests (tests/powerdal.py)
SYNC_POOL_INTERNALS = {
    "tests.sql.TestConnection.testRun",
    "tests.powerdal.TestPoolCheckIdle.test_default_tests_every_pooled_checkout",
    "tests.powerdal.TestPoolCheckIdle.test_idle_threshold_skips_recent_connections",
}
# start their own event loops, so they cannot run inside this one
OWN_LOOP = ("tests.powerdal.TestAsyncBridge", "tests.powerdal.TestFastAPI")

def flatten(s):
    for t in s:
        if isinstance(t, unittest.TestSuite):
            yield from flatten(t)
        else:
            yield t

def main():
    suite = unittest.defaultTestLoader.loadTestsFromName("tests")
    keep = [t for t in flatten(suite) if t.id() not in SYNC_POOL_INTERNALS and not t.id().startswith(OWN_LOOP)]
    print("excluded:", sorted(SYNC_POOL_INTERNALS), "+ async/FastAPI tests (run separately)")
    return unittest.TextTestRunner(verbosity=1).run(unittest.TestSuite(keep))

async def go():
    calls = {"n": 0}
    orig = aio.AsyncCursorProxy.execute
    def counting(self, *a, **k):
        calls["n"] += 1
        return orig(self, *a, **k)
    aio.AsyncCursorProxy.execute = counting
    res = await aio.run(main, commit=False)
    print("statements executed through AsyncConnection:", calls["n"])
    print("pools:", aio.pool_stats())
    return res

res = asyncio.run(go())
sys.exit(0 if res.wasSuccessful() else 1)
