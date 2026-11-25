"""Single-row select latency. Usage: python bench_single_row.py <pydal_checkout_path> <uri> <label>"""
import sys, time
sys.path.insert(0, sys.argv[1])
from pydal import DAL, Field
db = DAL(sys.argv[2], folder="/tmp", pool_size=5)
db.define_table("bench_one", Field("name"), Field("qty", "integer"))
if not db(db.bench_one).count():
    db.bench_one.bulk_insert([dict(name="n%d" % i, qty=i) for i in range(100)]); db.commit()
for _ in range(500): db(db.bench_one.id == 7).select().first()
best = 1e9
for r in range(5):
    t = time.perf_counter()
    for _ in range(3000): db(db.bench_one.id == 7).select().first()
    best = min(best, (time.perf_counter() - t) / 3000 * 1e6)
print("%s %.1f us" % (sys.argv[3], best))
