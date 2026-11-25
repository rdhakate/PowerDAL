"""
Process-wide globals.

* ``GLOBAL_LOCKER`` — reentrant lock used by the connection pool and a
  few schema-creation paths to serialize side-effects across threads.
* ``THREAD_LOCAL`` — per-thread scratch namespace; adapters store
  their current connection here. When ``greenlet`` is installed it is
  per-*greenlet* instead (see ``GreenletLocal``): every thread still
  gets its own namespace exactly as before, and each request run
  through ``pydal.aio`` gets its own too.
* ``DEFAULT`` — sentinel-as-callable used as a default parameter value
  by ``Field`` so ``default=None`` and "no default given" can be
  distinguished.
* ``IDENTITY`` — ``lambda x: x``, used as a default credential decoder.
* ``OR`` / ``AND`` — bound versions of the bitwise operators, used by
  ``functools.reduce`` to combine a list of subqueries.
"""

import threading

try:  # optional: only needed for pydal.aio
    from greenlet import getcurrent as _getcurrent
except ImportError:  # pragma: no cover - exercised when greenlet is absent
    _getcurrent = None


class GreenletLocal(object):
    """
    Drop-in replacement for ``threading.local`` scoped to the current
    greenlet.

    Every OS thread runs in its own (main) greenlet, so for ordinary
    threaded code this behaves exactly like ``threading.local``. Code
    running inside a separate greenlet — which is how ``pydal.aio``
    runs each request on the event loop — gets a private namespace, so
    concurrent requests never share a connection or a cursor.

    The namespace lives in the greenlet's own ``__dict__``, so it is
    released together with the greenlet, just as ``threading.local``
    data is released with its thread.
    """

    __slots__ = ("_pydal_key",)

    def __init__(self):
        object.__setattr__(self, "_pydal_key", "_pydal_local_%x" % id(self))

    # Hot path: adapters read their connection and cursor from here on
    # every statement, so lookups bind their helpers as default args.
    def _pydal_ns(self, _gc=_getcurrent, _oga=object.__getattribute__):
        gdict = _gc().__dict__
        key = _oga(self, "_pydal_key")
        try:
            return gdict[key]
        except KeyError:
            ns = gdict[key] = {}
            return ns

    def __getattribute__(self, name, _gc=_getcurrent, _oga=object.__getattribute__):
        gdict = _gc().__dict__
        try:
            ns = gdict[_oga(self, "_pydal_key")]
        except KeyError:
            ns = gdict[_oga(self, "_pydal_key")] = {}
        try:
            return ns[name]
        except KeyError:
            if name == "__dict__":  # threading.local exposes its namespace
                return ns
            raise AttributeError(name) from None

    def __setattr__(self, name, value):
        GreenletLocal._pydal_ns(self)[name] = value

    def __delattr__(self, name):
        try:
            del GreenletLocal._pydal_ns(self)[name]
        except KeyError:
            raise AttributeError(name) from None


def new_local():
    """Return a greenlet-aware local when greenlet is available, else threading.local."""
    if _getcurrent is None:
        return threading.local()
    return GreenletLocal()


GLOBAL_LOCKER: threading.RLock = threading.RLock()
THREAD_LOCAL = new_local()

# Sentinel marking "no value supplied" for Field defaults. Implemented
# as a callable so ``DEFAULT()`` returns ``None`` — matches the legacy
# behavior of Field's "compute default lazily" path.
DEFAULT = lambda: None


def IDENTITY(x):
    """
    Identity function: ``IDENTITY(x) == x``.

    Used as a default no-op transformer (e.g. for credential decoding).
    """
    return x


def OR(a, b):
    """Bitwise-OR ``a`` and ``b`` — query combinator for ``reduce``."""
    return a | b


def AND(a, b):
    """Bitwise-AND ``a`` and ``b`` — query combinator for ``reduce``."""
    return a & b
