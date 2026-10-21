"""Public shutdown hooks for deferred cleanup and final callbacks.

This package exposes helpers for registering callables to run during
interpreter shutdown and for scheduling temporary paths for deletion. The
implementation lives in :mod:`d3tools.exit.exit_handler` and uses Python's
standard ``atexit`` hook to execute queued callbacks in a predictable order.
"""

from .exit_handler import run_at_exit, run_at_exit_first, rm_at_exit