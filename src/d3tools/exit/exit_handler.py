"""Manage a shutdown callback sequence registered with Python's ``atexit`` hook.

The module keeps a single ordered callback list. Callbacks added through
:func:`run_at_exit` are appended, while callbacks added through
:func:`run_at_exit_first` are inserted at the front so they execute earlier.
Exceptions raised by any queued callback propagate immediately and stop the
remaining callbacks in that same sequence.
"""

import atexit


class ExitHandler:
    """Store and execute deferred shutdown callbacks in registration order.

    Attributes:
        functions: Sequence of ``(func, args, kwargs)`` tuples queued for
            execution at interpreter shutdown.
    """

    def __init__(self):
        """Initialize an empty shutdown callback queue."""
        self.functions = []

    def register(self, func, *args, **kwargs):
        """Queue a callback for execution during interpreter shutdown.

        Args:
            func: Callable to invoke when the interpreter exits.
            *args: Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.
        """
        self.functions.append((func, args, kwargs))

    def run(self):
        """Execute all queued callbacks in the stored order.

        Raises:
            Exception: If any callback raises an exception, the exception is
                propagated immediately and later queued callbacks do not run.
        """
        for func, args, kwargs in self.functions:
            func(*args, **kwargs)


# Create a global exit handler instance
exit_handler = ExitHandler()

# Register the custom exit handler to run at exit
atexit.register(exit_handler.run)


# Function to register your desired function first
def run_at_exit_first(func, *args, **kwargs):
    """Register a callback before all existing shutdown callbacks.

    The callback is inserted at the beginning of the pending sequence so it runs
    before callbacks already queued by :func:`run_at_exit`. Repeated calls to
    this helper prepend new callbacks, so the most recently registered callback
    runs first among the callbacks inserted through this function.

    Args:
        func: Callable to invoke when the interpreter exits.
        *args: Positional arguments passed to ``func`` at exit.
        **kwargs: Keyword arguments passed to ``func`` at exit.

    Raises:
        Exception: If ``func`` raises while running during shutdown, the
            exception is propagated immediately and the remaining queued
            callbacks are not executed.
    """
    exit_handler.functions.insert(0, (func, args, kwargs))


# Function to register other functions
def run_at_exit(func, *args, **kwargs):
    """Register a callback to run during interpreter shutdown.

    The callback is appended to the shutdown sequence, so callbacks registered
    with this function execute in registration order. Positional and keyword
    arguments are forwarded to the callback when it runs.

    Args:
        func: Callable to invoke when the interpreter exits.
        *args: Positional arguments passed to ``func`` at exit.
        **kwargs: Keyword arguments passed to ``func`` at exit.

    Raises:
        Exception: If ``func`` raises while running during shutdown, the
            exception is propagated immediately and later queued callbacks do
            not run.
    """
    exit_handler.register(func, *args, **kwargs)


# Function to force remove a path at the exit
def rm_at_exit(path, force = True):
    """Schedule a path for recursive removal during interpreter shutdown.

    This helper defers a system ``rm`` invocation to the shutdown callback
    sequence. With ``force=True``, it uses ``rm -rf``; otherwise it uses
    ``rm -r``. The subprocess return code is not checked.

    Args:
        path: Path to remove.
        force: Whether to pass ``-f`` in addition to ``-r``. Defaults to
            ``True``.

    Raises:
        OSError: If the subprocess cannot be started when the shutdown callback
            executes.
    """
    import subprocess
    if force:
        run_at_exit(subprocess.run, ['rm', '-rf', path])
    else:
        run_at_exit(subprocess.run, ['rm', '-r', path])
