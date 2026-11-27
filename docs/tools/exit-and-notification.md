# Exit handling and notification

D3tools provides a small exit-callback queue for deferred cleanup. It also
contains an experimental SMTP email helper, but that helper is known to be
unreliable and is not recommended for production workflows.

## Register exit callbacks

Import the supported registration helpers from `d3tools.exit`:

```python
from d3tools.exit import rm_at_exit, run_at_exit, run_at_exit_first
```

`run_at_exit(func, *args, **kwargs)` appends a callback to the shared queue.
Arguments are stored and forwarded when the callback runs. Python's `atexit`
mechanism invokes the queue during normal interpreter shutdown.

<!-- check-docs: skip -->
```python
from pathlib import Path

from d3tools.exit import run_at_exit

# This write is deferred until normal interpreter shutdown.
run_at_exit(Path("run-finished.txt").write_text, "finished\n")
```

This example is not executed by the documentation checker because it registers
a real process-exit side effect. Do not use deferred
callbacks for work that must complete after a crash, forced termination, or
`os._exit()`: Python does not guarantee that `atexit` handlers run in those
conditions.

### Ordering

Callbacks registered with `run_at_exit()` execute in registration order.
`run_at_exit_first()` inserts a callback at the front of the current queue.
Repeated `run_at_exit_first()` calls therefore run in reverse registration
order before appended callbacks:

```python
from d3tools.exit.exit_handler import ExitHandler

events = []
handler = ExitHandler()
handler.register(events.append, "first appended")
handler.register(events.append, "second appended")

# ExitHandler is the queue object used internally. Inserting at position zero
# is the behavior exposed by run_at_exit_first().
handler.functions.insert(0, (events.append, ("prepended",), {}))
handler.run()

assert events == ["prepended", "first appended", "second appended"]
```

`ExitHandler` itself is available from
`d3tools.exit.exit_handler`, but it is not re-exported from `d3tools.exit`.
Most callers should use the three package-level helper functions rather than
create another handler.

### Exceptions

Callbacks are called directly, without exception isolation. If one raises,
that exception propagates from the queue and later callbacks do not run:

```python
from d3tools.exit.exit_handler import ExitHandler

events = []
handler = ExitHandler()
handler.register(events.append, "started")
handler.register(lambda: (_ for _ in ()).throw(RuntimeError("cleanup failed")))
handler.register(events.append, "not reached")

try:
    handler.run()
except RuntimeError as error:
    assert str(error) == "cleanup failed"

assert events == ["started"]
```

If several independent cleanups are essential, wrap expected failures inside
the callbacks themselves and record them through the application's logger.

## Remove temporary paths at exit

`rm_at_exit(path, force=True)` schedules a recursive system `rm` command:

- `force=True` uses `rm -rf`;
- `force=False` uses `rm -r`.

The subprocess return code is not checked. The helper does not validate,
resolve, or constrain the path before registering it, so call it only with an
explicit temporary path created and owned by the current application. Never
pass a workspace root, home directory, unresolved environment variable, or
broad glob.

```python
import tempfile

from d3tools.exit import rm_at_exit

temporary_directory = tempfile.mkdtemp(prefix="d3tools-")
rm_at_exit(temporary_directory)
```

Remote Datasets use this mechanism for local transfer directories, and
thumbnail collections use it as additional cleanup for their temporary
rendering directory. Prefer a `TemporaryDirectory` context manager when the
lifetime is lexical and immediate cleanup is possible; use `rm_at_exit()` only
when a resource genuinely needs to survive until process shutdown.

## Workflow logging at exit

`WorkflowLogManager` registers its `close()` method with `run_at_exit()` so
managed handlers can be closed and remote log files uploaded during normal
shutdown. Python callers that know when processing is finished should still
call `workflow.logger.close()` explicitly: this surfaces errors at a useful
point and does not depend on interpreter shutdown.

See [Workflow lifecycle — Logging, exceptions, and
cleanup](../workflow_orchestration/under-the-hood/lifecycle.md#logging-exceptions-and-cleanup)
for the orchestration lifecycle.

## Experimental email notification

`EmailNotification` is present in `d3tools.notification`, but its source marks
email sending as known to be broken. It has no supported workflow JSON
integration, delivery guarantee, retry policy, or automated test coverage.
Treat it as an experimental implementation to inspect, not as an operational
notification service.

The constructor immediately attempts to connect and authenticate. It tries
SMTP hosts in this order:

1. the explicit `email_client`, when supplied;
2. the `SMTP_HOST` environment variable;
3. `mail.<sender-domain>`;
4. `smtp.<sender-domain>`.

Each connection uses port `2525`, a ten-second timeout, and STARTTLS. After a
connection succeeds, the helper logs in with:

| Setting | Default source |
| --- | --- |
| Login | `EMAIL_LOGIN`, falling back to the sender address. |
| Password | `EMAIL_PASSWORD`. |
| Alternative environment names | `email_login_env` and `email_pwd_env` constructor arguments. |

Do not put credentials in source code or documentation. Supply them through
the environment and ensure that the process environment is protected.

The intended API looks like this, but it should not be used for a production
workflow:

<!-- check-docs: skip -->
```python
from d3tools.notification import EmailNotification

notification = EmailNotification(
    from_address="automation@example.org",
    email_client="smtp.example.org",
)
notification.attach("output/run-report.txt")
notification.send(
    recipients=["operator@example.org", "archive@example.org"],
    subject="Workflow finished",
    body="The output is ready.",
)
```

### Message and failure behaviour

`attach(path)` reads the file immediately and adds it as a base64-encoded
`application/octet-stream` MIME part. The attachment filename is the path's
basename. Missing or unreadable files raise `OSError`.

`send(recipients, subject, body=None)` accepts one address or a list, adds a
plain-text MIME body, sends the complete message, and calls `quit()` on the
SMTP connection. When `body` is omitted, it uses a built-in product-availability
message. The object does not reset its message or reconnect after sending, so
it should not be reused.

Important limitations include:

- a sender address without `@` raises `IndexError` while deriving the domain;
- connection and STARTTLS failures are suppressed while the next candidate is
  tried, then reported as `ConnectionError` if all candidates fail;
- authentication, sending, and shutdown errors can propagate as SMTP
  exceptions;
- the SMTP port is fixed at `2525` and cannot be configured;
- certificates, recipients, addresses, and attachments receive no additional
  application-level validation;
- attachment file handles are not explicitly closed by `attach()`; and
- there is no retry, queuing, structured delivery result, or cleanup context.

For production notification, use a maintained mail or messaging client in the
application or engine that owns delivery requirements. For exact experimental
signatures, see the
[Notification API reference](../reference/notification.md). Exit helper
signatures are in the [Exit API reference](../reference/exit.md).
