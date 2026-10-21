# Notification package doc audit

| ID | Description | File & line no. | Type | Severity | Confidence | Proposed action |
|---|---|---|---|---|---|---|
| 1 | `EmailNotification` is the only public API and it is explicitly experimental, but the package still exposes only a bare generated API page with no topic-specific warning, usage caveat, or developer/user guide explaining the known-broken SMTP flow. This matters because the project documentation explicitly treats notification as experimental and known broken, yet the generated reference page does not surface that operational risk clearly enough to a caller who only sees the API reference. | `src/d3tools/notification/__init__.py:1-9`; `src/d3tools/notification/email_notification.py:1-18`; `docs/developer/reference/notification.md:1-5` | Missing public API docs | medium | confirmed | Add a short warning in the generated API page or create a small topic guide explaining the experimental status, required environment setup, and the fact that SMTP delivery is not supported as a production mechanism. |
| 2 | `EmailNotification.__init__` documents `email_pwd_env` as a default environment variable, but if that environment variable is unset the code calls `sm.login(email_login, email_pwd)` with `email_pwd` still `None`. This creates an undocumented failure path for callers: the docstring describes the variable name but not the actual requirement that a valid password must exist, so a missing credential can fail at SMTP authentication time without a clear public contract. | `src/d3tools/notification/email_notification.py:32-45`; `src/d3tools/notification/email_notification.py:68-78` | Unexpected/undocumented failure behavior | medium | confirmed | Clarify the credential contract in the docstring and, if the behavior is intended, validate the password earlier with a clear exception message instead of passing `None` through to `smtplib`. |
| 3 | No tests exercise the notification package, so the documented SMTP flow and the credential/error paths are not verified by the suite. This matters because `EmailNotification` is the public entry point and its behavior is otherwise inferred only from the implementation: attachment handling, recipient formatting, and SMTP failure paths are not covered by tests. | `src/d3tools/notification/__init__.py:1-9`; `src/d3tools/notification/email_notification.py:17-148`; `tests`: `Not covered by tests` | Untested behavior | low | confirmed | Add a focused test matrix covering successful send, attach behavior, missing credentials, and SMTP auth/connection failures so the public API contract is evidence-backed. |

No findings in the requested categories:
- Documentation mismatch: none found in the current package implementation and generated reference page.
- Probable bug: none proven from the docstrings alone; the risk in ID 2 is an undocumented failure path rather than a confirmed defect in a real server environment.

Audit scope and inventory:
- Package audited: `src/d3tools/notification`
- Source modules inspected: `src/d3tools/notification/__init__.py`, `src/d3tools/notification/email_notification.py`
- Reference page inspected: `docs/developer/reference/notification.md`
- Relevant checks run: `python scripts/check_docs.py docs/developer/reference/notification.md` -> `0 error(s), 0 warning(s)`; `python -m compileall src/d3tools/notification` -> success
- Recursive inventory status: complete for the selected package directory and the public API surface within that scope.

Areas that could not be verified:
- No integration or unit tests exist for `EmailNotification`, so real SMTP communication with a server was not verified.
- No user or developer guide exists for this topic beyond the auto-generated reference page, consistent with the current phase-1 documentation plan for experimental features.
