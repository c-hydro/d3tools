"""Experimental notification utilities.

This package currently exposes :class:`EmailNotification`, which builds and
sends a multipart email over SMTP. The feature is intentionally experimental
and is known to be unreliable; it is not a production-grade delivery
mechanism.
"""

from .email_notification import EmailNotification