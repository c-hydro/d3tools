"""Experimental email notification support.

Email sending is currently known to be broken; this module is not a reliable
delivery mechanism and should be treated as experimental only.
"""

from typing import Optional
import os

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

#TODO TEST
class EmailNotification:
    """Experimental SMTP-backed email notification helper.

    This class builds a multipart message, optionally attaches files, and sends
    it to one or more recipients using STARTTLS over SMTP. The instance stores
    the open SMTP connection and message state; after `send()`, the connection is
    closed and the message is not reset for reuse.
    """

    def __init__(self,
                 from_address: str,
                 email_client: Optional[str] = None,
                 email_login_env: Optional[str] = None,
                 email_pwd_env: Optional[str] = None):
        """Connect to an SMTP server and initialize the outgoing message.

        Args:
            from_address: Sender address. Its domain is used as a fallback when
                building candidate SMTP hosts.
            email_client: Preferred SMTP host to try first. If unset, the value
                of the `SMTP_HOST` environment variable is checked next, followed
                by `mail.<domain>` and `smtp.<domain>`.
            email_login_env: Name of the environment variable that contains the
                SMTP login. Defaults to `EMAIL_LOGIN`; if that variable is unset,
                `from_address` is used as the login.
            email_pwd_env: Name of the environment variable that contains the
                SMTP password. Defaults to `EMAIL_PASSWORD`.

        Raises:
            IndexError: If `from_address` does not contain an `@` separator.
            ConnectionError: If no candidate SMTP host accepts a connection and a
                STARTTLS upgrade.
            smtplib.SMTPException: If SMTP authentication fails.

        Notes:
            The connection is opened on port 2525 with a 10-second timeout.
            Errors from one candidate host are ignored while the next candidate is
            tried.
        """

        domain = from_address.split("@")[1]

        candidates = []

        if email_client:
            candidates.append(email_client)

        env_host = os.getenv("SMTP_HOST")
        if env_host:
            candidates.append(env_host)

        candidates.extend([
            f"mail.{domain}",
            f"smtp.{domain}",
        ])

        for candidate in candidates:
            try:
                sm = smtplib.SMTP(candidate, 2525, timeout=10)
                sm.starttls()
                email_client = candidate
                break
            except:
                continue
        else:
            raise ConnectionError(f"Could not connect to any SMTP server for domain {domain}. Tried: {candidates}")

        email_login_env = email_login_env or 'EMAIL_LOGIN'
        email_pwd_env = email_pwd_env or 'EMAIL_PASSWORD'
        email_login = os.getenv(email_login_env, from_address)
        email_pwd   = os.getenv(email_pwd_env)

        sm.login(email_login, email_pwd)
        self.sm = sm
        
        self.from_address = from_address

        msg = MIMEMultipart()
        msg['From'] = from_address
        self.msg = msg
    
    def attach(self, file: str):
        """Append a file as a MIME attachment to the message.

        Args:
            file: Path to the file to attach. The base name of the file is used
                as the attachment filename.

        Returns:
            None: The attachment is added to the instance's message in place.

        Raises:
            OSError: If the file cannot be opened or read.

        Note:
            The file is read immediately and the handle is left open; the method
            does not call `close()` on the opened file object.
        """

        attachment = open(file, "rb")
        pr = MIMEBase('application', 'octet-stream')
        pr.set_payload((attachment).read())
        encoders.encode_base64(pr)
        pr.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(file)}")
        self.msg.attach(pr)

    def send(self, recipients: str|list[str], subject: str, body: Optional[str] = None):
        """Send the composed message and close the SMTP connection.

        Args:
            recipients: A single recipient address or a list of recipient
                addresses. When a list is provided, it is joined with `, ` for
                the message's `To` header and passed through unchanged to
                `sendmail()`.
            subject: Subject line for the email.
            body: Plain-text body to include. If omitted, a default
                product-availability message is used.

        Returns:
            None: The message is sent immediately and the SMTP client is ended
                with `quit()`.

        Raises:
            smtplib.SMTPException: If sending the email or closing the SMTP
                session fails.

        Note:
            This method mutates the instance's message by setting `To` and
            `Subject`, appending a plain-text MIME part, and calling
            `sendmail()`. After `send()`, the SMTP connection is closed and the
            instance is not reset for reuse.
        """

        self.msg['To'] = ', '.join(recipients) if isinstance(recipients, list) else recipients
        self.msg['Subject'] = subject

        if body is None:
            body = "This is an automatically generated email to notify that the product below has been produced and is now available."
        
        self.msg.attach(MIMEText(body, 'plain'))
        text = self.msg.as_string()

        self.sm.sendmail(self.from_address, recipients, text)
        self.sm.quit()