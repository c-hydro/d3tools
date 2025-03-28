from typing import Optional
import os

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

#TODO TEST
class EmailNotification:
    def __init__(self,
                 from_address: str,
                 email_client: Optional[str] = None,
                 email_login_env: Optional[str] = None,
                 email_pwd_env: Optional[str] = None):
        
        email_client = email_client or f'mail.{from_address.split("@")[1]}'
        email_login_env = email_login_env or 'EMAIL_LOGIN'
        email_pwd_env = email_pwd_env or 'EMAIL_PASSWORD'

        email_login = os.getenv(email_login_env, from_address)
        email_pwd   = os.getenv(email_pwd_env)

        sm = smtplib.SMTP(email_client, 2525)
        sm.starttls()
        sm.login(email_login, email_pwd)
        self.sm = sm
        
        self.from_address = from_address

        msg = MIMEMultipart()
        msg['From'] = from_address
        self.msg = msg
    
    def attach(self, file: str):

        attachment = open(file, "rb")
        pr = MIMEBase('application', 'octet-stream')
        pr.set_payload((attachment).read())
        encoders.encode_base64(pr)
        pr.add_header('Content-Disposition', f"attachment; filename= {os.path.basename(file)}")
        self.msg.attach(pr)

    def send(self, recipients: str|list[str], subject: str, body: Optional[str] = None):

        self.msg['To'] = ', '.join(recipients) if isinstance(recipients, list) else recipients
        self.msg['Subject'] = subject

        if body is None:
            body = "This is an automatically generated email to notify that the product below has been produced and is now available."
        
        self.msg.attach(MIMEText(body, 'plain'))
        text = self.msg.as_string()

        self.sm.sendmail(self.from_address, recipients, text)
        self.sm.quit()