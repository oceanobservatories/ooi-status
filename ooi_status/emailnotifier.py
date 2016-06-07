import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import jinja2

from ooi_status.get_logger import get_logger

html_template = 'html_status.jinja'
plaintext_template = 'plaintext_status.jinja'
loader = jinja2.PackageLoader('ooi_status', 'templates')
env = jinja2.Environment(loader=loader, trim_blocks=True)


log = get_logger(__name__, logging.INFO)


class EmailNotifier(object):
    def __init__(self, server, from_addr, base_url, username, password):
        self.conn = smtplib.SMTP(server)
        self.from_addr = from_addr
        self.base_url = base_url
        if username and password:
            self.conn.login(username, password)

    def send_status(self, receivers, subject, status_dict):
        html = self.apply_template(status_dict, html_template, 'html')
        text = self.apply_template(status_dict, plaintext_template, 'plain')
        log.error('send_status: %r %r %r', receivers, subject, status_dict)

        for receiver in receivers:
            message = MIMEMultipart('alternative')
            message['Subject'] = subject
            message['From'] = self.from_addr
            message['To'] = receiver
            message.attach(text)
            message.attach(html)

            self.conn.sendmail(self.from_addr, [receiver], message.as_string())

    def send_html(self, receivers, subject, html):
        for receiver in receivers:
            message = MIMEText(html, 'html')
            message['Subject'] = subject
            message['From'] = self.from_addr
            message['To'] = receiver

            self.conn.sendmail(self.from_addr, [receiver], message.as_string())

    def apply_template(self, status_dict, template, mimetype):
        text = env.get_template(template).render(status_dict=status_dict, base_url=self.base_url)
        return MIMEText(text, mimetype)