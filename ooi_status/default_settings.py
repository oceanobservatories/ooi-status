# Postgres
POSTHOST = 'localhost'
PASSWORD = 'monitor'
USER = 'monitor'

# Email
SMTP_USER = None
SMTP_PASS = None
SMTP_HOST = 'localhost'
EMAIL_FROM = 'noreply@oceanobservatories.org'
DIGEST_SUBJECT = 'OOI Status Digest (%s) (%s)'
NOTIFY_SUBJECT = 'OOI STATUS CHANGE NOTIFICATION'

# Status Monitor VARS
WWW_ROOT = '.'
URL_ROOT = 'http://localhost:12571'
RESAMPLE_WINDOW_START_HOURS = 48
RESAMPLE_WINDOW_END_HOURS = 60

# AMQP
AMQP_URL = 'amqp://localhost'
AMQP_QUEUE = 'port_agent_stats'
