# Postgres
MONITOR_URL = 'postgresql+psycopg2://monitor@/monitor'
METADATA_URL = 'postgresql+psycopg2://awips@/metadata'

# AMQP
AMQP_URL = 'amqp://localhost'
AMQP_QUEUE = 'port_agent_stats'

# UFRAME STATUS NOTIFIER
NOTIFY_URL_ROOT = 'http://localhost'
NOTIFY_URL_PORT = 12587

# Tool Tip Text Associated with data availability display
DATA_NOT_EXPECTED = 'Not Expected'
DATA_MISSING = 'Missing'
DATA_PRESENT = 'Present'
DATA_SPARSE_1 = 'Sparse 1'
DATA_SPARSE_2 = 'Sparse 2'
DATA_SPARSE_3 = 'Sparse 3'

# Define colors for display of data availability results
COLOR_NOT_EXPECTED = '#ffffff'
COLOR_MISSING = '#d9534d'
COLOR_PRESENT = '#5cb87b'
COLOR_SPARSE_1 = '#7bcb7b'
COLOR_SPARSE_2 = '#90d890'
COLOR_SPARSE_3 = '#ace9ac'

# interval endpoints for determining sparsity of data
SPARSE_DATA_MIN = 1.0
SPARSE_DATA_MID = 1.5
SPARSE_DATA_MAX = 2.0

# Color coding for even/odd deployments
COLOR_EVEN_DEPLOYMENT = '#0073cf'
COLOR_ODD_DEPLOYMENT = '#cf5c00'
