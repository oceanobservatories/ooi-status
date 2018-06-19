# Postgres
MONITOR_URL ='postgresql+psycopg2://monitor@/monitor'
METADATA_URL = 'postgresql+psycopg2://awips@/metadata'

# AMQP
AMQP_URL = 'amqp://localhost'
AMQP_QUEUE = 'port_agent_stats'

# UFRAME STATUS NOTIFIER
NOTIFY_URL_ROOT = 'http://localhost'
NOTIFY_URL_PORT = 12587

# Tool Tip Text Associated with sparsity levels
SPARSITY_LEVEL_1 = 'Sparsity Level 1'
SPARSITY_LEVEL_2 = 'Sparsity Level 2'
SPARSITY_LEVEL_3 = 'Sparsity Level 3'

# interval endpoints for determining sparsity of data
SPARSE_DATA_MIN = 1.0
SPARSE_DATA_MID = 1.5
SPARSE_DATA_MAX = 2.0
