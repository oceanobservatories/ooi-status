"""Add per-stream expected rate

Revision ID: ad240391c6d6
Revises: bf5b9a4cbd64
Create Date: 2016-04-06 15:44:07.442320

"""

# revision identifiers, used by Alembic.
revision = 'ad240391c6d6'
down_revision = 'bf5b9a4cbd64'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.add_column('deployed_stream', sa.Column('fail_interval', sa.Integer(), nullable=True))
    op.add_column('deployed_stream', sa.Column('rate', sa.Float(), nullable=True))
    op.add_column('deployed_stream', sa.Column('warn_interval', sa.Integer(), nullable=True))


def downgrade():
    op.drop_column('deployed_stream', 'warn_interval')
    op.drop_column('deployed_stream', 'rate')
    op.drop_column('deployed_stream', 'fail_interval')
