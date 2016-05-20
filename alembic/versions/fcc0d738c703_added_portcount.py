"""added portcount

Revision ID: fcc0d738c703
Revises: f3d59112a529
Create Date: 2016-05-18 16:26:19.522355

"""

# revision identifiers, used by Alembic.
revision = 'fcc0d738c703'
down_revision = 'f3d59112a529'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('port_count',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('reference_designator', sa.String(), nullable=False),
    sa.Column('collected_time', sa.DateTime(), nullable=False),
    sa.Column('byte_count', sa.Integer(), nullable=True),
    sa.Column('seconds', sa.Float(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )


def downgrade():
    op.drop_table('port_count')
