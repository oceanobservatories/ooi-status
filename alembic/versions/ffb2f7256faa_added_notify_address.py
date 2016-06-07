"""Added notify address

Revision ID: ffb2f7256faa
Revises: f26437d7cf68
Create Date: 2016-06-01 08:41:44.874033

"""

# revision identifiers, used by Alembic.
revision = 'ffb2f7256faa'
down_revision = 'f26437d7cf68'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table('notify_address',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('email_addr', sa.String(), nullable=False),
    sa.Column('email_type', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('email_addr', 'email_type')
    )


def downgrade():
    op.drop_table('notify_address')
