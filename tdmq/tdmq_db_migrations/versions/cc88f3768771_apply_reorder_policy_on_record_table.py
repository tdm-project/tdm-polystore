"""apply reorder policy on record table

Revision ID: cc88f3768771
Revises: 288cd4eb46af
Create Date: 2021-11-22 17:02:52.414356

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'cc88f3768771'
down_revision = '288cd4eb46af'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("SELECT add_reorder_policy('record', 'record_source_id_time_idx');")


def downgrade():
    op.execute("SELECT remove_reorder_policy('conditions', if_exists => true);")
