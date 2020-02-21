"""Adds authorization column to source table

Revision ID: 288cd4eb46afa
Revises:
Create Date: 2020-02-17 11:23:30.493160

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '288cd4eb46af'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('source', sa.Column('private', sa.Boolean, default=True, nullable=False))   


def downgrade():
    op.drop_column('source', 'private')