"""add status_updated_at to positions

Revision ID: 0002_add_status_updated_at
Revises: 0001_initial
Create Date: 2026-05-08 00:01:00.000000

Adds status_updated_at to positions table for AC#14:
reconciler uses this instead of opened_at to detect stale open_pending_tp_sl.
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_add_status_updated_at"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "positions",
        sa.Column("status_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Backfill existing rows: set to opened_at as best approximation
    op.execute(
        "UPDATE positions SET status_updated_at = opened_at WHERE status_updated_at IS NULL"
    )


def downgrade() -> None:
    op.drop_column("positions", "status_updated_at")
