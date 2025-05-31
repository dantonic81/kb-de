"""create_dagster_schema

Revision ID: 00869267907f
Revises: 8e98a16e8c3c_manualfix
Create Date: 2025-05-31 11:03:06.814828

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '00869267907f'
down_revision: Union[str, None] = '8e98a16e8c3c_manualfix'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create the schema
    op.execute("CREATE SCHEMA IF NOT EXISTS dagster_metadata")

    # Grant permissions to Dagster's DB user (note DOUBLE quotes)
    op.execute('GRANT ALL ON SCHEMA dagster_metadata TO "user"')
    op.execute('ALTER DEFAULT PRIVILEGES IN SCHEMA dagster_metadata GRANT ALL ON TABLES TO "user"')

def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP SCHEMA IF EXISTS dagster_metadata CASCADE")