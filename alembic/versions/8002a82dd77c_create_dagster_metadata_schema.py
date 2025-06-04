"""create dagster_metadata schema

Revision ID: 8002a82dd77c
Revises: d5233a8698da
Create Date: 2025-06-02 19:24:12.433318

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "8002a82dd77c"
down_revision: Union[str, None] = "d5233a8698da"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        """
        CREATE SCHEMA IF NOT EXISTS dagster_metadata AUTHORIZATION "user";
    """
    )
    op.execute(
        """
        GRANT ALL ON SCHEMA dagster_metadata TO "user";
    """
    )
    op.execute(
        """
        ALTER DEFAULT PRIVILEGES FOR ROLE "user" IN SCHEMA dagster_metadata
        GRANT ALL ON TABLES TO "user";
    """
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP SCHEMA IF EXISTS dagster_metadata CASCADE")
