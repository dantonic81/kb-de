"""Add systolic and diastolic to biometrics

Revision ID: 8fafd8157949
Revises: 656b6f81490c
Create Date: 2025-05-29 18:28:52.148112

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8fafd8157949'
down_revision: Union[str, None] = '656b6f81490c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('biometrics', sa.Column('systolic', sa.Integer(), nullable=True))
    op.add_column('biometrics', sa.Column('diastolic', sa.Integer(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('biometrics', 'diastolic')
    op.drop_column('biometrics', 'systolic')
