"""Add unique constraints to patients and biometrics

Revision ID: 8e98a16e8c3c
Revises: 8fafd8157949
Create Date: 2025-05-30 09:08:40.642741

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8e98a16e8c3c'
down_revision: Union[str, None] = '8fafd8157949'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_unique_constraint(
        'unique_email',
        'patients',
        ['email']
    )

    op.create_unique_constraint(
        'unique_biometric_entry',
        'biometrics',
        [
            'patient_id',
            'biometric_type',
            'systolic',
            'diastolic',
            'value',
            'unit',
            'timestamp'
        ]
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('unique_email', 'patients', type_='unique')
    op.drop_constraint('unique_biometric_entry', 'biometrics', type_='unique')
