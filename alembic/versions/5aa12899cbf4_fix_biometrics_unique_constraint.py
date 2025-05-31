"""fix_biometrics_unique_constraint

Revision ID: 5aa12899cbf4
Revises: 00869267907f
Create Date: 2025-05-31 22:06:42.206776

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = '5aa12899cbf4'
down_revision: Union[str, None] = '00869267907f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Drop the problematic constraint
    op.drop_constraint('unique_biometric_entry', 'biometrics', type_='unique')

    # 2. Create proper index-based uniqueness
    op.execute(text("""
        CREATE UNIQUE INDEX unique_biometric_entry ON biometrics (
            patient_id,
            biometric_type,
            timestamp,
            COALESCE(value::text, ''),
            COALESCE(systolic::text, ''),
            COALESCE(diastolic::text, '')
        )
    """))


def downgrade() -> None:
    # 1. Remove our index
    op.drop_index('unique_biometric_entry', table_name='biometrics')

    # 2. Recreate the old constraint (simplified version)
    op.create_unique_constraint(
        'unique_biometric_entry',
        'biometrics',
        ['patient_id', 'biometric_type', 'timestamp', 'value']
    )
