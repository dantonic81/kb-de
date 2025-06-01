"""biometrics_add_upsert_constraint

Revision ID: 131045a89ff6
Revises: 5aa12899cbf4
Create Date: 2025-06-01 11:47:08.903198

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '131045a89ff6'
down_revision: Union[str, None] = '5aa12899cbf4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        'uq_biometric_entry',
        'biometrics',
        ['patient_id', 'biometric_type', 'timestamp']
    )


def downgrade() -> None:
    op.drop_constraint('uq_biometric_entry', 'biometrics')