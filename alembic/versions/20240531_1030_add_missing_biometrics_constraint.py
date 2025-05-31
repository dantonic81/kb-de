"""Add missing unique constraint to biometrics

Revision ID: 8e98a16e8c3c_manualfix
Revises: 8e98a16e8c3c
Create Date: 2024-05-31 10:30:00.000000
"""

from alembic import op
import sqlalchemy as sa

# Revision identifiers (must match exactly)
revision = '8e98a16e8c3c_manualfix'
down_revision = '0e45a3581a53'
branch_labels = None
depends_on = None

def upgrade():
    op.create_unique_constraint(
        'unique_biometric_entry',
        'biometrics',
        ['patient_id', 'biometric_type', 'systolic', 'diastolic', 'value', 'unit', 'timestamp'],
        schema='public'
    )

def downgrade():
    op.drop_constraint('unique_biometric_entry', 'biometrics', type_='unique')