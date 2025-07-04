"""initial

Revision ID: d5233a8698da
Revises:
Create Date: 2025-06-02 16:35:50.333131

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d5233a8698da"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "patients",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=True),
        sa.Column("dob", sa.Date(), nullable=True),
        sa.Column("gender", sa.String(), nullable=True),
        sa.Column("address", sa.String(), nullable=True),
        sa.Column("email", sa.String(length=150), nullable=True),
        sa.Column("phone", sa.String(), nullable=True),
        sa.Column("sex", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_patients_email"), "patients", ["email"], unique=True)
    op.create_index(op.f("ix_patients_id"), "patients", ["id"], unique=False)
    op.create_table(
        "biometric_trends",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("patient_id", sa.Integer(), nullable=True),
        sa.Column("biometric_type", sa.String(length=50), nullable=True),
        sa.Column("trend", sa.String(length=20), nullable=True),
        sa.Column("analyzed_at", sa.DateTime(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["patient_id"],
            ["patients.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "patient_id", "biometric_type", name="uq_patient_biometric_trend"
        ),
    )
    op.create_index(
        op.f("ix_biometric_trends_biometric_type"),
        "biometric_trends",
        ["biometric_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_biometric_trends_patient_id"),
        "biometric_trends",
        ["patient_id"],
        unique=False,
    )
    op.create_table(
        "biometrics",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("patient_id", sa.Integer(), nullable=False),
        sa.Column("biometric_type", sa.String(length=50), nullable=False),
        sa.Column("value", sa.Float(), nullable=True),
        sa.Column("systolic", sa.Integer(), nullable=True),
        sa.Column("diastolic", sa.Integer(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["patient_id"],
            ["patients.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_biometrics_biometric_type"),
        "biometrics",
        ["biometric_type"],
        unique=False,
    )
    op.create_index(op.f("ix_biometrics_id"), "biometrics", ["id"], unique=False)
    op.create_index(
        op.f("ix_biometrics_patient_id"), "biometrics", ["patient_id"], unique=False
    )
    op.create_index(
        op.f("ix_biometrics_timestamp"), "biometrics", ["timestamp"], unique=False
    )
    op.create_table(
        "patient_biometric_hourly_summary",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("patient_id", sa.Integer(), nullable=False),
        sa.Column("biometric_type", sa.String(), nullable=False),
        sa.Column("hour_start", sa.DateTime(), nullable=False),
        sa.Column("min_value", sa.Float(), nullable=True),
        sa.Column("max_value", sa.Float(), nullable=True),
        sa.Column("avg_value", sa.Float(), nullable=True),
        sa.Column("count", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["patient_id"],
            ["patients.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "patient_id", "biometric_type", "hour_start", name="uix_patient_type_hour"
        ),
    )
    op.create_index(
        op.f("ix_patient_biometric_hourly_summary_biometric_type"),
        "patient_biometric_hourly_summary",
        ["biometric_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_patient_biometric_hourly_summary_hour_start"),
        "patient_biometric_hourly_summary",
        ["hour_start"],
        unique=False,
    )
    op.create_index(
        op.f("ix_patient_biometric_hourly_summary_id"),
        "patient_biometric_hourly_summary",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_patient_biometric_hourly_summary_patient_id"),
        "patient_biometric_hourly_summary",
        ["patient_id"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        op.f("ix_patient_biometric_hourly_summary_patient_id"),
        table_name="patient_biometric_hourly_summary",
    )
    op.drop_index(
        op.f("ix_patient_biometric_hourly_summary_id"),
        table_name="patient_biometric_hourly_summary",
    )
    op.drop_index(
        op.f("ix_patient_biometric_hourly_summary_hour_start"),
        table_name="patient_biometric_hourly_summary",
    )
    op.drop_index(
        op.f("ix_patient_biometric_hourly_summary_biometric_type"),
        table_name="patient_biometric_hourly_summary",
    )
    op.drop_table("patient_biometric_hourly_summary")
    op.drop_index(op.f("ix_biometrics_timestamp"), table_name="biometrics")
    op.drop_index(op.f("ix_biometrics_patient_id"), table_name="biometrics")
    op.drop_index(op.f("ix_biometrics_id"), table_name="biometrics")
    op.drop_index(op.f("ix_biometrics_biometric_type"), table_name="biometrics")
    op.drop_table("biometrics")
    op.drop_index(op.f("ix_biometric_trends_patient_id"), table_name="biometric_trends")
    op.drop_index(
        op.f("ix_biometric_trends_biometric_type"), table_name="biometric_trends"
    )
    op.drop_table("biometric_trends")
    op.drop_index(op.f("ix_patients_id"), table_name="patients")
    op.drop_index(op.f("ix_patients_email"), table_name="patients")
    op.drop_table("patients")
    # ### end Alembic commands ###
