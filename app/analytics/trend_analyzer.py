# trend_analyzer.py
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Generator

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from statsmodels.tsa.seasonal import seasonal_decompose

from app.db.models import Patient, Biometric, BiometricTrend

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

# Analysis parameters - configurable if needed
TREND_WINDOW = timedelta(hours=5)  # 30 days for production
MIN_DATA_POINTS = 2  # 5 for production


class TrendAnalyzer:
    BIOMETRIC_TYPES = ["glucose", "weight", "blood_pressure"]
    STABILITY_THRESHOLDS = {"glucose": 0.1, "weight": 0.03, "blood_pressure": 0.07}

    def __init__(self):
        pass  # No persistent session; use context manager

    def analyze_all_patients(self):
        """Analyze trends for all patients and biometric types, batching to limit memory usage."""
        logger.info("Starting trend analysis for all patients")
        for patient in self._get_patients(batch_size=100):
            for biometric_type in self.BIOMETRIC_TYPES:
                try:
                    self.analyze_patient_trend(patient.id, biometric_type)
                except Exception:
                    logger.exception(
                        f"Failed analyzing patient {patient.id} biometric {biometric_type}"
                    )
        logger.info("Completed trend analysis for all patients")

    def _get_patients(self, batch_size: int = 100) -> Generator[Patient, None, None]:
        """Yield patients in batches to avoid loading all at once."""
        with SessionLocal() as session:
            offset = 0
            while True:
                batch = session.query(Patient).offset(offset).limit(batch_size).all()
                if not batch:
                    break
                yield from batch
                offset += batch_size

    def analyze_patient_trend(self, patient_id: int, biometric_type: str):
        """Analyze and store trend for a specific patient's biometric data."""
        with SessionLocal() as session:
            measurements = self._get_measurements(session, patient_id, biometric_type)
            if len(measurements) < MIN_DATA_POINTS:
                logger.info(
                    f"Insufficient data for patient {patient_id} biometric {biometric_type}"
                )
                self._store_trend(
                    session, patient_id, biometric_type, "insufficient_data"
                )
                return

            # Extract values and timestamps
            if biometric_type == "blood_pressure":
                values = [(m.systolic + m.diastolic) / 2 for m in measurements]
            else:
                values = [m.value for m in measurements]

            timestamps = [m.timestamp for m in measurements]

            # Run analyses
            linear_trend = self._linear_trend_analysis(timestamps, values)
            percentage_change = self._percentage_change(values)
            volatility = self._volatility_analysis(values)
            seasonal = self._seasonal_decomposition(timestamps, values)

            analysis_results = {
                "linear_trend": linear_trend,
                "percentage_change": percentage_change,
                "volatility": volatility,
                "seasonal_decomposition": seasonal,
            }

            trend = self._classify_trend(biometric_type, analysis_results)
            self._store_trend(session, patient_id, biometric_type, trend)

            logger.info(
                f"Trend analysis for patient {patient_id} - {biometric_type}: "
                f"{trend} (Linear slope: {linear_trend['slope']:.3f}, "
                f"Change: {percentage_change:.1f}%)"
            )

    def _get_measurements(
        self, session: Session, patient_id: int, biometric_type: str
    ) -> List[Biometric]:
        """Retrieve measurements within the analysis window."""
        cutoff = datetime.now(timezone.utc) - TREND_WINDOW
        return (
            session.query(Biometric)
            .filter(
                Biometric.patient_id == patient_id,
                Biometric.biometric_type == biometric_type,
                Biometric.timestamp >= cutoff,
            )
            .order_by(Biometric.timestamp.asc())
            .all()
        )

    def _linear_trend_analysis(
        self, timestamps: List[datetime], values: List[float]
    ) -> Dict[str, float]:
        """Perform linear regression on the time series data."""
        x = np.array([ts.timestamp() for ts in timestamps])
        y = np.array(values)
        x_norm = x - x.min()  # Normalize to reduce numerical issues

        A = np.vstack([x_norm, np.ones(len(x_norm))]).T
        slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
        r_squared = self._calculate_r_squared(x_norm, y, slope, intercept)

        return {"slope": slope, "intercept": intercept, "r_squared": r_squared}

    def _calculate_r_squared(
        self, x: np.ndarray, y: np.ndarray, slope: float, intercept: float
    ) -> float:
        """Calculate coefficient of determination (R^2)."""
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        return 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

    def _percentage_change(self, values: List[float]) -> float:
        """Calculate percentage change between first and last value."""
        if len(values) < 2 or values[0] == 0:
            return 0.0
        return ((values[-1] - values[0]) / values[0]) * 100

    def _volatility_analysis(self, values: List[float]) -> float:
        """Calculate coefficient of variation (volatility)."""
        mean_val = np.mean(values)
        if mean_val == 0:
            return 0.0
        return np.std(values) / mean_val

    def _seasonal_decomposition(
        self, timestamps: List[datetime], values: List[float]
    ) -> Optional[Dict[str, float]]:
        """Perform seasonal decomposition of the time series if possible."""
        try:
            series = pd.Series(values, index=pd.to_datetime(timestamps))
            # Resample to daily frequency, forward fill missing values
            series = series.asfreq("D").ffill()

            series_var = np.var(series)
            if series_var == 0 or len(series) < 8:
                return None

            decomposition = seasonal_decompose(series, model="additive", period=7)
            trend_strength = (
                np.var(decomposition.trend) / series_var
                if np.var(decomposition.trend) > 0
                else 0
            )
            seasonality_strength = (
                np.var(decomposition.seasonal) / series_var
                if np.var(decomposition.seasonal) > 0
                else 0
            )

            return {
                "trend_strength": trend_strength,
                "seasonality_strength": seasonality_strength,
            }
        except Exception as e:
            logger.warning(f"Seasonal decomposition failed: {e}")
            return None

    def _classify_trend(self, biometric_type: str, analysis_results: Dict) -> str:
        """Classify the overall trend based on analysis metrics."""
        linear = analysis_results["linear_trend"]
        pct_change = analysis_results["percentage_change"]
        volatility = analysis_results["volatility"]

        threshold = self.STABILITY_THRESHOLDS.get(biometric_type, 0.1)

        if volatility > threshold * 2:
            return "volatile"

        if abs(linear["slope"]) < threshold and abs(pct_change) < threshold * 10:
            return "stable"

        if (linear["slope"] > 0 and pct_change > 0) or (
            linear["r_squared"] > 0.7 and linear["slope"] > 0
        ):
            return "increasing"

        if (linear["slope"] < 0 and pct_change < 0) or (
            linear["r_squared"] > 0.7 and linear["slope"] < 0
        ):
            return "decreasing"

        return "stable"

    def _store_trend(
        self, session: Session, patient_id: int, biometric_type: str, trend: str
    ):
        """Store or update the trend result in the database."""
        try:
            record = (
                session.query(BiometricTrend)
                .filter_by(patient_id=patient_id, biometric_type=biometric_type)
                .one_or_none()
            )

            now = datetime.now(timezone.utc)
            if record:
                record.trend = trend
                record.analyzed_at = now
            else:
                record = BiometricTrend(
                    patient_id=patient_id,
                    biometric_type=biometric_type,
                    trend=trend,
                    analyzed_at=now,
                )
                session.add(record)
            session.commit()
        except Exception:
            logger.exception(
                f"Failed to store trend for patient {patient_id}, biometric {biometric_type}"
            )
            session.rollback()


def main():
    analyzer = TrendAnalyzer()
    analyzer.analyze_all_patients()


if __name__ == "__main__":
    main()
