# trend_analyzer.py
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, func
import numpy as np
from statsmodels.tsa.seasonal import seasonal_decompose
from app.db.models import Patient, Biometric, BiometricTrend


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Analysis parameters
# TREND_WINDOW = timedelta(days=30)  # Analyze last 30 days of data
TREND_WINDOW = timedelta(hours=5)

# MIN_DATA_POINTS = 5  # Minimum measurements needed for analysis
MIN_DATA_POINTS = 2  # Minimum measurements needed for analysis

STABILITY_THRESHOLDS = {
    'glucose': 0.1,  # 10% variation considered stable
    'weight': 0.03,  # 3% variation
    'blood_pressure': 0.07  # 7% variation
}


class TrendAnalyzer:
    def __init__(self):
        self.session = Session()

    def __del__(self):
        self.session.close()

    def analyze_all_patients(self):
        """Analyze trends for all patients and biometric types"""
        patients = self.session.query(Patient).all()
        biometric_types = ['glucose', 'weight', 'blood_pressure']

        for patient in patients:
            for bio_type in biometric_types:
                self.analyze_patient_trend(patient.id, bio_type)

    def analyze_patient_trend(self, patient_id: int, biometric_type: str):
        """Analyze and store trend for a specific patient's biometric"""
        measurements = self._get_measurements(patient_id, biometric_type)

        if len(measurements) < MIN_DATA_POINTS:
            logger.info(f"Insufficient data for {patient_id}-{biometric_type}")
            self._store_trend(patient_id, biometric_type, 'insufficient_data')
            return

        # Extract values and timestamps
        if biometric_type == 'blood_pressure':
            values = [(m.systolic + m.diastolic) / 2 for m in measurements]
        else:
            values = [m.value for m in measurements]

        timestamps = [m.timestamp for m in measurements]

        # Perform multiple analyses
        analysis_results = {
            'linear_trend': self._linear_trend_analysis(timestamps, values),
            'percentage_change': self._percentage_change(values),
            'volatility': self._volatility_analysis(values),
            'seasonal_decomposition': self._seasonal_decomposition(timestamps, values)
        }

        # Determine overall trend
        trend = self._classify_trend(biometric_type, analysis_results)
        self._store_trend(patient_id, biometric_type, trend)

        logger.info(
            f"Trend analysis for patient {patient_id} - {biometric_type}: "
            f"{trend} (Linear slope: {analysis_results['linear_trend']['slope']:.3f}, "
            f"Change: {analysis_results['percentage_change']:.1f}%)"
        )

    def _get_measurements(self, patient_id: int, biometric_type: str):
        """Get measurements for analysis window"""
        cutoff = datetime.now() - TREND_WINDOW
        return self.session.query(Biometric).filter(
            Biometric.patient_id == patient_id,
            Biometric.biometric_type == biometric_type,
            Biometric.timestamp >= cutoff
        ).order_by(Biometric.timestamp.asc()).all()

    def _linear_trend_analysis(self, timestamps: List[datetime], values: List[float]) -> Dict:
        """Perform linear regression on the time series"""
        x = np.array([ts.timestamp() for ts in timestamps])
        y = np.array(values)

        # Normalize x to avoid large numbers
        x_norm = x - x.min()

        # Calculate slope and intercept
        A = np.vstack([x_norm, np.ones(len(x_norm))]).T
        slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]

        return {
            'slope': slope,
            'intercept': intercept,
            'r_squared': self._calculate_r_squared(x_norm, y, slope, intercept)
        }

    def _calculate_r_squared(self, x, y, slope, intercept):
        """Calculate coefficient of determination"""
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        return 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

    def _percentage_change(self, values: List[float]) -> float:
        """Calculate percentage change from first to last value"""
        if len(values) < 2:
            return 0
        return ((values[-1] - values[0]) / values[0]) * 100

    def _volatility_analysis(self, values: List[float]) -> float:
        """Calculate coefficient of variation"""
        return np.std(values) / np.mean(values) if np.mean(values) != 0 else 0

    def _seasonal_decomposition(self, timestamps: List[datetime], values: List[float]):
        """Perform seasonal decomposition of time series"""
        try:
            # Create a pandas Series with regular frequency
            series = pd.Series(
                values,
                index=pd.to_datetime(timestamps)
            ).asfreq('D').ffill()  # Daily frequency, forward fill missing

            if len(series) > 7:  # Need at least 7 points for weekly seasonality
                decomposition = seasonal_decompose(series, model='additive', period=7)
                return {
                    'trend_strength': np.var(decomposition.trend) / np.var(series),
                    'seasonality_strength': np.var(decomposition.seasonal) / np.var(series)
                }
        except Exception as e:
            logger.warning(f"Seasonal decomposition failed: {e}")

        return None

    def _classify_trend(self, biometric_type: str, analysis_results: Dict) -> str:
        """Classify trend based on multiple analysis metrics"""
        linear = analysis_results['linear_trend']
        pct_change = analysis_results['percentage_change']
        volatility = analysis_results['volatility']

        # Get thresholds for this biometric type
        threshold = STABILITY_THRESHOLDS.get(biometric_type, 0.1)

        # Check if data is too volatile to determine trend
        if volatility > threshold * 2:
            return 'volatile'

        # Consider both linear trend and percentage change
        if abs(linear['slope']) < threshold and abs(pct_change) < threshold * 10:
            return 'stable'
        elif (linear['slope'] > 0 and pct_change > 0) or (linear['r_squared'] > 0.7 and linear['slope'] > 0):
            return 'increasing'
        elif (linear['slope'] < 0 and pct_change < 0) or (linear['r_squared'] > 0.7 and linear['slope'] < 0):
            return 'decreasing'
        else:
            return 'stable'

    def _store_trend(self, patient_id: int, biometric_type: str, trend: str):
        """Store trend analysis result in database"""
        try:
            record = BiometricTrend(
                patient_id=patient_id,
                biometric_type=biometric_type,
                trend=trend,
                analyzed_at=datetime.now()
            )
            self.session.merge(record)
            self.session.commit()
        except Exception as e:
            logger.error(f"Failed to store trend: {e}")
            self.session.rollback()


def main():
    """Run trend analysis for all patients"""
    logger.info("Starting trend analysis")
    analyzer = TrendAnalyzer()
    analyzer.analyze_all_patients()
    logger.info("Trend analysis completed")


if __name__ == "__main__":
    main()