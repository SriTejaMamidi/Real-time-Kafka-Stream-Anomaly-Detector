"""
Isolation Forest based anomaly detector for streaming data.
"""

import logging
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import numpy as np
import joblib

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """Anomaly detector using Isolation Forest."""

    def __init__(
            self,
            contamination: float = 0.05,
            threshold: float = 0.8,
            n_estimators: int = 100,
            random_state: int = 42
    ):
        """Initialize anomaly detector.

        Args:
            contamination: Expected proportion of anomalies
            threshold: Decision threshold for anomaly score
            n_estimators: Number of isolation trees
            random_state: Random seed
        """
        self.contamination = contamination
        self.threshold = threshold
        self.n_estimators = n_estimators
        self.random_state = random_state

        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=random_state,
            n_jobs=-1
        )

        self.scaler = StandardScaler()
        self.is_trained = False

        logger.info("AnomalyDetector initialized")

    def train(self, X_train: np.ndarray):
        """Train detector on historical data.

        Args:
            X_train: Training data (n_samples, n_features)
        """
        logger.info(f"Training on {X_train.shape[0]} samples")

        # Fit scaler
        X_scaled = self.scaler.fit_transform(X_train)

        # Fit model
        self.model.fit(X_scaled)

        self.is_trained = True
        logger.info("Training completed")

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict anomaly scores.

        Args:
            X: Input data (n_samples, n_features)

        Returns:
            Anomaly scores (0-1, higher = more anomalous)
        """
        if not self.is_trained:
            raise ValueError("Model not trained. Call train() first.")

        X_scaled = self.scaler.transform(X)
        scores = -self.model.score_samples(X_scaled)

        # Normalize to [0, 1]
        scores = (scores - scores.min()) / (scores.max() - scores.min() + 1e-8)

        return scores

    def detect(self, X: np.ndarray) -> np.ndarray:
        """Detect anomalies (binary).

        Args:
            X: Input data

        Returns:
            Binary anomaly flags (1 = anomaly, -1 = normal)
        """
        if not self.is_trained:
            raise ValueError("Model not trained")

        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled)

    def detect_with_scores(self, X: np.ndarray):
        """Get both predictions and scores.

        Args:
            X: Input data

        Returns:
            Tuple of (predictions, scores)
        """
        predictions = self.detect(X)
        scores = self.predict(X)

        return predictions, scores

    def save(self, path: str):
        """Save trained model.

        Args:
            path: Path to save model
        """
        joblib.dump(
            {"model": self.model, "scaler": self.scaler},
            path
        )
        logger.info(f"Model saved to {path}")

    def load(self, path: str):
        """Load trained model.

        Args:
            path: Path to load model from
        """
        data = joblib.load(path)
        self.model = data["model"]
        self.scaler = data["scaler"]
        self.is_trained = True
        logger.info(f"Model loaded from {path}")
