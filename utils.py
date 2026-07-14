"""
Utility functions for anomaly detection pipeline.
"""

import logging
import yaml
from pathlib import Path


def setup_logging(log_file: str = "logs/anomaly_detection.log"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def load_config(config_path: str) -> dict:
    """Load YAML configuration.
    
    Args:
        config_path: Path to config file
        
    Returns:
        Configuration dictionary
    """
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_feature_columns() -> list:
    """Get anomaly detection feature columns."""
    return [
        "cpu_usage",
        "memory_usage",
        "network_io",
        "disk_io"
    ]
