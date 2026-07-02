from setuptools import setup, find_packages

setup(
    name="kafka-anomaly-detection",
    version="1.0.0",
    description="Production real-time anomaly detection using Kafka & PySpark (sub-200ms latency)",
    author="Mamidi Sri Teja",
    author_email="sri.teja@example.com",
    url="https://github.com/yourusername/Kafka-Anomaly-Detection",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "kafka-python>=2.0.0",
        "pyspark>=3.3.0",
        "pandas>=2.0.0",
        "scikit-learn>=1.3.0",
        "boto3>=1.26.0",
        "pyyaml>=6.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.9",
        "Topic :: System :: Monitoring",
    ],
)
