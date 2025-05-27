"""
Data Quality Assessment Library for Databricks

A PySpark-based library for collecting and evaluating data quality metrics
from Databricks Unity Catalog.
"""

__version__ = "0.0.1"
__author__ = "Nicolas Surleraux"

# AI-NOTE: Main package exports - expose key classes for end users
from .client import DataQAClient
from .metrics import TableMetrics

__all__ = ["DataQAClient", "TableMetrics"]