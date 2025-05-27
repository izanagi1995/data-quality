"""
Data Quality Metrics Collection

Provides classes and functions for collecting data quality metrics
from Databricks Unity Catalog tables using PySpark.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, isnan, isnull
from pydantic import BaseModel
from datetime import datetime


class MetricsResult(BaseModel):
    """Pydantic model for metrics collection results."""
    table_name: str
    catalog: str
    schema: str
    timestamp: datetime
    row_count: int
    column_count: int
    null_counts: Dict[str, int]
    duplicate_count: int
    metrics: Dict[str, Any]


class TableMetrics:
    """
    Class to collect data quality metrics from a single Unity Catalog table.
    
    Provides an easy interface for end users to assess data quality
    on their Databricks tables without complex PySpark operations.
    """
    
    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize the TableMetrics collector.
        
        Args:
            spark: SparkSession instance. If None, will get or create one.
        """
        self.spark = spark or SparkSession.builder.getOrCreate()
    
    def collect_metrics(self, table_name: str) -> MetricsResult:
        """
        Collect comprehensive data quality metrics for a table.
        
        Args:
            table_name: Full table name in format 'catalog.schema.table'
            
        Returns:
            MetricsResult containing all collected metrics
            
        Raises:
            ValueError: If table name format is invalid
            Exception: If table cannot be accessed or metrics collection fails
        """
        # AI-NOTE: Validate table name format for Unity Catalog
        if table_name.count('.') != 2:
            raise ValueError("Table name must be in format 'catalog.schema.table'")
        
        catalog, schema, table = table_name.split('.')
        
        try:
            # AI-NOTE: Load table using Unity Catalog three-part naming
            df = self.spark.table(table_name)
            
            # Basic metrics collection
            row_count = df.count()
            column_count = len(df.columns)
            
            # Null counts for each column
            null_counts = self._calculate_null_counts(df)
            
            # Duplicate row count
            duplicate_count = self._calculate_duplicate_count(df)
            
            # Additional custom metrics
            additional_metrics = self._calculate_additional_metrics(df)
            
            return MetricsResult(
                table_name=table,
                catalog=catalog,
                schema=schema,
                timestamp=datetime.now(),
                row_count=row_count,
                column_count=column_count,
                null_counts=null_counts,
                duplicate_count=duplicate_count,
                metrics=additional_metrics
            )
            
        except Exception as e:
            # AI-NOTE: Specific error handling for table access and metrics collection
            raise Exception(f"Failed to collect metrics for table {table_name}: {e}")
    
    def _calculate_null_counts(self, df: DataFrame) -> Dict[str, int]:
        """Calculate null counts for each column."""
        null_counts = {}
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts[column] = null_count
        return null_counts
    
    def _calculate_duplicate_count(self, df: DataFrame) -> int:
        """Calculate the number of duplicate rows."""
        total_rows = df.count()
        distinct_rows = df.distinct().count()
        return total_rows - distinct_rows
    
    def _calculate_additional_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate additional data quality metrics."""
        # AI-TODO: Implement more sophisticated metrics
        # (e.g., data type validation, pattern matching, range checks)
        return {
            "distinct_count": df.distinct().count(),
            "schema_info": [{"name": field.name, "type": str(field.dataType)} 
                           for field in df.schema.fields]
        }