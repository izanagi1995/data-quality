"""
Data Quality Metrics Collection

Provides classes and functions for collecting data quality metrics
from Databricks Unity Catalog tables using PySpark.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, isnan, isnull, when,
    approx_count_distinct, avg, stddev, mean,
    expr, percentile_approx, min as spark_min, max as spark_max
)
from pyspark.sql.types import (
    IntegerType, LongType, FloatType, DoubleType, 
    DecimalType, NumericType
)
from pyspark.sql.utils import AnalysisException
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
    unique_counts: Dict[str, int]
    numeric_stats: Dict[str, Dict[str, float]]
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
            
            # Unique counts for each column using approx_count_distinct
            unique_counts = self._calculate_unique_counts(df)
            
            # Numeric statistics for numeric columns
            numeric_stats = self._calculate_numeric_stats(df)
            
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
                unique_counts=unique_counts,
                numeric_stats=numeric_stats,
                duplicate_count=duplicate_count,
                metrics=additional_metrics
            )
            
        except AnalysisException as e:
            # AI-NOTE: Handle table access errors (table not found, permission issues, etc.)
            raise AnalysisException(f"Failed to access table {table_name}. Verify table exists and you have permissions: {e}")
        except Exception as e:
            # AI-NOTE: Handle other unexpected errors during metrics collection
            raise RuntimeError(f"Unexpected error during metrics collection for table {table_name}: {e}")
    
    def _calculate_null_counts(self, df: DataFrame) -> Dict[str, int]:
        """
        Calculate null counts for each column using a single aggregation.
        
        Handles both numeric and non-numeric columns appropriately.
        """
        # AI-NOTE: Build aggregation expressions for all columns in single pass for performance
        agg_exprs = []
        for field in df.schema.fields:
            column_name = field.name
            # AI-NOTE: For numeric columns, check both isNull and isnan; for others, only isNull
            if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                # Numeric columns can have both null and NaN values
                null_expr = spark_sum(when(col(column_name).isNull() | isnan(col(column_name)), 1).otherwise(0))
            else:
                # Non-numeric columns only check for null values
                null_expr = spark_sum(when(col(column_name).isNull(), 1).otherwise(0))
            
            agg_exprs.append(null_expr.alias(f"null_count_{column_name}"))
        
        # Execute single aggregation for all columns
        result = df.agg(*agg_exprs).collect()[0]
        
        # Extract results into dictionary
        null_counts = {}
        for field in df.schema.fields:
            column_name = field.name
            null_counts[column_name] = result[f"null_count_{column_name}"]
        
        return null_counts
    
    def _calculate_duplicate_count(self, df: DataFrame) -> int:
        """Calculate the number of duplicate rows."""
        total_rows = df.count()
        distinct_rows = df.distinct().count()
        return total_rows - distinct_rows
    
    def _calculate_unique_counts(self, df: DataFrame) -> Dict[str, int]:
        """
        Calculate approximate unique value counts for each column using approx_count_distinct.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary mapping column names to approximate unique counts
        """
        # AI-NOTE: Using approx_count_distinct for performance on large datasets
        unique_counts = {}
        for column in df.columns:
            try:
                unique_count = df.select(approx_count_distinct(col(column))).collect()[0][0]
                unique_counts[column] = unique_count
            except (AnalysisException, TypeError) as e:
                # AI-NOTE: Handle cases where approx_count_distinct fails (e.g., complex types, unsupported data types)
                unique_counts[column] = 0
        return unique_counts
    
    def _calculate_numeric_stats(self, df: DataFrame) -> Dict[str, Dict[str, float]]:
        """
        Calculate comprehensive statistics for numeric columns.
        
        Includes: avg, stddev, mean, median, min, max, p90, p95, p99
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary mapping numeric column names to their statistics
        """
        # AI-NOTE: Filter to only numeric columns using proper type checking
        numeric_columns = [field.name for field in df.schema.fields 
                          if isinstance(field.dataType, (IntegerType, LongType, FloatType, DoubleType, DecimalType))]
        
        numeric_stats = {}
        
        for column in numeric_columns:
            try:
                # AI-NOTE: Collect all statistics in a single aggregation for performance
                stats_df = df.select(
                    avg(col(column)).alias("avg"),
                    stddev(col(column)).alias("stddev"),
                    mean(col(column)).alias("mean"),
                    spark_min(col(column)).alias("min"),
                    spark_max(col(column)).alias("max"),
                    percentile_approx(col(column), 0.5).alias("median"),
                    percentile_approx(col(column), 0.9).alias("p90"),
                    percentile_approx(col(column), 0.95).alias("p95"),
                    percentile_approx(col(column), 0.99).alias("p99")
                ).collect()[0]
                
                numeric_stats[column] = {
                    "avg": float(stats_df["avg"]) if stats_df["avg"] is not None else 0.0,
                    "stddev": float(stats_df["stddev"]) if stats_df["stddev"] is not None else 0.0,
                    "mean": float(stats_df["mean"]) if stats_df["mean"] is not None else 0.0,
                    "median": float(stats_df["median"]) if stats_df["median"] is not None else 0.0,
                    "min": float(stats_df["min"]) if stats_df["min"] is not None else 0.0,
                    "max": float(stats_df["max"]) if stats_df["max"] is not None else 0.0,
                    "p90": float(stats_df["p90"]) if stats_df["p90"] is not None else 0.0,
                    "p95": float(stats_df["p95"]) if stats_df["p95"] is not None else 0.0,
                    "p99": float(stats_df["p99"]) if stats_df["p99"] is not None else 0.0
                }
            except (AnalysisException, ArithmeticError) as e:
                # AI-NOTE: Handle edge cases like all-null numeric columns or division by zero in stddev
                numeric_stats[column] = {
                    "avg": 0.0, "stddev": 0.0, "mean": 0.0, "median": 0.0,
                    "min": 0.0, "max": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0
                }
        
        return numeric_stats
    
    def _calculate_additional_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate additional data quality metrics."""
        # AI-NOTE: Keep existing functionality for backward compatibility
        return {
            "distinct_count": df.distinct().count(),
            "schema_info": [{"name": field.name, "type": str(field.dataType)} 
                           for field in df.schema.fields]
        }