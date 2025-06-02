"""
Data Quality Metrics Collection

Provides classes and functions for collecting data quality metrics
from Databricks Unity Catalog tables using PySpark.
"""

from typing import Dict, Any, List, Optional, Union
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
import logging


class VersionMetadata(BaseModel):
    """Metadata for a specific table version."""
    version: Optional[int] = None
    timestamp: Optional[datetime] = None
    operation: Optional[str] = None
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    commit_info: Optional[Dict[str, Any]] = None


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
    version_metadata: Optional[VersionMetadata] = None


class HistoricalMetricsResult(BaseModel):
    """Pydantic model for historical metrics collection results."""
    table_name: str
    catalog: str
    schema: str
    collection_timestamp: datetime
    total_versions_found: int
    successful_collections: int
    failed_collections: int
    vacuum_affected_versions: List[int]
    metrics_by_version: List[MetricsResult]
    error_summary: Dict[str, List[str]]


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
        self.logger = logging.getLogger(__name__)
    
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
    
    def collect_historical_metrics(
        self, 
        table_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        version_start: Optional[int] = None,
        version_end: Optional[int] = None,
        max_versions: int = 100,
        sample_rate: Optional[int] = None
    ) -> HistoricalMetricsResult:
        """
        Collect data quality metrics for all available historical versions of a table.
        
        Handles VACUUM-affected versions gracefully by skipping inaccessible versions
        and continuing with the collection process.
        
        Args:
            table_name: Full table name in format 'catalog.schema.table'
            start_date: Start date for filtering versions (ISO format: 'YYYY-MM-DD')
            end_date: End date for filtering versions (ISO format: 'YYYY-MM-DD')
            version_start: Start version number for filtering
            version_end: End version number for filtering
            max_versions: Maximum number of versions to process (default: 100)
            sample_rate: If specified, process every Nth version (e.g., 5 = every 5th version)
            
        Returns:
            HistoricalMetricsResult containing metrics for all accessible versions
            
        Raises:
            ValueError: If table name format is invalid or invalid date ranges
            Exception: If table cannot be accessed or no versions are found
        """
        # AI-NOTE: Validate table name format for Unity Catalog
        if table_name.count('.') != 2:
            raise ValueError("Table name must be in format 'catalog.schema.table'")
        
        catalog, schema, table = table_name.split('.')
        
        try:
            # AI-NOTE: Get table history to find available versions
            versions_info = self._get_table_versions(table_name, start_date, end_date, version_start, version_end)
            
            if not versions_info:
                raise Exception(f"No table versions found for {table_name} with specified criteria")
            
            # AI-NOTE: Apply sampling and limit constraints
            if sample_rate and sample_rate > 1:
                versions_info = versions_info[::sample_rate]
            
            if len(versions_info) > max_versions:
                self.logger.warning(f"Found {len(versions_info)} versions, limiting to {max_versions}")
                versions_info = versions_info[:max_versions]
            
            # AI-NOTE: Collect metrics for each version with VACUUM error handling
            successful_metrics = []
            failed_versions = []
            vacuum_affected = []
            error_summary = {"vacuum_errors": [], "analysis_errors": [], "other_errors": []}
            
            for version_info in versions_info:
                try:
                    version_metrics = self._collect_version_metrics(
                        table_name, version_info, catalog, schema, table
                    )
                    successful_metrics.append(version_metrics)
                    
                except AnalysisException as e:
                    error_msg = str(e).lower()
                    version_num = version_info.get('version', -1)
                    
                    if any(keyword in error_msg for keyword in ['not available', 'vacuum', 'retention', 'cleaned up']):
                        # AI-NOTE: Handle VACUUM-related errors gracefully
                        vacuum_affected.append(version_num)
                        error_summary["vacuum_errors"].append(f"Version {version_num}: {str(e)}")
                        self._log_vacuum_warning(table_name, version_num, e)
                    else:
                        # AI-NOTE: Handle other analysis errors
                        failed_versions.append(version_num)
                        error_summary["analysis_errors"].append(f"Version {version_num}: {str(e)}")
                        self.logger.error(f"Analysis error for {table_name} version {version_num}: {e}")
                        
                except Exception as e:
                    # AI-NOTE: Handle unexpected errors
                    version_num = version_info.get('version', -1)
                    failed_versions.append(version_num)
                    error_summary["other_errors"].append(f"Version {version_num}: {str(e)}")
                    self.logger.error(f"Unexpected error for {table_name} version {version_num}: {e}")
            
            # AI-NOTE: Log summary of collection results
            total_versions = len(versions_info)
            successful_count = len(successful_metrics)
            failed_count = len(failed_versions) + len(vacuum_affected)
            
            self.logger.info(
                f"Historical metrics collection for {table_name}: "
                f"{successful_count}/{total_versions} successful, "
                f"{len(vacuum_affected)} VACUUM-affected, "
                f"{len(failed_versions)} other failures"
            )
            
            return HistoricalMetricsResult(
                table_name=table,
                catalog=catalog,
                schema=schema,
                collection_timestamp=datetime.now(),
                total_versions_found=total_versions,
                successful_collections=successful_count,
                failed_collections=failed_count,
                vacuum_affected_versions=vacuum_affected,
                metrics_by_version=successful_metrics,
                error_summary=error_summary
            )
            
        except Exception as e:
            raise RuntimeError(f"Failed to collect historical metrics for table {table_name}: {e}")
    
    def _get_table_versions(self, table_name: str, start_date: Optional[str], end_date: Optional[str], 
                           version_start: Optional[int], version_end: Optional[int]) -> List[Dict[str, Any]]:
        """
        Get available table versions using DESCRIBE HISTORY.
        
        Returns list of version dictionaries with metadata, sorted by version number
        in descending order (newest first). This ordering ensures that when limits
        are applied, the most recent versions are prioritized for analysis.
        """
        try:
            # AI-NOTE: Use DESCRIBE HISTORY to get table version information
            history_df = self.spark.sql(f"DESCRIBE HISTORY {table_name}")
            versions = history_df.collect()
            
            # AI-NOTE: Convert to list of dictionaries and sort by version descending
            versions_info = []
            for row in versions:
                version_dict = row.asDict()
                
                # AI-NOTE: Apply filtering based on provided criteria
                version_num = version_dict.get('version')
                timestamp = version_dict.get('timestamp')
                
                # Filter by version range
                if version_start is not None and version_num < version_start:
                    continue
                if version_end is not None and version_num > version_end:
                    continue
                
                # Filter by date range
                if start_date and timestamp:
                    if timestamp.date() < datetime.fromisoformat(start_date).date():
                        continue
                if end_date and timestamp:
                    if timestamp.date() > datetime.fromisoformat(end_date).date():
                        continue
                
                versions_info.append(version_dict)
            
            # AI-NOTE: Sort by version number (newest first)
            return sorted(versions_info, key=lambda x: x.get('version', 0), reverse=True)
            
        except Exception as e:
            self.logger.error(f"Failed to get table versions for {table_name}: {e}")
            raise
    
    def _collect_version_metrics(self, table_name: str, version_info: Dict[str, Any], 
                                catalog: str, schema: str, table: str) -> MetricsResult:
        """
        Collect metrics for a specific table version.
        
        Args:
            table_name: Full table name
            version_info: Dictionary containing version metadata
            catalog, schema, table: Table name components
            
        Returns:
            MetricsResult with version-specific metrics
        """
        version_num = version_info.get('version')
        
        # AI-NOTE: Use Delta Lake time travel syntax to access specific version
        if version_num is not None:
            versioned_table = f"{table_name}@v{version_num}"
        else:
            versioned_table = table_name
        
        # AI-NOTE: Load the specific version of the table
        df = self.spark.table(versioned_table)
        
        # AI-NOTE: Collect standard metrics for this version
        row_count = df.count()
        column_count = len(df.columns)
        null_counts = self._calculate_null_counts(df)
        unique_counts = self._calculate_unique_counts(df)
        numeric_stats = self._calculate_numeric_stats(df)
        duplicate_count = self._calculate_duplicate_count(df)
        additional_metrics = self._calculate_additional_metrics(df)
        
        # AI-NOTE: Create version metadata object
        version_metadata = VersionMetadata(
            version=version_info.get('version'),
            timestamp=version_info.get('timestamp'),
            operation=version_info.get('operation'),
            user_id=version_info.get('userId'),
            user_name=version_info.get('userName'),
            commit_info={
                k: v for k, v in version_info.items() 
                if k not in ['version', 'timestamp', 'operation', 'userId', 'userName']
            }
        )
        
        return MetricsResult(
            table_name=table,
            catalog=catalog,
            schema=schema,
            timestamp=version_info.get('timestamp', datetime.now()),
            row_count=row_count,
            column_count=column_count,
            null_counts=null_counts,
            unique_counts=unique_counts,
            numeric_stats=numeric_stats,
            duplicate_count=duplicate_count,
            metrics=additional_metrics,
            version_metadata=version_metadata
        )
    
    def _log_vacuum_warning(self, table_name: str, version: Union[int, str], error: Exception) -> None:
        """
        Log a warning message for VACUUM-affected versions.
        
        Provides clear guidance about why versions might be unavailable.
        """
        self.logger.warning(
            f"Version {version} of table {table_name} is not available, "
            f"likely due to VACUUM operation. This is expected behavior when "
            f"Databricks has cleaned up old table versions. Error: {error}"
        )
    
    def _calculate_additional_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate additional data quality metrics."""
        # AI-NOTE: Keep existing functionality for backward compatibility
        return {
            "distinct_count": df.distinct().count(),
            "schema_info": [{"name": field.name, "type": str(field.dataType)} 
                           for field in df.schema.fields]
        }