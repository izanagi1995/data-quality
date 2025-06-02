# Data QA Library

A Python library for collecting data quality metrics from Databricks Unity Catalog tables using PySpark.

## Features

- **Current Table Metrics**: Collect comprehensive data quality metrics for the current state of a table
- **Historical Table Metrics**: Analyze all historical versions of a table using Delta Lake time travel
- **VACUUM Handling**: Gracefully handle versions that are no longer available due to Databricks VACUUM operations
- **Flexible Filtering**: Filter historical analysis by date ranges, version numbers, or sampling rates
- **Performance Optimized**: Efficient PySpark operations designed for large-scale data processing

## Installation

```bash
pip install data-qa-lib
```

## Quick Start

### Basic Table Metrics

```python
from pyspark.sql import SparkSession
from data_qa_lib.metrics import TableMetrics

# Initialize Spark session and metrics collector
spark = SparkSession.builder.getOrCreate()
metrics = TableMetrics(spark)

# Collect metrics for current table state
result = metrics.collect_metrics("catalog.schema.table_name")

print(f"Row count: {result.row_count}")
print(f"Column count: {result.column_count}")
print(f"Null counts: {result.null_counts}")
print(f"Duplicate rows: {result.duplicate_count}")
```

### Historical Table Metrics

```python
# Collect metrics for all available historical versions
historical_result = metrics.collect_historical_metrics("catalog.schema.table_name")

print(f"Total versions found: {historical_result.total_versions_found}")
print(f"Successful collections: {historical_result.successful_collections}")
print(f"VACUUM-affected versions: {historical_result.vacuum_affected_versions}")

# Access metrics for each version
for version_metrics in historical_result.metrics_by_version:
    version_info = version_metrics.version_metadata
    print(f"Version {version_info.version}: {version_metrics.row_count} rows")
```

### Advanced Historical Analysis

```python
# Filter by date range
historical_result = metrics.collect_historical_metrics(
    "catalog.schema.table_name",
    start_date="2024-01-01",
    end_date="2024-12-31"
)

# Filter by version range
historical_result = metrics.collect_historical_metrics(
    "catalog.schema.table_name",
    version_start=10,
    version_end=50
)

# Sample every 5th version for large tables
historical_result = metrics.collect_historical_metrics(
    "catalog.schema.table_name",
    sample_rate=5,
    max_versions=100
)
```

## Data Models

### MetricsResult

Contains comprehensive data quality metrics for a single table version:

- `table_name`, `catalog`, `schema`: Table identification
- `timestamp`: Collection timestamp
- `row_count`, `column_count`: Basic table dimensions
- `null_counts`: Null value counts per column
- `unique_counts`: Approximate unique value counts per column
- `numeric_stats`: Statistical measures for numeric columns (avg, stddev, percentiles)
- `duplicate_count`: Number of duplicate rows
- `version_metadata`: Version information (for historical collections)

### HistoricalMetricsResult

Contains results from historical metrics collection:

- `total_versions_found`: Number of table versions discovered
- `successful_collections`: Number of versions successfully analyzed
- `failed_collections`: Number of versions that failed analysis
- `vacuum_affected_versions`: List of versions unavailable due to VACUUM
- `metrics_by_version`: List of MetricsResult objects for each successful version
- `error_summary`: Detailed error information by category

### VersionMetadata

Contains metadata about a specific table version:

- `version`: Version number
- `timestamp`: Version creation timestamp
- `operation`: Delta Lake operation that created the version
- `user_id`, `user_name`: User who performed the operation
- `commit_info`: Additional commit metadata

## VACUUM Handling

Databricks automatically removes old table versions through VACUUM operations to manage storage costs. This library handles these scenarios gracefully:

- **Automatic Detection**: Identifies when versions are unavailable due to VACUUM
- **Graceful Skipping**: Continues processing remaining versions when some fail
- **Clear Logging**: Provides informative warnings about inaccessible versions
- **Error Categorization**: Separates VACUUM errors from other analysis failures

```python
# Example handling VACUUM-affected tables
result = metrics.collect_historical_metrics("catalog.schema.large_table")

if result.vacuum_affected_versions:
    print(f"Note: {len(result.vacuum_affected_versions)} versions were cleaned up by VACUUM")
    print("This is normal behavior for cost optimization in Databricks")

# Check detailed error information
for error in result.error_summary["vacuum_errors"]:
    print(f"VACUUM-related: {error}")
```

## Performance Considerations

- Use `sample_rate` parameter for tables with many versions
- Set `max_versions` to limit processing for very large table histories
- Historical analysis processes versions in parallel where possible
- Consider date range filtering for focused analysis periods

## Requirements

- Python >= 3.11
- PySpark >= 3.5.0
- Databricks runtime with Delta Lake support
- Access to Unity Catalog tables

## Contributing

Contributions are welcome! Please ensure all tests pass and follow the existing code style.

```bash
# Run tests
pytest tests/

# Check code style
ruff check data_qa_lib/
```