"""
Tests for data quality metrics collection functionality.

Tests both current and historical metrics collection with proper
error handling for VACUUM-affected table versions.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from pyspark.sql.utils import AnalysisException

from data_qa_lib.metrics import (
    TableMetrics, MetricsResult, HistoricalMetricsResult, 
    VersionMetadata
)


class TestTableMetrics:
    """Test cases for TableMetrics class."""
    
    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession for testing."""
        spark = Mock()
        spark.sql.return_value = Mock()
        spark.table.return_value = Mock()
        return spark
    
    @pytest.fixture
    def table_metrics(self, mock_spark):
        """Create TableMetrics instance with mocked SparkSession."""
        return TableMetrics(spark=mock_spark)
    
    def test_collect_metrics_invalid_table_name(self, table_metrics):
        """Test that invalid table names raise ValueError."""
        with pytest.raises(ValueError, match="Table name must be in format 'catalog.schema.table'"):
            table_metrics.collect_metrics("invalid_table_name")
    
    def test_collect_historical_metrics_invalid_table_name(self, table_metrics):
        """Test that invalid table names raise ValueError for historical collection."""
        with pytest.raises(ValueError, match="Table name must be in format 'catalog.schema.table'"):
            table_metrics.collect_historical_metrics("invalid_table_name")
    
    @patch('data_qa_lib.metrics.TableMetrics._get_table_versions')
    def test_collect_historical_metrics_no_versions(self, mock_get_versions, table_metrics):
        """Test handling when no table versions are found."""
        mock_get_versions.return_value = []
        
        with pytest.raises(Exception, match="No table versions found"):
            table_metrics.collect_historical_metrics("catalog.schema.table")
    
    @patch('data_qa_lib.metrics.TableMetrics._get_table_versions')
    @patch('data_qa_lib.metrics.TableMetrics._collect_version_metrics')
    def test_collect_historical_metrics_success(self, mock_collect_version, mock_get_versions, table_metrics):
        """Test successful historical metrics collection."""
        # AI-NOTE: Setup mock data for successful collection
        mock_versions = [
            {'version': 2, 'timestamp': datetime.now(), 'operation': 'WRITE'},
            {'version': 1, 'timestamp': datetime.now(), 'operation': 'CREATE'}
        ]
        mock_get_versions.return_value = mock_versions
        
        mock_metrics = MetricsResult(
            table_name="test_table",
            catalog="test_catalog", 
            schema="test_schema",
            timestamp=datetime.now(),
            row_count=100,
            column_count=5,
            null_counts={},
            unique_counts={},
            numeric_stats={},
            duplicate_count=0,
            metrics={}
        )
        mock_collect_version.return_value = mock_metrics
        
        result = table_metrics.collect_historical_metrics("test_catalog.test_schema.test_table")
        
        assert isinstance(result, HistoricalMetricsResult)
        assert result.total_versions_found == 2
        assert result.successful_collections == 2
        assert result.failed_collections == 0
        assert len(result.vacuum_affected_versions) == 0
        assert len(result.metrics_by_version) == 2
    
    @patch('data_qa_lib.metrics.TableMetrics._get_table_versions')
    @patch('data_qa_lib.metrics.TableMetrics._collect_version_metrics')
    def test_collect_historical_metrics_vacuum_errors(self, mock_collect_version, mock_get_versions, table_metrics):
        """Test handling of VACUUM-affected versions."""
        # AI-NOTE: Setup test data with VACUUM errors
        mock_versions = [
            {'version': 3, 'timestamp': datetime.now(), 'operation': 'WRITE'},
            {'version': 2, 'timestamp': datetime.now(), 'operation': 'WRITE'},
            {'version': 1, 'timestamp': datetime.now(), 'operation': 'CREATE'}
        ]
        mock_get_versions.return_value = mock_versions
        
        # AI-NOTE: Simulate VACUUM error for version 1, success for others
        def side_effect(table_name, version_info, catalog, schema, table):
            if version_info['version'] == 1:
                raise AnalysisException("Version 1 is not available due to vacuum")
            return MetricsResult(
                table_name="test_table",
                catalog="test_catalog",
                schema="test_schema", 
                timestamp=datetime.now(),
                row_count=100,
                column_count=5,
                null_counts={},
                unique_counts={},
                numeric_stats={},
                duplicate_count=0,
                metrics={}
            )
        
        mock_collect_version.side_effect = side_effect
        
        result = table_metrics.collect_historical_metrics("test_catalog.test_schema.test_table")
        
        assert result.total_versions_found == 3
        assert result.successful_collections == 2
        assert result.failed_collections == 1
        assert 1 in result.vacuum_affected_versions
        assert len(result.error_summary["vacuum_errors"]) == 1
    
    @patch('data_qa_lib.metrics.TableMetrics._get_table_versions')
    @patch('data_qa_lib.metrics.TableMetrics._collect_version_metrics')
    def test_collect_historical_metrics_with_sampling(self, mock_collect_version, mock_get_versions, table_metrics):
        """Test historical collection with sampling rate."""
        # AI-NOTE: Create 10 versions for sampling test
        mock_versions = [
            {'version': i, 'timestamp': datetime.now(), 'operation': 'WRITE'}
            for i in range(10, 0, -1)
        ]
        mock_get_versions.return_value = mock_versions
        
        mock_metrics = MetricsResult(
            table_name="test_table",
            catalog="test_catalog",
            schema="test_schema",
            timestamp=datetime.now(),
            row_count=100,
            column_count=5,
            null_counts={},
            unique_counts={},
            numeric_stats={},
            duplicate_count=0,
            metrics={}
        )
        mock_collect_version.return_value = mock_metrics
        
        # AI-NOTE: Test sampling every 3rd version
        result = table_metrics.collect_historical_metrics(
            "test_catalog.test_schema.test_table", 
            sample_rate=3
        )
        
        # With 10 versions and sample_rate=3, should process versions at indices 0,3,6,9 = 4 versions
        expected_processed = len(mock_versions[::3])
        assert result.successful_collections == expected_processed
    
    @patch('data_qa_lib.metrics.TableMetrics._get_table_versions')
    @patch('data_qa_lib.metrics.TableMetrics._collect_version_metrics')
    def test_collect_historical_metrics_max_versions_limit(self, mock_collect_version, mock_get_versions, table_metrics):
        """Test max_versions parameter limits processing."""
        # AI-NOTE: Create more versions than the limit
        mock_versions = [
            {'version': i, 'timestamp': datetime.now(), 'operation': 'WRITE'}
            for i in range(20, 0, -1)
        ]
        mock_get_versions.return_value = mock_versions
        
        mock_metrics = MetricsResult(
            table_name="test_table",
            catalog="test_catalog",
            schema="test_schema",
            timestamp=datetime.now(),
            row_count=100,
            column_count=5,
            null_counts={},
            unique_counts={},
            numeric_stats={},
            duplicate_count=0,
            metrics={}
        )
        mock_collect_version.return_value = mock_metrics
        
        # AI-NOTE: Limit to 5 versions
        result = table_metrics.collect_historical_metrics(
            "test_catalog.test_schema.test_table",
            max_versions=5
        )
        
        assert result.successful_collections == 5
        assert result.total_versions_found == 5  # Should be limited before processing


class TestVersionMetadata:
    """Test cases for VersionMetadata model."""
    
    def test_version_metadata_creation(self):
        """Test creating VersionMetadata with various fields."""
        metadata = VersionMetadata(
            version=5,
            timestamp=datetime.now(),
            operation="WRITE",
            user_id="user123",
            user_name="Test User",
            commit_info={"readVersion": 4, "isolationLevel": "Serializable"}
        )
        
        assert metadata.version == 5
        assert metadata.operation == "WRITE"
        assert metadata.user_id == "user123"
        assert metadata.commit_info["readVersion"] == 4
    
    def test_version_metadata_optional_fields(self):
        """Test VersionMetadata with minimal required fields."""
        metadata = VersionMetadata()
        
        assert metadata.version is None
        assert metadata.timestamp is None
        assert metadata.operation is None


class TestHistoricalMetricsResult:
    """Test cases for HistoricalMetricsResult model."""
    
    def test_historical_metrics_result_creation(self):
        """Test creating HistoricalMetricsResult."""
        result = HistoricalMetricsResult(
            table_name="test_table",
            catalog="test_catalog",
            schema="test_schema",
            collection_timestamp=datetime.now(),
            total_versions_found=10,
            successful_collections=8,
            failed_collections=2,
            vacuum_affected_versions=[1, 2],
            metrics_by_version=[],
            error_summary={"vacuum_errors": ["Version 1: not available"], "analysis_errors": [], "other_errors": []}
        )
        
        assert result.table_name == "test_table"
        assert result.total_versions_found == 10
        assert result.successful_collections == 8
        assert result.failed_collections == 2
        assert 1 in result.vacuum_affected_versions
        assert len(result.error_summary["vacuum_errors"]) == 1