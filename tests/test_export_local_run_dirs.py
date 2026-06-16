"""Tests for export_local_run_dirs.py CLI."""

import argparse
import csv
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open
from io import StringIO

import pytest

# Add scripts directory to path to import export_local_run_dirs
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))

from export_local_run_dirs import build_connection_config, export_local_run_dirs


class TestBuildConnectionConfig:
    """Tests for build_connection_config helper function."""

    def test_cli_args_only(self):
        """CLI arguments should work without a config file."""
        args = argparse.Namespace(
            url='https://localhost:9200',
            username='testuser',
            password='testpass',
            verify_ssl=True,
            timeout=60,
            config=None
        )

        config = build_connection_config(args)

        assert config['url'] == 'https://localhost:9200'
        assert config['username'] == 'testuser'
        assert config['password'] == 'testpass'
        assert config['verify_ssl'] is True
        assert config['timeout'] == 60

    def test_cli_overrides_config_file(self, tmp_path):
        """CLI arguments should override config file values."""
        config_file = tmp_path / 'config.yml'
        config_file.write_text('''
opensearch:
  url: https://config-host:9200
  username: config_user
  password: config_pass
  verify_ssl: false
  timeout: 30
''')

        args = argparse.Namespace(
            url='https://cli-host:9200',
            username='cli_user',
            password=None,
            verify_ssl=True,
            timeout=None,
            config=config_file
        )

        config = build_connection_config(args)

        assert config['url'] == 'https://cli-host:9200'
        assert config['username'] == 'cli_user'
        assert config['verify_ssl'] is True
        assert config['password'] == 'config_pass'
        assert config['timeout'] == 30

    def test_config_file_only(self, tmp_path):
        """Config file only should work."""
        config_file = tmp_path / 'config.yml'
        config_file.write_text('''
opensearch:
  url: https://config-host:9200
  username: config_user
  password: config_pass
''')

        args = argparse.Namespace(
            url=None,
            username=None,
            password=None,
            verify_ssl=None,
            timeout=None,
            config=config_file
        )

        config = build_connection_config(args)

        assert config['url'] == 'https://config-host:9200'
        assert config['username'] == 'config_user'
        assert config['password'] == 'config_pass'

    def test_error_when_no_url_provided(self, monkeypatch):
        """Should exit with error when no URL provided."""
        args = argparse.Namespace(
            url=None,
            username=None,
            password=None,
            verify_ssl=None,
            timeout=None,
            config=Path('nonexistent.yml')
        )

        with pytest.raises(SystemExit) as exc_info:
            build_connection_config(args)

        assert exc_info.value.code == 1


class TestExportLocalRunDirs:
    """Tests for export_local_run_dirs function."""

    def test_query_and_export(self, tmp_path):
        """Should query OpenSearch and export unique values to CSV."""
        output_file = tmp_path / 'output.csv'

        # Mock OpenSearch response with aggregation buckets
        mock_opensearch = MagicMock()
        mock_opensearch._make_request.return_value = {
            'aggregations': {
                'unique_local_run_dirs': {
                    'buckets': [
                        {'key': '/path/to/run1', 'doc_count': 5},
                        {'key': '/path/to/run2', 'doc_count': 3},
                        {'key': '/path/to/run3', 'doc_count': 1}
                    ]
                }
            }
        }

        result = export_local_run_dirs(
            opensearch=mock_opensearch,
            index='zathras-results',
            output_path=output_file
        )

        # Verify OpenSearch was queried with aggregation
        mock_opensearch._make_request.assert_called_once()
        call_args = mock_opensearch._make_request.call_args
        assert call_args[0][0] == '/zathras-results/_search'
        assert 'aggs' in call_args[1]['data']

        # Verify CSV file was created with correct content
        assert output_file.exists()

        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3
        assert rows[0]['local_run_dir'] == '/path/to/run1'
        assert rows[0]['doc_count'] == '5'
        assert rows[1]['local_run_dir'] == '/path/to/run2'
        assert rows[2]['local_run_dir'] == '/path/to/run3'

        # Verify result summary
        assert result['unique_count'] == 3
        assert result['output_file'] == str(output_file)

    def test_handles_empty_results(self, tmp_path):
        """Should handle case when no documents have local_run_dir."""
        output_file = tmp_path / 'empty.csv'

        mock_opensearch = MagicMock()
        mock_opensearch._make_request.return_value = {
            'aggregations': {
                'unique_local_run_dirs': {
                    'buckets': []
                }
            }
        }

        result = export_local_run_dirs(
            opensearch=mock_opensearch,
            index='zathras-results',
            output_path=output_file
        )

        # Should still create CSV with headers
        assert output_file.exists()

        with open(output_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 0
        assert result['unique_count'] == 0

    def test_handles_connection_error(self, tmp_path):
        """Should raise exception on connection error."""
        output_file = tmp_path / 'output.csv'

        mock_opensearch = MagicMock()
        mock_opensearch._make_request.side_effect = Exception('Connection refused')

        with pytest.raises(Exception, match='Connection refused'):
            export_local_run_dirs(
                opensearch=mock_opensearch,
                index='zathras-results',
                output_path=output_file
            )

        # Should not create output file on error
        assert not output_file.exists()
