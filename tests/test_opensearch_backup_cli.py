"""Tests for opensearch_backup.py CLI argument handling."""

import argparse
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add scripts directory to path to import opensearch_backup
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))

from opensearch_backup import build_connection_config, main, cmd_backup


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
        # Create a mock config file
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
            password=None,  # Not provided via CLI
            verify_ssl=True,
            timeout=None,  # Not provided via CLI
            config=config_file
        )

        config = build_connection_config(args)

        # CLI values take precedence
        assert config['url'] == 'https://cli-host:9200'
        assert config['username'] == 'cli_user'
        assert config['verify_ssl'] is True

        # Config file values used when CLI not provided
        assert config['password'] == 'config_pass'
        assert config['timeout'] == 30

    def test_config_file_only_backwards_compatible(self, tmp_path):
        """Config file only should work (backwards compatibility)."""
        config_file = tmp_path / 'config.yml'
        config_file.write_text('''
opensearch:
  url: https://config-host:9200
  username: config_user
  password: config_pass
  verify_ssl: true
  timeout: 45
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
        assert config['verify_ssl'] is True
        assert config['timeout'] == 45

    def test_error_when_no_url_provided(self, tmp_path, monkeypatch):
        """Should raise error when neither config nor --url provided and default config doesn't exist."""
        # Change to a temp directory where default config doesn't exist
        monkeypatch.chdir(tmp_path)

        args = argparse.Namespace(
            url=None,
            username=None,
            password=None,
            verify_ssl=None,
            timeout=None,
            config=None
        )

        with pytest.raises(SystemExit) as exc_info:
            build_connection_config(args)

        assert exc_info.value.code == 1

    def test_optional_auth_fields(self, tmp_path, monkeypatch):
        """Auth fields should be optional."""
        # Change to a temp directory where default config doesn't exist
        monkeypatch.chdir(tmp_path)

        args = argparse.Namespace(
            url='https://localhost:9200',
            username=None,
            password=None,
            verify_ssl=False,
            timeout=30,
            config=None
        )

        config = build_connection_config(args)

        assert config['url'] == 'https://localhost:9200'
        assert config.get('username') is None
        assert config.get('password') is None
        assert config['verify_ssl'] is False

    def test_verify_ssl_defaults_to_true(self, tmp_path):
        """verify_ssl should default to True when not specified."""
        config_file = tmp_path / 'config.yml'
        config_file.write_text('''
opensearch:
  url: https://localhost:9200
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

        assert config['verify_ssl'] is True

    def test_timeout_defaults_to_30(self, tmp_path):
        """timeout should default to 30 when not specified."""
        config_file = tmp_path / 'config.yml'
        config_file.write_text('''
opensearch:
  url: https://localhost:9200
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

        assert config['timeout'] == 30


class TestCLIArguments:
    """Tests for CLI argument parsing."""

    def test_url_argument_accepted(self):
        """Parser should accept --url argument."""
        with patch('sys.argv', ['opensearch_backup.py', '--url', 'https://localhost:9200', 'backup', '--index', 'zathras-results']):
            with patch('opensearch_backup.cmd_backup') as mock_cmd:
                mock_cmd.return_value = 0
                result = main()
                assert result == 0
                # Verify args were passed correctly
                call_args = mock_cmd.call_args[0][0]
                assert call_args.url == 'https://localhost:9200'

    def test_username_password_arguments_accepted(self):
        """Parser should accept --username and --password arguments."""
        with patch('sys.argv', ['opensearch_backup.py', '--url', 'https://localhost:9200',
                                '--username', 'testuser', '--password', 'testpass',
                                'backup', '--index', 'zathras-results']):
            with patch('opensearch_backup.cmd_backup') as mock_cmd:
                mock_cmd.return_value = 0
                result = main()
                assert result == 0
                call_args = mock_cmd.call_args[0][0]
                assert call_args.username == 'testuser'
                assert call_args.password == 'testpass'

    def test_verify_ssl_flag_accepted(self):
        """Parser should accept --verify-ssl flag."""
        with patch('sys.argv', ['opensearch_backup.py', '--url', 'https://localhost:9200',
                                '--verify-ssl', 'backup', '--index', 'zathras-results']):
            with patch('opensearch_backup.cmd_backup') as mock_cmd:
                mock_cmd.return_value = 0
                result = main()
                assert result == 0
                call_args = mock_cmd.call_args[0][0]
                assert call_args.verify_ssl is True

    def test_no_verify_ssl_flag_accepted(self):
        """Parser should accept --no-verify-ssl flag."""
        with patch('sys.argv', ['opensearch_backup.py', '--url', 'https://localhost:9200',
                                '--no-verify-ssl', 'backup', '--index', 'zathras-results']):
            with patch('opensearch_backup.cmd_backup') as mock_cmd:
                mock_cmd.return_value = 0
                result = main()
                assert result == 0
                call_args = mock_cmd.call_args[0][0]
                assert call_args.verify_ssl is False

    def test_timeout_argument_accepted(self):
        """Parser should accept --timeout argument."""
        with patch('sys.argv', ['opensearch_backup.py', '--url', 'https://localhost:9200',
                                '--timeout', '120', 'backup', '--index', 'zathras-results']):
            with patch('opensearch_backup.cmd_backup') as mock_cmd:
                mock_cmd.return_value = 0
                result = main()
                assert result == 0
                call_args = mock_cmd.call_args[0][0]
                assert call_args.timeout == 120

    def test_all_connection_args_together(self):
        """Parser should accept all connection arguments together."""
        with patch('sys.argv', ['opensearch_backup.py',
                                '--url', 'https://localhost:9200',
                                '--username', 'user',
                                '--password', 'pass',
                                '--no-verify-ssl',
                                '--timeout', '60',
                                'backup', '--index', 'zathras-results']):
            with patch('opensearch_backup.cmd_backup') as mock_cmd:
                mock_cmd.return_value = 0
                result = main()
                assert result == 0
                call_args = mock_cmd.call_args[0][0]
                assert call_args.url == 'https://localhost:9200'
                assert call_args.username == 'user'
                assert call_args.password == 'pass'
                assert call_args.verify_ssl is False
                assert call_args.timeout == 60


class TestRestoreFilenameParser:
    """Tests for restore command filename parsing logic."""

    def test_parse_filename_with_date_and_time_timestamp(self):
        """Should correctly strip _YYYYMMDD_HHMMSS timestamp from filename."""
        from opensearch_backup import parse_index_from_filename

        # Standard backup filename format
        result = parse_index_from_filename('zathras-results_20260614_194102.ndjson.gz')
        assert result == 'zathras-results'

    def test_parse_filename_with_timestamp_no_compression(self):
        """Should handle non-compressed files with timestamp."""
        from opensearch_backup import parse_index_from_filename

        result = parse_index_from_filename('zathras-timeseries_20231225_093045.ndjson')
        assert result == 'zathras-timeseries'

    def test_parse_filename_without_timestamp(self):
        """Should return filename as-is when no timestamp pattern found."""
        from opensearch_backup import parse_index_from_filename

        result = parse_index_from_filename('my-custom-index.ndjson.gz')
        assert result == 'my-custom-index'

    def test_parse_filename_with_underscores_in_index_name(self):
        """Should preserve underscores in index name while stripping timestamp."""
        from opensearch_backup import parse_index_from_filename

        result = parse_index_from_filename('my_complex_index_name_20260101_120000.ndjson.gz')
        assert result == 'my_complex_index_name'

    def test_parse_filename_with_partial_timestamp(self):
        """Should not strip malformed timestamp patterns."""
        from opensearch_backup import parse_index_from_filename

        # Only date, no time
        result = parse_index_from_filename('index_20260614.ndjson.gz')
        assert result == 'index_20260614'

        # Invalid date format
        result = parse_index_from_filename('index_2026614_194102.ndjson.gz')
        assert result == 'index_2026614_194102'

    def test_parse_filename_edge_case_timestamp_in_index_name(self):
        """Should only strip timestamp at end of filename, not in middle."""
        from opensearch_backup import parse_index_from_filename

        # Index name contains digits that look like timestamp
        result = parse_index_from_filename('backup_20260614_194102_data_20260615_120000.ndjson.gz')
        assert result == 'backup_20260614_194102_data'


class TestBackupCommandIntegration:
    """Tests for backup command using build_connection_config."""

    def test_backup_uses_cli_args(self, tmp_path, monkeypatch):
        """Backup command should use CLI args via build_connection_config."""
        # Change to temp directory to avoid picking up default config
        monkeypatch.chdir(tmp_path)

        # Create output directory
        output_dir = tmp_path / 'backups'
        output_dir.mkdir()

        args = argparse.Namespace(
            url='https://localhost:9200',
            username='testuser',
            password='testpass',
            verify_ssl=False,
            timeout=60,
            config=None,
            index='zathras-results',
            output=str(output_dir),
            compress=True,
            batch_size=1000,
            verbose=False
        )

        # Mock OpenSearchBackup class to avoid actual network calls
        with patch('opensearch_backup.OpenSearchBackup') as mock_backup_class:
            mock_backup = MagicMock()
            mock_backup_class.return_value = mock_backup

            # Mock get_index_stats to return test data
            mock_backup.get_index_stats.return_value = {
                'doc_count': 100,
                'size_bytes': 1024,
                'size_human': '1.00 KB'
            }

            # Mock backup_index to return test data
            mock_backup.backup_index.return_value = {
                'index': 'zathras-results',
                'documents': 100,
                'batches': 1,
                'file_path': str(output_dir / 'zathras-results_test.ndjson.gz'),
                'file_size': 512,
                'compressed': True
            }

            # Mock user confirmation
            with patch('opensearch_backup.confirm_action', return_value=True):
                result = cmd_backup(args)

            # Verify backup utility was initialized with CLI args
            assert mock_backup_class.called
            init_call = mock_backup_class.call_args
            assert init_call[1]['url'] == 'https://localhost:9200'
            assert init_call[1]['username'] == 'testuser'
            assert init_call[1]['password'] == 'testpass'
            assert init_call[1]['verify_ssl'] is False
            assert init_call[1]['timeout'] == 60

            # Verify backup was successful
            assert result == 0

    def test_backup_uses_config_file_when_no_cli_args(self, tmp_path):
        """Backup command should use config file when no CLI args provided."""
        # Create a config file
        config_file = tmp_path / 'config.yml'
        config_file.write_text('''
opensearch:
  url: https://config-host:9200
  username: config_user
  password: config_pass
  verify_ssl: true
  timeout: 45
''')

        output_dir = tmp_path / 'backups'
        output_dir.mkdir()

        args = argparse.Namespace(
            url=None,
            username=None,
            password=None,
            verify_ssl=None,
            timeout=None,
            config=config_file,
            index='zathras-results',
            output=str(output_dir),
            compress=True,
            batch_size=1000,
            verbose=False
        )

        with patch('opensearch_backup.OpenSearchBackup') as mock_backup_class:
            mock_backup = MagicMock()
            mock_backup_class.return_value = mock_backup

            mock_backup.get_index_stats.return_value = {
                'doc_count': 100,
                'size_bytes': 1024,
                'size_human': '1.00 KB'
            }

            mock_backup.backup_index.return_value = {
                'index': 'zathras-results',
                'documents': 100,
                'batches': 1,
                'file_path': str(output_dir / 'zathras-results_test.ndjson.gz'),
                'file_size': 512,
                'compressed': True
            }

            with patch('opensearch_backup.confirm_action', return_value=True):
                result = cmd_backup(args)

            # Verify backup utility was initialized with config file values
            init_call = mock_backup_class.call_args
            assert init_call[1]['url'] == 'https://config-host:9200'
            assert init_call[1]['username'] == 'config_user'
            assert init_call[1]['password'] == 'config_pass'
            assert init_call[1]['verify_ssl'] is True
            assert init_call[1]['timeout'] == 45

            assert result == 0
