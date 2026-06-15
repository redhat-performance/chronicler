"""Tests for opensearch_backup.py CLI argument handling."""

import argparse
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Add scripts directory to path to import opensearch_backup
sys.path.insert(0, str(Path(__file__).parent.parent / 'scripts'))

from opensearch_backup import build_connection_config


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

    def test_error_when_no_url_provided(self):
        """Should raise error when neither config nor --url provided."""
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

    def test_optional_auth_fields(self):
        """Auth fields should be optional."""
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
