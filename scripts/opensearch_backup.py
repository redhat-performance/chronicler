#!/usr/bin/env python3
"""
OpenSearch Backup and Restore Utility

Backs up and restores data from OpenSearch indices (zathras-results, zathras-timeseries).
Uses scroll API for efficient backup of large datasets and bulk API for restore.

Usage:
    # Backup single index
    ./opensearch_backup.py backup --index zathras-results --output backups/

    # Backup both indices
    ./opensearch_backup.py backup --index both --output backups/

    # Restore from backup
    ./opensearch_backup.py restore --input backups/zathras-results_20260612.ndjson

    # List backups
    ./opensearch_backup.py list --directory backups/
"""

import argparse
import base64
import gzip
import json
import logging
import ssl
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse


class OpenSearchBackup:
    """Handles backup and restore of OpenSearch indices."""

    def __init__(
        self,
        url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_token: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: int = 60,
    ):
        """
        Initialize OpenSearch backup utility.

        Args:
            url: OpenSearch endpoint URL
            username: Username for basic auth
            password: Password for basic auth
            auth_token: Bearer token for authentication
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
        """
        if not isinstance(url, str):
            raise ValueError(f"URL must be a string, got: {type(url).__name__}")

        url = url.strip().rstrip('/')
        if not url or not url.startswith(('http://', 'https://')):
            raise ValueError("URL must start with http:// or https://")

        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError("URL must include a host")

        self.url = url
        self.username = username
        self.password = password
        self.auth_token = auth_token
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)

    def _build_headers(self) -> Dict[str, str]:
        """Build HTTP headers for requests."""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        if self.auth_token:
            headers['Authorization'] = f'Bearer {self.auth_token}'
        elif self.username and self.password:
            credentials = f"{self.username}:{self.password}"
            encoded = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
            headers['Authorization'] = f'Basic {encoded}'

        return headers

    def _make_request(
        self,
        endpoint: str,
        method: str = 'GET',
        data: Optional[Any] = None,
        is_bulk: bool = False
    ) -> Dict[str, Any]:
        """
        Make HTTP request to OpenSearch.

        Args:
            endpoint: API endpoint path
            method: HTTP method
            data: Request payload
            is_bulk: Whether this is a bulk request

        Returns:
            Response data as dictionary
        """
        url = urljoin(self.url, endpoint)
        headers = self._build_headers()

        if is_bulk:
            headers['Content-Type'] = 'application/x-ndjson'

        # Prepare request data
        if data is not None:
            if is_bulk:
                request_data = data.encode('utf-8') if isinstance(data, str) else data
            else:
                request_data = json.dumps(data).encode('utf-8')
        else:
            request_data = None

        req = urllib.request.Request(
            url,
            data=request_data,
            headers=headers,
            method=method
        )

        # Create SSL context if needed
        context = None
        if not self.verify_ssl and url.startswith('https'):
            context = ssl._create_unverified_context()

        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=context) as response:
                response_body = response.read().decode('utf-8')
                # Handle empty-body responses (e.g., successful HEAD requests)
                if not response_body:
                    return {}
                return json.loads(response_body)
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            raise urllib.error.HTTPError(e.url, e.code, f"HTTP {e.code} error: {error_body}", e.headers, None)
        except urllib.error.URLError as e:
            raise Exception(f"Connection error: {e.reason}")

    def get_index_stats(self, index: str) -> Dict[str, Any]:
        """
        Get statistics for an index.

        Args:
            index: Index name

        Returns:
            Dict with 'doc_count', 'size_bytes', 'size_human'
        """
        try:
            # Get count
            count_response = self._make_request(f'/{index}/_count')
            doc_count = count_response.get('count', 0)

            # Get size from stats
            stats_response = self._make_request(f'/{index}/_stats')
            indices = stats_response.get('indices', {})
            index_stats = indices.get(index, {})
            primaries = index_stats.get('primaries', {})
            store = primaries.get('store', {})
            size_bytes = store.get('size_in_bytes', 0)

            # Calculate human-readable size
            size_human = OpenSearchBackup._format_bytes(size_bytes)

            return {
                'doc_count': doc_count,
                'size_bytes': size_bytes,
                'size_human': size_human
            }
        except Exception as e:
            self.logger.error(f"Failed to get stats for {index}: {e}")
            raise

    @staticmethod
    def _format_bytes(size_bytes: int) -> str:
        """Format bytes to human-readable size."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

    def backup_index(
        self,
        index: str,
        output_path: Path,
        compress: bool = True,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Backup an index using scroll API.

        Args:
            index: Index name to backup
            output_path: Output file path
            compress: Whether to gzip compress the output
            batch_size: Number of documents per scroll batch

        Returns:
            Dict with backup statistics
        """
        self.logger.info(f"Starting backup of index '{index}'...")

        # Initialize scroll
        scroll_timeout = '5m'
        query = {
            'size': batch_size,
            'query': {'match_all': {}},
            '_source': True
        }

        try:
            # Initial scroll request
            response = self._make_request(
                f'/{index}/_search?scroll={scroll_timeout}',
                method='POST',
                data=query
            )

            scroll_id = response.get('_scroll_id')
            hits = response.get('hits', {}).get('hits', [])
            total_docs = response.get('hits', {}).get('total', {}).get('value', 0)

            # Open output file
            if compress:
                output_file = gzip.open(output_path, 'wt', encoding='utf-8')
            else:
                output_file = open(output_path, 'w', encoding='utf-8')

            docs_written = 0
            batches = 0

            try:
                while hits:
                    batches += 1
                    for hit in hits:
                        # Write document in NDJSON format
                        # Store both _id and _source for restore
                        doc_record = {
                            '_id': hit['_id'],
                            '_source': hit['_source']
                        }
                        output_file.write(json.dumps(doc_record) + '\n')
                        docs_written += 1

                    # Progress update
                    if docs_written % 10000 == 0:
                        self.logger.info(f"  Backed up {docs_written:,} / {total_docs:,} documents...")

                    # Get next batch
                    scroll_response = self._make_request(
                        '/_search/scroll',
                        method='POST',
                        data={
                            'scroll': scroll_timeout,
                            'scroll_id': scroll_id
                        }
                    )
                    scroll_id = scroll_response.get('_scroll_id')
                    hits = scroll_response.get('hits', {}).get('hits', [])

            finally:
                output_file.close()
                # Clear scroll context
                try:
                    self._make_request(
                        '/_search/scroll',
                        method='DELETE',
                        data={'scroll_id': scroll_id}
                    )
                except:
                    pass

            file_size = output_path.stat().st_size
            self.logger.info(f"✓ Backup complete: {docs_written:,} documents written to {output_path}")
            self.logger.info(f"  Backup file size: {OpenSearchBackup._format_bytes(file_size)}")

            return {
                'index': index,
                'documents': docs_written,
                'batches': batches,
                'file_path': str(output_path),
                'file_size': file_size,
                'compressed': compress
            }

        except Exception as e:
            self.logger.error(f"Backup failed: {e}")
            # Clean up partial backup file
            if output_path.exists():
                output_path.unlink()
            raise

    def restore_index(
        self,
        index: str,
        input_path: Path,
        bulk_size: int = 500
    ) -> Dict[str, Any]:
        """
        Restore an index from backup using bulk API.

        Note: The target index must exist before restore. Create it manually
        with appropriate mappings and settings to ensure schema compatibility.

        Args:
            index: Target index name (must already exist)
            input_path: Backup file path
            bulk_size: Number of documents per bulk request

        Returns:
            Dict with restore statistics
        """
        self.logger.info(f"Starting restore to index '{index}' from {input_path}...")

        # Check if index exists
        try:
            self._make_request(f'/{index}', method='HEAD')
            self.logger.info(f"Index '{index}' exists")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                # Index not found - provide clear guidance
                raise Exception(
                    f"Index '{index}' does not exist. Please create it manually with appropriate "
                    f"mappings and settings before restoring. Automatic index creation has been "
                    f"disabled to prevent data loss from missing schema definitions."
                )
            else:
                # Other HTTP errors (401, 403, 5xx) - re-raise with context
                raise Exception(f"Failed to check index '{index}': HTTP {e.code} - {e.msg}")
        except Exception as e:
            # Network errors, SSL errors, etc - re-raise with context
            raise Exception(f"Failed to connect to OpenSearch while checking index '{index}': {e}")

        # Open backup file
        if input_path.suffix == '.gz':
            input_file = gzip.open(input_path, 'rt', encoding='utf-8')
        else:
            input_file = open(input_path, 'r', encoding='utf-8')

        docs_restored = 0
        docs_failed = 0
        batches = 0
        batch = []

        try:
            for line_num, line in enumerate(input_file, 1):
                if not line.strip():
                    continue

                try:
                    doc_record = json.loads(line)
                    batch.append(doc_record)

                    if len(batch) >= bulk_size:
                        # Send bulk request
                        stats = self._bulk_index(index, batch)
                        docs_restored += stats['successful']
                        docs_failed += stats['failed']
                        batches += 1

                        if docs_restored % 10000 == 0:
                            self.logger.info(f"  Restored {docs_restored:,} documents...")

                        batch = []

                except json.JSONDecodeError as e:
                    self.logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    docs_failed += 1

            # Send remaining documents
            if batch:
                stats = self._bulk_index(index, batch)
                docs_restored += stats['successful']
                docs_failed += stats['failed']
                batches += 1

        finally:
            input_file.close()

        self.logger.info(f"✓ Restore complete: {docs_restored:,} documents restored")
        if docs_failed > 0:
            self.logger.warning(f"  {docs_failed:,} documents failed to restore")

        return {
            'index': index,
            'documents_restored': docs_restored,
            'documents_failed': docs_failed,
            'batches': batches
        }

    def _bulk_index(self, index: str, documents: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Bulk index documents.

        Args:
            index: Target index
            documents: List of document records with '_id' and '_source'

        Returns:
            Dict with 'successful' and 'failed' counts
        """
        # Build bulk request body
        bulk_lines = []
        for doc in documents:
            doc_id = doc.get('_id')
            source = doc.get('_source', {})

            # Action line
            action = {'index': {'_index': index, '_id': doc_id}}
            bulk_lines.append(json.dumps(action))
            # Document line
            bulk_lines.append(json.dumps(source))

        bulk_body = '\n'.join(bulk_lines) + '\n'

        try:
            response = self._make_request(
                '/_bulk',
                method='POST',
                data=bulk_body,
                is_bulk=True
            )

            # Count successes and failures
            items = response.get('items', [])
            successful = 0
            failed = 0

            for item in items:
                index_result = item.get('index', {})
                status = index_result.get('status', 0)
                if status in [200, 201]:
                    successful += 1
                else:
                    failed += 1
                    error = index_result.get('error', {})
                    if error:
                        self.logger.debug(f"Failed to index doc {index_result.get('_id')}: {error.get('reason')}")

            return {'successful': successful, 'failed': failed}

        except Exception as e:
            self.logger.error(f"Bulk index failed: {e}")
            return {'successful': 0, 'failed': len(documents)}


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load OpenSearch configuration from YAML file."""
    try:
        import yaml
        with open(config_path) as f:
            config = yaml.safe_load(f)
            return config.get('opensearch', {})
    except ImportError:
        print("ERROR: PyYAML not installed. Install with: pip install PyYAML", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to load config from {config_path}: {e}", file=sys.stderr)
        sys.exit(1)


def build_connection_config(args) -> Dict[str, Any]:
    """
    Build OpenSearch connection configuration from CLI args and/or config file.

    CLI arguments take precedence over config file values.
    At least one of --config or --url must be provided.

    Args:
        args: Parsed command-line arguments

    Returns:
        Dict with connection configuration (url, username, password, verify_ssl, timeout)

    Raises:
        SystemExit: If neither --config nor --url is provided
    """
    # Start with empty config
    config = {}

    # Load from config file if provided (default to config/export_config.yml if not specified)
    config_path = args.config if args.config else Path('config/export_config.yml')

    if config_path.exists():
        config = load_config(config_path)

    # Override with CLI arguments (if provided)
    if args.url is not None:
        config['url'] = args.url

    if args.username is not None:
        config['username'] = args.username

    if args.password is not None:
        config['password'] = args.password

    if args.verify_ssl is not None:
        config['verify_ssl'] = args.verify_ssl

    if args.timeout is not None:
        config['timeout'] = args.timeout

    # Validate: must have at least a URL
    if 'url' not in config:
        print(
            "ERROR: No OpenSearch URL provided. Use either:\n"
            "  --config <path>  (to load from config file)\n"
            "  --url <url>      (to specify URL directly)\n",
            file=sys.stderr
        )
        sys.exit(1)

    # Apply defaults
    config.setdefault('verify_ssl', True)
    config.setdefault('timeout', 30)

    return config


def confirm_action(message: str) -> bool:
    """Ask user for confirmation."""
    while True:
        response = input(f"{message} [y/N]: ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no', '']:
            return False
        print("Please answer 'y' or 'n'")


def cmd_backup(args):
    """Execute backup command."""
    # Build connection config from CLI args and/or config file
    config = build_connection_config(args)

    # Initialize backup utility
    backup = OpenSearchBackup(
        url=config['url'],
        username=config.get('username'),
        password=config.get('password'),
        auth_token=config.get('auth_token'),
        verify_ssl=config.get('verify_ssl', True),
        timeout=config.get('timeout', 30)
    )

    # Determine indices to backup
    if args.index == 'both':
        indices = [
            config.get('summary_index', 'zathras-results'),
            config.get('timeseries_index', 'zathras-timeseries')
        ]
    else:
        indices = [args.index]

    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get stats and estimate size
    total_docs = 0
    total_size = 0
    stats_by_index = {}

    print("\n" + "=" * 70)
    print("BACKUP ESTIMATION")
    print("=" * 70)

    for index in indices:
        try:
            stats = backup.get_index_stats(index)
            stats_by_index[index] = stats
            total_docs += stats['doc_count']
            total_size += stats['size_bytes']

            print(f"\nIndex: {index}")
            print(f"  Documents: {stats['doc_count']:,}")
            print(f"  Index size: {stats['size_human']}")

        except Exception as e:
            print(f"\nERROR: Failed to get stats for '{index}': {e}", file=sys.stderr)
            return 1

    # Estimate backup size (compressed is roughly 20-30% of index size)
    estimated_size = total_size * 0.25 if args.compress else total_size

    print("\n" + "-" * 70)
    print(f"Total documents: {total_docs:,}")
    print(f"Total index size: {OpenSearchBackup._format_bytes(total_size)}")
    print(f"Estimated backup size: {OpenSearchBackup._format_bytes(int(estimated_size))}")
    if args.compress:
        print("  (with gzip compression)")
    print("=" * 70 + "\n")

    # Confirm with user
    if not confirm_action("Proceed with backup?"):
        print("Backup cancelled.")
        return 0

    # Perform backup
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results = []

    for index in indices:
        # Generate output filename
        ext = '.ndjson.gz' if args.compress else '.ndjson'
        output_file = output_dir / f"{index}_{timestamp}{ext}"

        try:
            result = backup.backup_index(
                index=index,
                output_path=output_file,
                compress=args.compress,
                batch_size=args.batch_size
            )
            results.append(result)
        except Exception as e:
            print(f"\nERROR: Backup of '{index}' failed: {e}", file=sys.stderr)
            return 1

    # Summary
    print("\n" + "=" * 70)
    print("BACKUP SUMMARY")
    print("=" * 70)
    for result in results:
        print(f"\nIndex: {result['index']}")
        print(f"  Documents: {result['documents']:,}")
        print(f"  File: {result['file_path']}")
        print(f"  Size: {OpenSearchBackup._format_bytes(result['file_size'])}")
    print("=" * 70 + "\n")

    return 0


def cmd_restore(args):
    """Execute restore command."""
    # Load config
    config = load_config(args.config)

    # Initialize backup utility
    backup = OpenSearchBackup(
        url=config['url'],
        username=config.get('username'),
        password=config.get('password'),
        auth_token=config.get('auth_token'),
        verify_ssl=config.get('verify_ssl', True),
        timeout=config.get('timeout', 30)
    )

    input_path = Path(args.input)

    if not input_path.exists():
        print(f"ERROR: Backup file not found: {input_path}", file=sys.stderr)
        return 1

    # Determine target index
    if args.index:
        target_index = args.index
    else:
        # Try to infer from filename
        filename = input_path.stem
        if filename.endswith('.ndjson'):
            filename = filename[:-7]
        # Remove timestamp if present
        parts = filename.rsplit('_', 1)
        target_index = parts[0]

    print("\n" + "=" * 70)
    print("RESTORE CONFIRMATION")
    print("=" * 70)
    print(f"\nSource file: {input_path}")
    print(f"Target index: {target_index}")
    print(f"File size: {OpenSearchBackup._format_bytes(input_path.stat().st_size)}")
    print("\n" + "=" * 70 + "\n")

    if not confirm_action("Proceed with restore?"):
        print("Restore cancelled.")
        return 0

    # Perform restore
    try:
        result = backup.restore_index(
            index=target_index,
            input_path=input_path,
            bulk_size=args.bulk_size
        )

        print("\n" + "=" * 70)
        print("RESTORE SUMMARY")
        print("=" * 70)
        print(f"\nIndex: {result['index']}")
        print(f"  Documents restored: {result['documents_restored']:,}")
        if result['documents_failed'] > 0:
            print(f"  Documents failed: {result['documents_failed']:,}")
        print("=" * 70 + "\n")

        return 0

    except Exception as e:
        print(f"\nERROR: Restore failed: {e}", file=sys.stderr)
        return 1


def cmd_list(args):
    """List available backups."""
    backup_dir = Path(args.directory)

    if not backup_dir.exists():
        print(f"Backup directory does not exist: {backup_dir}")
        return 0

    # Find backup files
    backups = sorted(backup_dir.glob('*.ndjson*'))

    if not backups:
        print(f"No backup files found in {backup_dir}")
        return 0

    print("\n" + "=" * 70)
    print("AVAILABLE BACKUPS")
    print("=" * 70 + "\n")

    for backup_file in backups:
        size = backup_file.stat().st_size
        # Format size
        size_str = OpenSearchBackup._format_bytes(size)
        print(f"{backup_file.name}")
        print(f"  Size: {size_str}")
        print(f"  Path: {backup_file}")
        print()

    print("=" * 70 + "\n")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='Backup and restore OpenSearch indices',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        '--config',
        type=Path,
        default=None,
        help='Path to export config file (default: config/export_config.yml)'
    )

    # Connection parameters (optional - override config file)
    parser.add_argument(
        '--url',
        type=str,
        default=None,
        help='OpenSearch URL (e.g., https://localhost:9200)'
    )

    parser.add_argument(
        '--username',
        type=str,
        default=None,
        help='Username for basic authentication'
    )

    parser.add_argument(
        '--password',
        type=str,
        default=None,
        help='Password for basic authentication'
    )

    ssl_group = parser.add_mutually_exclusive_group()
    ssl_group.add_argument(
        '--verify-ssl',
        action='store_true',
        dest='verify_ssl',
        default=None,
        help='Verify SSL certificates (default: true)'
    )

    ssl_group.add_argument(
        '--no-verify-ssl',
        action='store_false',
        dest='verify_ssl',
        help='Skip SSL certificate verification'
    )

    parser.add_argument(
        '--timeout',
        type=int,
        default=None,
        help='Request timeout in seconds (default: 30)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    subparsers.required = True

    # Backup command
    backup_parser = subparsers.add_parser('backup', help='Backup index(es)')
    backup_parser.add_argument(
        '--index',
        choices=['zathras-results', 'zathras-timeseries', 'both'],
        default='both',
        help='Index to backup (default: both)'
    )
    backup_parser.add_argument(
        '--output',
        type=str,
        default='backups/',
        help='Output directory for backups (default: backups/)'
    )
    backup_parser.add_argument(
        '--compress',
        action='store_true',
        default=True,
        help='Compress backup with gzip (default: True)'
    )
    backup_parser.add_argument(
        '--no-compress',
        action='store_false',
        dest='compress',
        help='Do not compress backup'
    )
    backup_parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Documents per scroll batch (default: 1000)'
    )
    backup_parser.set_defaults(func=cmd_backup)

    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore from backup')
    restore_parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Backup file to restore from'
    )
    restore_parser.add_argument(
        '--index',
        type=str,
        help='Target index name (default: infer from filename)'
    )
    restore_parser.add_argument(
        '--bulk-size',
        type=int,
        default=500,
        help='Documents per bulk request (default: 500)'
    )
    restore_parser.set_defaults(func=cmd_restore)

    # List command
    list_parser = subparsers.add_parser('list', help='List available backups')
    list_parser.add_argument(
        '--directory',
        type=str,
        default='backups/',
        help='Backup directory (default: backups/)'
    )
    list_parser.set_defaults(func=cmd_list)

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )

    # Execute command
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        return 130
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
