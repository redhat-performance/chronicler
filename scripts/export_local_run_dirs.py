#!/usr/bin/env python3
"""
Export Unique local_run_dir Values from OpenSearch

Queries the OpenSearch zathras-results index to extract all unique
test_configuration.parameters.local_run_dir values and exports them to a CSV file.

Uses aggregations API for efficient extraction without scanning all documents.

Usage:
    # Using config file
    ./export_local_run_dirs.py --config config/export_config.yml --output local_run_dirs.csv

    # Using CLI arguments
    ./export_local_run_dirs.py --url https://localhost:9200 --username user --password pass \\
        --output local_run_dirs.csv

    # With custom index
    ./export_local_run_dirs.py --config config/export_config.yml --index custom-index \\
        --output local_run_dirs.csv
"""

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path to import OpenSearchBackup
sys.path.insert(0, str(Path(__file__).parent))

from opensearch_backup import OpenSearchBackup, load_config


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


def export_local_run_dirs(
    opensearch: OpenSearchBackup,
    index: str,
    output_path: Path,
    max_buckets: int = 10000
) -> Dict[str, Any]:
    """
    Export unique local_run_dir values to CSV.

    Args:
        opensearch: OpenSearchBackup instance for making requests
        index: Index name to query
        output_path: Path to output CSV file
        max_buckets: Maximum number of unique values to return

    Returns:
        Dict with export statistics

    Raises:
        Exception: On connection or query errors
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Querying index '{index}' for unique local_run_dir values...")

    # Build aggregation query to get unique local_run_dir values
    # Note: field is already mapped as keyword type, so no .keyword suffix needed
    query = {
        'size': 0,  # Don't need document hits, only aggregations
        'aggs': {
            'unique_local_run_dirs': {
                'terms': {
                    'field': 'test_configuration.parameters.local_run_dir',
                    'size': max_buckets,
                    'order': {'_key': 'asc'}  # Sort alphabetically
                }
            }
        }
    }

    # Execute query
    response = opensearch._make_request(
        f'/{index}/_search',
        method='POST',
        data=query
    )

    # Extract buckets from aggregation response
    buckets = response.get('aggregations', {}).get('unique_local_run_dirs', {}).get('buckets', [])

    logger.info(f"Found {len(buckets)} unique local_run_dir values")

    # Write to CSV
    logger.info(f"Writing results to {output_path}...")

    with open(output_path, 'w', newline='') as csvfile:
        fieldnames = ['local_run_dir', 'doc_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for bucket in buckets:
            writer.writerow({
                'local_run_dir': bucket['key'],
                'doc_count': bucket['doc_count']
            })

    logger.info(f"✓ Export complete: {len(buckets)} unique values written to {output_path}")

    return {
        'unique_count': len(buckets),
        'output_file': str(output_path)
    }


def main():
    parser = argparse.ArgumentParser(
        description='Export unique local_run_dir values from OpenSearch to CSV',
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

    # Export parameters
    parser.add_argument(
        '--index',
        type=str,
        default='zathras-results',
        help='Index to query (default: zathras-results)'
    )

    parser.add_argument(
        '--output',
        type=Path,
        default='local_run_dirs.csv',
        help='Output CSV file path (default: local_run_dirs.csv)'
    )

    parser.add_argument(
        '--max-buckets',
        type=int,
        default=10000,
        help='Maximum unique values to return (default: 10000)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )

    # Build connection config
    config = build_connection_config(args)

    # Initialize OpenSearch connection
    opensearch = OpenSearchBackup(
        url=config['url'],
        username=config.get('username'),
        password=config.get('password'),
        auth_token=config.get('auth_token'),
        verify_ssl=config.get('verify_ssl', True),
        timeout=config.get('timeout', 30)
    )

    try:
        # Export local_run_dir values
        result = export_local_run_dirs(
            opensearch=opensearch,
            index=args.index,
            output_path=args.output,
            max_buckets=args.max_buckets
        )

        print(f"\n✓ Export complete:")
        print(f"  Unique values: {result['unique_count']}")
        print(f"  Output file: {result['output_file']}")

        return 0

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
