"""
HammerDB Transactional Database Benchmark Processor

Processes HammerDB TPROC-C results for MariaDB, MSSQL, and PostgreSQL. All three
databases are driven by the same hammerdb-wrapper CSV output: a sweep across
virtual user ("connection") counts, reporting TPM (transactions per minute) for
each configuration.

Expects CSV with connection,TPM,Start_Date,End_Date columns (ISO 8601 timestamps).
Rows with a missing connection or TPM value (e.g. HammerDB failed to install/run
for that configuration) are skipped rather than raising, so a partially-broken
run still reports whatever data is available instead of failing outright.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base_processor import BaseProcessor
from .timestamp_utils import validate_iso8601_timestamp
from ..schema import (
    PrimaryMetric,
    Run,
    StatisticalSummary,
    TimeSeriesPoint,
    create_run_key,
    create_sequence_key,
)
from ..utils.parser_utils import parse_csv_timeseries

logger = logging.getLogger(__name__)


class HammerDBProcessor(BaseProcessor):
    """
    Shared processor for HammerDB TPROC-C results.

    Subclasses only need to set get_test_name() to the database-specific
    registry name (e.g. "hammerdb_mariadb").
    """

    def parse_runs(self, extracted_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse HammerDB runs into object-based structure.

        Returns:
            {
                "run_1": {
                    "run_number": 0,
                    "status": "PASS",
                    "metrics": {...},
                    "timeseries": {"sequence_0": {...}, ...}
                }
            }
        """
        test_name = self.get_test_name()
        csv_file = self._find_csv_file(extracted_result, test_name)

        if not csv_file:
            logger.warning(f"HammerDB CSV file not found for {test_name}")
            return {}

        rows = parse_csv_timeseries(str(csv_file), delimiter=',', skip_comments=True)
        connection_data = self._filter_valid_rows(rows, test_name)

        if not connection_data:
            logger.warning(f"No valid connection/TPM rows found in {csv_file}")
            return {}

        runs = {create_run_key(0): self._build_run_object(connection_data)}

        logger.info(
            f"Parsed {len(connection_data)} HammerDB connection configuration(s) for {test_name}"
        )
        return runs

    def _find_csv_file(self, extracted_result: Dict[str, Any], test_name: str) -> Optional[Path]:
        """
        Find results_{test_name}.csv.

        The wrapper nests the CSV one directory below the timestamped result dir
        (e.g. hammerdb_mariadb_<ts>/mariadb/results_hammerdb_mariadb.csv), so the
        generic archive_handler top-level glob (files['results_csv']) misses it.
        Fall back to a recursive search from extracted_path.
        """
        files = extracted_result.get("files") or {}
        if files.get("results_csv"):
            return Path(files["results_csv"])

        extracted_path = extracted_result.get("extracted_path")
        if not extracted_path:
            return None

        matches = sorted(Path(extracted_path).rglob(f"results_{test_name}.csv"))
        return matches[0] if matches else None

    def _filter_valid_rows(
        self, rows: List[Dict[str, Any]], test_name: str
    ) -> List[Dict[str, Any]]:
        """
        Keep only rows with numeric connection/TPM values.

        HammerDB writes a row per configured connection count even when the
        underlying test failed to produce a result (e.g. hammerdbcli missing),
        leaving connection/TPM blank. Those rows are dropped here rather than
        raising, so a partially-broken run still reports whatever configurations
        did produce data. Start_Date/End_Date, by contrast, are always required
        and validated -- a missing or malformed timestamp raises ProcessorError,
        since that indicates CSV corruption rather than a normal HammerDB
        partial-failure mode.
        """
        valid = []
        for row in rows:
            connection = row.get('connection')
            tpm = row.get('TPM')
            if not isinstance(connection, (int, float)) or not isinstance(tpm, (int, float)):
                continue

            row['Start_Date'] = validate_iso8601_timestamp(
                row.get('Start_Date'), f"connection={connection}:", test_name="HammerDB"
            )
            row['End_Date'] = validate_iso8601_timestamp(
                row.get('End_Date'), f"connection={connection}:", test_name="HammerDB"
            )

            valid.append(row)

        return valid

    def _build_run_object(self, connection_data: List[Dict[str, Any]]) -> Run:
        """Convert filtered connection/TPM rows into a Run object."""
        peak = max(connection_data, key=lambda r: r['TPM'])

        metrics: Dict[str, Any] = {
            'peak_connection_config': peak['connection'],
            'peak_tpm': peak['TPM'],
        }
        for row in connection_data:
            metrics[f"throughput_connections_{row['connection']}_tpm"] = row['TPM']

        timeseries = {}
        for idx, row in enumerate(connection_data):
            timeseries[create_sequence_key(idx)] = TimeSeriesPoint(
                timestamp=row['Start_Date'],
                metrics={
                    'connections': row['connection'],
                    'tpm': row['TPM'],
                },
            )

        config = {
            'connection_configurations': [row['connection'] for row in connection_data],
        }

        return Run(
            run_number=0,
            status="PASS",  # HammerDB CSV doesn't provide explicit pass/fail
            metrics=metrics,
            configuration=config,
            timeseries=timeseries if timeseries else None,
        )

    def _extract_primary_metrics(
        self, runs: Dict[str, Any],
        overall_stats: Optional[StatisticalSummary]
    ) -> Optional[List[PrimaryMetric]]:
        """Peak TPM across all connection configurations is the primary metric."""
        if not runs:
            return None

        first_run = list(runs.values())[0]
        metrics = first_run.metrics if hasattr(first_run, 'metrics') else None
        if not metrics or 'peak_tpm' not in metrics:
            return None

        return [PrimaryMetric(name='Peak-TPM', value=float(metrics['peak_tpm']), unit='TPM')]


class HammerDBMariaDBProcessor(HammerDBProcessor):
    """Processor for HammerDB TPROC-C results against MariaDB."""

    def get_test_name(self) -> str:
        return "hammerdb_mariadb"


class HammerDBMSSQLProcessor(HammerDBProcessor):
    """Processor for HammerDB TPROC-C results against MSSQL."""

    def get_test_name(self) -> str:
        return "hammerdb_mssql"


class HammerDBPostgresProcessor(HammerDBProcessor):
    """Processor for HammerDB TPROC-C results against PostgreSQL."""

    def get_test_name(self) -> str:
        return "hammerdb_postgres"
