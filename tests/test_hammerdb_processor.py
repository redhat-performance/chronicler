"""
HammerDB processor: connection/TPM parsing, nested CSV lookup, empty-row handling.
"""

import pytest
from pathlib import Path

from chronicler.processors.hammerdb_processor import (
    HammerDBMariaDBProcessor,
    HammerDBMSSQLProcessor,
    HammerDBPostgresProcessor,
)
from chronicler.processors.base_processor import ProcessorError

pytestmark = pytest.mark.unit

VALID_CSV = """\
# Test general meta start
# Test: hammerdb_mariadb
# Results version: None
# Test general meta end
connection,TPM,Start_Date,End_Date
10,15000,2026-08-28T05:15:34Z,2026-08-28T05:15:46Z
20,28000,2026-08-28T05:15:58Z,2026-08-28T05:16:10Z
40,31000,2026-08-28T05:16:22Z,2026-08-28T05:16:34Z
"""

# Real shape seen in production: HammerDB failed to install (hammerdbcli missing),
# so every row has timestamps but no connection/TPM data.
EMPTY_VALUES_CSV = """\
# Test general meta start
# Test: hammerdb_mariadb
# Test general meta end
connection,TPM,Start_Date,End_Date
,,2026-08-28T05:15:34Z,2026-08-28T05:15:36Z
,,2026-08-28T05:15:46Z,2026-08-28T05:15:48Z
"""

PARTIAL_CSV = """\
connection,TPM,Start_Date,End_Date
10,15000,2026-08-28T05:15:34Z,2026-08-28T05:15:46Z
,,2026-08-28T05:15:58Z,2026-08-28T05:16:10Z
"""


def _write_nested_csv(result_dir: Path, test_name: str, db_dir: str, csv: str) -> Path:
    """
    Mirror the real wrapper layout:
    {test_name}_<timestamp>/{db_dir}/results_{test_name}.csv
    """
    nested = result_dir / f"{test_name}_2026.08.28-05.16.10" / db_dir
    nested.mkdir(parents=True)
    path = nested / f"results_{test_name}.csv"
    path.write_text(csv)
    return path


def test_hammerdb_mariadb_valid_rows(result_dir):
    """Valid CSV rows produce one run with peak TPM and per-connection metrics."""
    _write_nested_csv(result_dir, "hammerdb_mariadb", "mariadb", VALID_CSV)
    processor = HammerDBMariaDBProcessor(str(result_dir))
    runs = processor.parse_runs({"extracted_path": str(result_dir), "files": {}})

    assert len(runs) == 1
    run = list(runs.values())[0]
    assert run.status == "PASS"
    assert run.metrics["peak_connection_config"] == 40
    assert run.metrics["peak_tpm"] == 31000
    assert run.metrics["throughput_connections_10_tpm"] == 15000
    assert len(run.timeseries) == 3


def test_hammerdb_empty_values_skipped(result_dir):
    """Rows with blank connection/TPM (e.g. hammerdbcli missing) are skipped, not errors."""
    _write_nested_csv(result_dir, "hammerdb_mariadb", "mariadb", EMPTY_VALUES_CSV)
    processor = HammerDBMariaDBProcessor(str(result_dir))
    runs = processor.parse_runs({"extracted_path": str(result_dir), "files": {}})

    assert runs == {}


def test_hammerdb_partial_rows(result_dir):
    """A mix of valid and blank rows keeps only the valid ones."""
    _write_nested_csv(result_dir, "hammerdb_mariadb", "mariadb", PARTIAL_CSV)
    processor = HammerDBMariaDBProcessor(str(result_dir))
    runs = processor.parse_runs({"extracted_path": str(result_dir), "files": {}})

    assert len(runs) == 1
    run = list(runs.values())[0]
    assert run.metrics["peak_tpm"] == 15000
    assert len(run.timeseries) == 1


def test_hammerdb_csv_not_found(result_dir):
    """No matching CSV anywhere under extracted_path returns no runs."""
    processor = HammerDBMariaDBProcessor(str(result_dir))
    runs = processor.parse_runs({"extracted_path": str(result_dir), "files": {}})
    assert runs == {}


def test_hammerdb_uses_files_results_csv_when_present(result_dir):
    """If archive_handler ever does resolve files['results_csv'], prefer it directly."""
    path = _write_nested_csv(result_dir, "hammerdb_mariadb", "mariadb", VALID_CSV)
    processor = HammerDBMariaDBProcessor(str(result_dir))
    runs = processor.parse_runs(
        {"extracted_path": str(result_dir), "files": {"results_csv": str(path)}}
    )
    assert len(runs) == 1


def test_hammerdb_invalid_timestamp_raises(result_dir):
    """A malformed Start_Date on an otherwise-valid row raises ProcessorError."""
    csv = """\
connection,TPM,Start_Date,End_Date
10,15000,not-a-date,2026-08-28T05:15:46Z
"""
    _write_nested_csv(result_dir, "hammerdb_mariadb", "mariadb", csv)
    processor = HammerDBMariaDBProcessor(str(result_dir))
    with pytest.raises(ProcessorError):
        processor.parse_runs({"extracted_path": str(result_dir), "files": {}})


@pytest.mark.parametrize(
    "processor_class,test_name,db_dir",
    [
        (HammerDBMariaDBProcessor, "hammerdb_mariadb", "mariadb"),
        (HammerDBMSSQLProcessor, "hammerdb_mssql", "mssql"),
        (HammerDBPostgresProcessor, "hammerdb_postgres", "postgres"),
    ],
)
def test_hammerdb_variant_test_names(result_dir, processor_class, test_name, db_dir):
    """Each DB variant reports its own registry test name and finds its own CSV."""
    processor = processor_class(str(result_dir))
    assert processor.get_test_name() == test_name

    _write_nested_csv(result_dir, test_name, db_dir, VALID_CSV.replace("hammerdb_mariadb", test_name))
    runs = processor.parse_runs({"extracted_path": str(result_dir), "files": {}})
    assert len(runs) == 1
