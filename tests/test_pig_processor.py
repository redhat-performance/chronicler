"""
Pig processor: tuned profile glob, status derivation, empty results.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.pig_processor import PigProcessor
from chronicler.schema import Run

pytestmark = pytest.mark.unit

PIG_CSV = """\
#threads:sched_eff
1:4.00
32:1.03
64:1.00
"""


def _setup_pig_dir(tmp_path: Path, profile: str, csv: str = PIG_CSV, report: str = None):
    """Create results_pig_<profile>/ with a CSV and optional test_results_report."""
    results_dir = tmp_path / f"results_pig_{profile}"
    results_dir.mkdir()
    (results_dir / "results_pig.csv").write_text(csv)
    if report is not None:
        (results_dir / "test_results_report").write_text(report)
    return results_dir


def _parse(tmp_path: Path) -> dict:
    processor = PigProcessor(str(tmp_path))
    return processor.parse_runs({"extracted_path": str(tmp_path)})


def test_pig_multiple_profiles(tmp_path):
    """With two profile dirs, the glob picks the first alphabetically (balanced)."""
    _setup_pig_dir(tmp_path, "balanced", csv="#threads:sched_eff\n1:99.00\n", report="Ran 3 PASS")
    _setup_pig_dir(tmp_path, "virtual-guest", csv="#threads:sched_eff\n1:1.00\n", report="Ran 3 PASS")
    runs = _parse(tmp_path)
    assert len(runs) == 1
    run = list(runs.values())[0]
    assert run.metrics["max_sched_eff"] == 99.0, "Should pick balanced (first alphabetically)"


def test_pig_status_without_report(tmp_path):
    """No test_results_report means status is UNKNOWN."""
    _setup_pig_dir(tmp_path, "throughput-performance")
    runs = _parse(tmp_path)
    assert len(runs) == 1
    run = list(runs.values())[0]
    assert run.status == "UNKNOWN"


def test_pig_status_fail_with_report(tmp_path):
    """Report without 'Ran' keyword means status is not PASS."""
    _setup_pig_dir(tmp_path, "throughput-performance", report="FAIL: 1 of 3")
    runs = _parse(tmp_path)
    assert len(runs) == 1
    run = list(runs.values())[0]
    assert run.status == "UNKNOWN"


def test_pig_empty_glob(tmp_path):
    """No results_pig_* directories returns empty dict."""
    runs = _parse(tmp_path)
    assert runs == {}


# --- Status derivation tests (base_processor.build_results logic) ---
# When test_results_report is missing, base_processor derives status from
# individual run statuses. These tests exercise that code path by mocking
# parse_runs to return runs with known statuses, and patching out the
# archive extraction that build_results normally does.


def _make_runs(statuses):
    """Build a runs dict with Run objects having the given statuses."""
    return {
        f"run_{i}": Run(run_number=i, status=s, metrics={"sched_eff": 1.0})
        for i, s in enumerate(statuses)
    }


def _mock_build_results(tmp_path, run_statuses):
    """
    Exercise the status derivation in build_results() by mocking:
    - archive extraction (to avoid needing a real zip)
    - parse_runs (to return runs with controlled statuses)
    - test_results_report absent (so status starts as UNKNOWN)
    """
    runs = _make_runs(run_statuses)

    # Create a dummy results zip so the existence check passes
    dummy_zip = tmp_path / "results_pig.zip"
    dummy_zip.write_bytes(b"")

    processor = PigProcessor(str(tmp_path))

    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract, \
         patch.object(processor, "parse_runs", return_value=runs):
        mock_extract.return_value = {"files": {}, "extracted_path": str(tmp_path)}
        results = processor.build_results()

    return results


def test_pig_status_derived_all_pass(tmp_path):
    """No report + all runs PASS → overall status PASS."""
    results = _mock_build_results(tmp_path, ["PASS", "PASS", "PASS"])
    assert results.status == "PASS"


def test_pig_status_derived_mixed(tmp_path):
    """No report + mixed PASS/FAIL → overall status FAIL."""
    results = _mock_build_results(tmp_path, ["PASS", "FAIL", "PASS"])
    assert results.status == "FAIL"
