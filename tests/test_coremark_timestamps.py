"""
CoreMark processor: timestamp validation (valid, missing, invalid/empty in row).
"""

import json
import pytest
from pathlib import Path

from chronicler.processors.coremark_processor import CoreMarkProcessor

pytestmark = pytest.mark.unit
from conftest import run_processor_parse

FILE_KEY = "results_csv"
FILENAME = "results_coremark.csv"


def _write_csv(result_dir: Path, content: str) -> Path:
    path = result_dir / FILENAME
    path.write_text(content.strip())
    return path


def test_coremark_valid_timestamps(result_dir):
    """Valid comma-delimited CSV with iteration,threads,IterationsPerSec,Start_Date,End_Date parses successfully."""
    csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,119358.448340,2026-02-04T00:13:05Z,2026-02-04T00:13:39Z"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        CoreMarkProcessor,
        result_dir,
        {
            FILE_KEY: str(path),
            "run_summaries": [],
            "version": None,
            "tuned_setting": None,
        },
        expect_error=False,
    )


def test_coremark_no_timestamp_columns(result_dir):
    """Legacy format without Start_Date/End_Date raises ProcessorError."""
    csv = """iteration,threads,IterationsPerSec
1,4,119358.448340"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        CoreMarkProcessor,
        result_dir,
        {
            FILE_KEY: str(path),
            "run_summaries": [],
            "version": None,
            "tuned_setting": None,
        },
        expect_error=True,
    )


def test_coremark_empty_start_date_in_row(result_dir):
    """One row with missing Start_Date raises ProcessorError."""
    csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,119358.448340,,2026-02-04T00:13:39Z"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        CoreMarkProcessor,
        result_dir,
        {
            FILE_KEY: str(path),
            "run_summaries": [],
            "version": None,
            "tuned_setting": None,
        },
        expect_error=True,
    )


def test_coremark_extracts_benchmark_version_from_csv_comments(result_dir):
    """Benchmark version in CSV comments is extracted to test.version."""
    csv = """# Test general meta start
# Test: coremark
# Results version: v1.01
# Host: test-host
# Test general meta end
iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,119358.448340,2026-02-04T00:13:05Z,2026-02-04T00:13:39Z"""

    path = _write_csv(result_dir, csv)

    # Create test_info with wrapper version
    test_info_data = {
        "coremark": {
            "test_name": "coremark",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = CoreMarkProcessor(str(result_dir))
    extracted_result = {
        "files": {
            FILE_KEY: str(path),
            "run_summaries": [],
            "version": None,
            "tuned_setting": None,
        }
    }

    # Parse runs to extract benchmark version
    processor.parse_runs(extracted_result)

    # Now build_test_info should use the extracted benchmark version
    test_info = processor.build_test_info()

    # test.version should be benchmark version
    assert test_info.version == "v1.01"
    # test.wrapper_version should be wrapper version
    assert test_info.wrapper_version == "v2.0"


def test_coremark_fallback_to_wrapper_version_when_benchmark_version_missing(result_dir):
    """When benchmark version is missing from CSV, test.version falls back to wrapper version."""
    csv = """# Test general meta start
# Test: coremark
# Host: test-host
# Test general meta end
iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,119358.448340,2026-02-04T00:13:05Z,2026-02-04T00:13:39Z"""

    path = _write_csv(result_dir, csv)

    # Create test_info with wrapper version
    test_info_data = {
        "coremark": {
            "test_name": "coremark",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = CoreMarkProcessor(str(result_dir))
    extracted_result = {
        "files": {
            FILE_KEY: str(path),
            "run_summaries": [],
            "version": None,
            "tuned_setting": None,
        }
    }

    # Parse runs (no benchmark version in CSV)
    processor.parse_runs(extracted_result)

    # build_test_info should fall back to wrapper version
    test_info = processor.build_test_info()

    # Both should fall back to wrapper version
    assert test_info.version == "v2.0"
    assert test_info.wrapper_version == "v2.0"


def test_coremark_resets_benchmark_version_between_parses(result_dir):
    """Benchmark version is reset between parse calls to prevent stale state."""
    csv_with_version = """# Results version: v1.01
iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,119358.448340,2026-02-04T00:13:05Z,2026-02-04T00:13:39Z"""

    csv_without_version = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,119358.448340,2026-02-04T00:13:05Z,2026-02-04T00:13:39Z"""

    # Create test_info with wrapper version
    test_info_data = {
        "coremark": {
            "test_name": "coremark",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = CoreMarkProcessor(str(result_dir))

    # First parse: has benchmark version
    path1 = result_dir / "results1.csv"
    path1.write_text(csv_with_version.strip())
    extracted1 = {
        "files": {
            FILE_KEY: str(path1),
            "run_summaries": [],
            "version": None,
            "tuned_setting": None,
        }
    }
    processor.parse_runs(extracted1)
    test_info1 = processor.build_test_info()
    assert test_info1.version == "v1.01"

    # Second parse: no benchmark version (should reset, not reuse v1.01)
    path2 = result_dir / "results2.csv"
    path2.write_text(csv_without_version.strip())
    extracted2 = {
        "files": {
            FILE_KEY: str(path2),
            "run_summaries": [],
            "version": None,
            "tuned_setting": None,
        }
    }
    processor.parse_runs(extracted2)
    test_info2 = processor.build_test_info()
    assert test_info2.version == "v2.0"  # Should fall back, not reuse v1.01
