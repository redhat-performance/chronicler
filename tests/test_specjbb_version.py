"""
SpecJBB processor: benchmark version extraction tests.

Tests that benchmark version is extracted from CSV comments and used in test.version,
with proper state reset between parses.
"""

import json
import pytest
from pathlib import Path

from chronicler.processors.specjbb_processor import SpecJBBProcessor

pytestmark = pytest.mark.unit


def _write_csv(result_dir: Path, content: str) -> Path:
    """Write specjbb CSV file with given content."""
    path = result_dir / "results_specjbb.csv"
    path.write_text(content.strip())
    return path


def test_specjbb_extracts_benchmark_version_from_csv_comments(result_dir):
    """Benchmark version in CSV comments is extracted to test.version."""
    csv = """# Test general meta start
# Test: specjbb
# Results version: 1.0
# Host: test-host
# Test general meta end
Warehouses,Bops,Numb_JVMs,Start_Date,End_Date
4,323051,1,2026-05-03T01:28:55Z,2026-05-03T01:37:03Z
8,566936,1,2026-05-03T01:28:55Z,2026-05-03T01:37:03Z"""

    path = _write_csv(result_dir, csv)

    # Create test_info with wrapper version
    test_info_data = {
        "specjbb": {
            "test_name": "specjbb",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = SpecJBBProcessor(str(result_dir))
    extracted_result = {
        "files": {
            "results_specjbb_csv": str(path)
        }
    }

    # Parse runs to extract benchmark version
    processor.parse_runs(extracted_result)

    # Now build_test_info should use the extracted benchmark version
    test_info = processor.build_test_info()

    # test.version should be benchmark version
    assert test_info.version == "1.0"
    # test.wrapper_version should be wrapper version
    assert test_info.wrapper_version == "v2.0"


def test_specjbb_fallback_to_wrapper_version_when_benchmark_version_missing(result_dir):
    """When benchmark version is missing from CSV, test.version falls back to wrapper version."""
    csv = """# Test general meta start
# Test: specjbb
# Host: test-host
# Test general meta end
Warehouses,Bops,Numb_JVMs,Start_Date,End_Date
4,323051,1,2026-05-03T01:28:55Z,2026-05-03T01:37:03Z
8,566936,1,2026-05-03T01:28:55Z,2026-05-03T01:37:03Z"""

    path = _write_csv(result_dir, csv)

    # Create test_info with wrapper version
    test_info_data = {
        "specjbb": {
            "test_name": "specjbb",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = SpecJBBProcessor(str(result_dir))
    extracted_result = {
        "files": {
            "results_specjbb_csv": str(path)
        }
    }

    # Parse runs (no benchmark version in CSV)
    processor.parse_runs(extracted_result)

    # build_test_info should fall back to wrapper version
    test_info = processor.build_test_info()

    # Both should fall back to wrapper version
    assert test_info.version == "v2.0"
    assert test_info.wrapper_version == "v2.0"


def test_specjbb_resets_benchmark_version_between_parses(result_dir):
    """Benchmark version is reset between parse calls to prevent stale state."""
    csv_with_version = """# Results version: 1.0
Warehouses,Bops,Numb_JVMs,Start_Date,End_Date
4,323051,1,2026-05-03T01:28:55Z,2026-05-03T01:37:03Z
8,566936,1,2026-05-03T01:28:55Z,2026-05-03T01:37:03Z"""

    csv_without_version = """Warehouses,Bops,Numb_JVMs,Start_Date,End_Date
4,323051,1,2026-05-03T01:28:55Z,2026-05-03T01:37:03Z
8,566936,1,2026-05-03T01:28:55Z,2026-05-03T01:37:03Z"""

    # Create test_info with wrapper version
    test_info_data = {
        "specjbb": {
            "test_name": "specjbb",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = SpecJBBProcessor(str(result_dir))

    # First parse: has benchmark version
    path1 = result_dir / "results1.csv"
    path1.write_text(csv_with_version.strip())
    extracted1 = {
        "files": {
            "results_specjbb_csv": str(path1)
        }
    }
    processor.parse_runs(extracted1)
    test_info1 = processor.build_test_info()
    assert test_info1.version == "1.0"

    # Second parse: no benchmark version (should reset, not reuse 1.0)
    path2 = result_dir / "results2.csv"
    path2.write_text(csv_without_version.strip())
    extracted2 = {
        "files": {
            "results_specjbb_csv": str(path2)
        }
    }
    processor.parse_runs(extracted2)
    test_info2 = processor.build_test_info()
    assert test_info2.version == "v2.0"  # Should fall back, not reuse 1.0
