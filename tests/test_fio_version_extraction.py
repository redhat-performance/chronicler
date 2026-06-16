"""
FIO processor: benchmark version extraction.

Tests that FIO extracts benchmark version (e.g., "fio-3.36") from fio-results.json
and stores it in test.version instead of wrapper version.

Validates RPOPC-1318.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.fio_processor import FioProcessor

pytestmark = pytest.mark.unit


@pytest.fixture
def result_dir_with_fio_version(tmp_path):
    """Create FIO result directory with version in fio-results.json and wrapper version in test_info."""
    # Create fio-results.json with FIO version
    fio_data = {
        "fio version": "fio-3.36",
        "timestamp": 1776775499,
        "jobs": [
            {
                "jobname": "test-job",
                "groupid": 0,
                "elapsed": 120,
                "read": {
                    "io_bytes": 1000000,
                    "bw": 8333,
                    "iops": 2083,
                    "runtime": 120000,
                    "total_ios": 250000,
                    "lat_ns": {"mean": 480000, "min": 100000, "max": 1000000},
                    "clat_ns": {"mean": 470000, "min": 90000, "max": 990000},
                    "slat_ns": {"mean": 10000, "min": 1000, "max": 50000}
                }
            }
        ]
    }

    fio_json_path = tmp_path / "fio-results.json"
    with open(fio_json_path, "w") as f:
        json.dump(fio_data, f)

    # Create test_info with wrapper version
    test_info = {
        "fio": {
            "test_name": "fio",
            "repo_file": "v2.1.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    return tmp_path


@pytest.fixture
def result_dir_without_fio_version(tmp_path):
    """Create FIO result directory without version in fio-results.json."""
    # Create fio-results.json WITHOUT FIO version
    fio_data = {
        "timestamp": 1776775499,
        "jobs": [
            {
                "jobname": "test-job",
                "groupid": 0,
                "elapsed": 120,
                "read": {
                    "io_bytes": 1000000,
                    "bw": 8333,
                    "iops": 2083,
                    "runtime": 120000,
                    "total_ios": 250000,
                    "lat_ns": {"mean": 480000, "min": 100000, "max": 1000000},
                    "clat_ns": {"mean": 470000, "min": 90000, "max": 990000},
                    "slat_ns": {"mean": 10000, "min": 1000, "max": 50000}
                }
            }
        ]
    }

    fio_json_path = tmp_path / "fio-results.json"
    with open(fio_json_path, "w") as f:
        json.dump(fio_data, f)

    # Create test_info with wrapper version
    test_info = {
        "fio": {
            "test_name": "fio",
            "repo_file": "v2.1.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    return tmp_path


def test_fio_version_in_test_version_field(result_dir_with_fio_version):
    """Test that FIO benchmark version is stored in test.version."""
    processor = FioProcessor(str(result_dir_with_fio_version))
    extracted_result = {"files": {"fio_results_json": str(result_dir_with_fio_version / "fio-results.json")},
                       "extracted_path": str(result_dir_with_fio_version)}

    # Mock archive extraction and call build_test_info
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs to extract FIO version (this should populate _fio_version)
        processor.parse_runs(extracted_result)

        # Now build_test_info should use the extracted FIO version
        test_info = processor.build_test_info()

    # Verify test.version contains benchmark version, not wrapper version
    assert test_info.version == "fio-3.36", \
        f"Expected test.version='fio-3.36', got '{test_info.version}'"

    # Verify wrapper_version contains wrapper version
    assert test_info.wrapper_version == "v2.1", \
        f"Expected wrapper_version='v2.1', got '{test_info.wrapper_version}'"


def test_fio_version_fallback_to_wrapper_version(result_dir_without_fio_version):
    """Test that wrapper version is used when FIO version is missing."""
    processor = FioProcessor(str(result_dir_without_fio_version))
    extracted_result = {"files": {"fio_results_json": str(result_dir_without_fio_version / "fio-results.json")},
                       "extracted_path": str(result_dir_without_fio_version)}

    # Mock archive extraction and call build_test_info
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs (no FIO version to extract)
        processor.parse_runs(extracted_result)

        # build_test_info should fall back to wrapper version
        test_info = processor.build_test_info()

    # When FIO version is missing, should fall back to wrapper version
    assert test_info.version == "v2.1", \
        f"Expected test.version='v2.1' (fallback), got '{test_info.version}'"

    assert test_info.wrapper_version == "v2.1", \
        f"Expected wrapper_version='v2.1', got '{test_info.wrapper_version}'"


def test_fio_version_no_wrapper_version(tmp_path):
    """Test FIO version extraction when no wrapper version exists."""
    # Create fio-results.json with FIO version but NO test_info file
    fio_data = {
        "fio version": "fio-3.36",
        "timestamp": 1776775499,
        "jobs": [
            {
                "jobname": "test-job",
                "groupid": 0,
                "elapsed": 120,
                "read": {
                    "io_bytes": 1000000,
                    "bw": 8333,
                    "iops": 2083,
                    "runtime": 120000,
                    "total_ios": 250000,
                    "lat_ns": {"mean": 480000, "min": 100000, "max": 1000000},
                    "clat_ns": {"mean": 470000, "min": 90000, "max": 990000},
                    "slat_ns": {"mean": 10000, "min": 1000, "max": 50000}
                }
            }
        ]
    }

    fio_json_path = tmp_path / "fio-results.json"
    with open(fio_json_path, "w") as f:
        json.dump(fio_data, f)

    processor = FioProcessor(str(tmp_path))
    extracted_result = {"files": {"fio_results_json": str(fio_json_path)},
                       "extracted_path": str(tmp_path)}

    # Mock archive extraction
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs to extract FIO version
        processor.parse_runs(extracted_result)

        # build_test_info should use extracted FIO version
        test_info = processor.build_test_info()

    # Should still extract FIO version
    assert test_info.version == "fio-3.36", \
        f"Expected test.version='fio-3.36', got '{test_info.version}'"

    # Wrapper version should be "unknown" when test_info is missing
    assert test_info.wrapper_version == "unknown", \
        f"Expected wrapper_version='unknown', got '{test_info.wrapper_version}'"
