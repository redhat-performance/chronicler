"""
SPEC CPU 2017 processor: benchmark version extraction.

Tests that SPEC CPU 2017 extracts benchmark version from version file
and stores it in test.version instead of wrapper version.

Validates RPOPC-1319.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.speccpu2017_processor import SpecCPU2017Processor

pytestmark = pytest.mark.unit


@pytest.fixture
def result_dir_with_speccpu_version(tmp_path):
    """Create SPEC CPU 2017 result directory with version file and wrapper version in test_info."""
    # Create CSV with valid timestamps
    csv_content = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
500.perlbench_r,1,100.5,9.95,2026-02-26T03:21:32Z,2026-02-26T03:25:00Z
502.gcc_r,1,200.0,8.50,2026-02-26T03:25:05Z,2026-02-26T03:30:00Z"""

    csv_path = tmp_path / "CPU2017.003.intrate.refrate.results.csv"
    csv_path.write_text(csv_content)

    # Create version file with SPEC CPU version
    version_path = tmp_path / "version"
    version_path.write_text("1.1.9\n")

    # Create test_results_report for status
    report_path = tmp_path / "test_results_report"
    report_path.write_text("Ran SPEC CPU 2017")

    # Create test_info with wrapper version
    test_info = {
        "speccpu2017": {
            "test_name": "speccpu2017",
            "repo_file": "v2.6.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    return tmp_path


@pytest.fixture
def result_dir_without_speccpu_version(tmp_path):
    """Create SPEC CPU 2017 result directory without version file."""
    # Create CSV with valid timestamps
    csv_content = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
500.perlbench_r,1,100.5,9.95,2026-02-26T03:21:32Z,2026-02-26T03:25:00Z
502.gcc_r,1,200.0,8.50,2026-02-26T03:25:05Z,2026-02-26T03:30:00Z"""

    csv_path = tmp_path / "CPU2017.003.intrate.refrate.results.csv"
    csv_path.write_text(csv_content)

    # Create test_results_report for status
    report_path = tmp_path / "test_results_report"
    report_path.write_text("Ran SPEC CPU 2017")

    # Create test_info with wrapper version
    test_info = {
        "speccpu2017": {
            "test_name": "speccpu2017",
            "repo_file": "v2.6.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    return tmp_path


def test_speccpu2017_version_in_test_version_field(result_dir_with_speccpu_version):
    """Test that SPEC CPU 2017 benchmark version is stored in test.version."""
    processor = SpecCPU2017Processor(str(result_dir_with_speccpu_version))
    extracted_result = {
        "files": {},
        "extracted_path": str(result_dir_with_speccpu_version)
    }

    # Mock archive extraction and call build_test_info
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs to extract SPEC CPU version (this should populate _speccpu_version)
        processor.parse_runs(extracted_result)

        # Now build_test_info should use the extracted SPEC CPU version
        test_info = processor.build_test_info()

    # Verify test.version contains benchmark version, not wrapper version
    assert test_info.version == "1.1.9", \
        f"Expected test.version='1.1.9', got '{test_info.version}'"

    # Verify wrapper_version contains wrapper version
    assert test_info.wrapper_version == "v2.6", \
        f"Expected wrapper_version='v2.6', got '{test_info.wrapper_version}'"


def test_speccpu2017_version_fallback_to_wrapper_version(result_dir_without_speccpu_version):
    """Test that wrapper version is used when SPEC CPU version is missing."""
    processor = SpecCPU2017Processor(str(result_dir_without_speccpu_version))
    extracted_result = {
        "files": {},
        "extracted_path": str(result_dir_without_speccpu_version)
    }

    # Mock archive extraction and call build_test_info
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs (no SPEC CPU version to extract)
        processor.parse_runs(extracted_result)

        # build_test_info should fall back to wrapper version
        test_info = processor.build_test_info()

    # When SPEC CPU version is missing, should fall back to wrapper version
    assert test_info.version == "v2.6", \
        f"Expected test.version='v2.6' (fallback), got '{test_info.version}'"

    assert test_info.wrapper_version == "v2.6", \
        f"Expected wrapper_version='v2.6', got '{test_info.wrapper_version}'"


def test_speccpu2017_version_no_wrapper_version(tmp_path):
    """Test SPEC CPU version extraction when no wrapper version exists."""
    # Create CSV with valid timestamps
    csv_content = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
500.perlbench_r,1,100.5,9.95,2026-02-26T03:21:32Z,2026-02-26T03:25:00Z"""

    csv_path = tmp_path / "CPU2017.003.intrate.refrate.results.csv"
    csv_path.write_text(csv_content)

    # Create version file with SPEC CPU version but NO test_info file
    version_path = tmp_path / "version"
    version_path.write_text("1.1.9\n")

    # Create test_results_report for status
    report_path = tmp_path / "test_results_report"
    report_path.write_text("Ran SPEC CPU 2017")

    processor = SpecCPU2017Processor(str(tmp_path))
    extracted_result = {
        "files": {},
        "extracted_path": str(tmp_path)
    }

    # Mock archive extraction
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs to extract SPEC CPU version
        processor.parse_runs(extracted_result)

        # build_test_info should use extracted SPEC CPU version
        test_info = processor.build_test_info()

    # Should still extract SPEC CPU version
    assert test_info.version == "1.1.9", \
        f"Expected test.version='1.1.9', got '{test_info.version}'"

    # Wrapper version should be "unknown" when test_info is missing
    assert test_info.wrapper_version == "unknown", \
        f"Expected wrapper_version='unknown', got '{test_info.wrapper_version}'"


def test_speccpu2017_version_resets_between_parses(tmp_path):
    """Processor reuse: version state should not leak between parse_runs() calls."""
    # First parse: WITH benchmark version
    csv_content = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
500.perlbench_r,1,100.5,9.95,2026-02-26T03:21:32Z,2026-02-26T03:25:00Z"""

    csv_path1 = tmp_path / "CPU2017.003.intrate.refrate.results.csv"
    csv_path1.write_text(csv_content)

    version_path1 = tmp_path / "version"
    version_path1.write_text("1.1.9\n")

    report_path = tmp_path / "test_results_report"
    report_path.write_text("Ran SPEC CPU 2017")

    # Create test_info file with wrapper version for fallback
    test_info = {
        "speccpu2017": {
            "test_name": "speccpu2017",
            "repo_file": "v2.6.tar.gz"
        }
    }
    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    processor = SpecCPU2017Processor(str(tmp_path))
    extracted_result1 = {
        "files": {},
        "extracted_path": str(tmp_path)
    }

    # First parse
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result1
        processor.parse_runs(extracted_result1)
        test_info1 = processor.build_test_info()

    assert test_info1.version == "1.1.9", "First parse should extract benchmark version"

    # Second parse: WITHOUT benchmark version (remove version file, reusing same processor instance)
    version_path1.unlink()

    extracted_result2 = {
        "files": {},
        "extracted_path": str(tmp_path)
    }

    # Second parse with same processor instance
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result2
        processor.parse_runs(extracted_result2)
        test_info2 = processor.build_test_info()

    # Should fall back to wrapper version, NOT retain stale "1.1.9"
    assert test_info2.version == "v2.6", \
        f"Second parse should fall back to wrapper version, not retain stale '1.1.9'. Got '{test_info2.version}'"
    assert test_info2.wrapper_version == "v2.6"
