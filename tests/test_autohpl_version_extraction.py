"""
Auto HPL processor: benchmark version extraction.

Tests that Auto HPL extracts benchmark version (e.g., "1.0") from CSV comments
and stores it in test.version instead of wrapper version.

Follows pattern from RPOPC-1318 (fio) and RPOPC-1319 (coremark).
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.autohpl_processor import AutoHPLProcessor

pytestmark = pytest.mark.unit


@pytest.fixture
def result_dir_with_autohpl_version(tmp_path):
    """Create Auto HPL result directory with version in CSV comments and wrapper version in test_info."""
    # Create CSV with benchmark version in comments
    csv_content = """# Test general meta start
# Test: auto_hpl
# Results version: 1.0
# Host: test-host
# Test general meta end
T/V,N,NB,P,Q,Time,Gflops,Start_Date,End_Date
WR12R2R4,77568,256,1,1,552.77,5.6289e+02,2026-05-02T20:12:49Z,2026-05-02T20:24:43Z
"""

    csv_path = tmp_path / "results_auto_hpl.csv"
    csv_path.write_text(csv_content.strip())

    # Create test_info with wrapper version
    test_info = {
        "auto_hpl": {
            "test_name": "auto_hpl",
            "repo_file": "v2.5.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    # Create version file (wrapper version)
    version_path = tmp_path / "version"
    version_path.write_text("v2.5")

    # Create test_results_report (for PASS status)
    report_path = tmp_path / "test_results_report"
    report_path.write_text("Ran 1 test")

    return tmp_path


@pytest.fixture
def result_dir_without_autohpl_version(tmp_path):
    """Create Auto HPL result directory without version in CSV comments."""
    # Create CSV WITHOUT benchmark version in comments
    csv_content = """# Test general meta start
# Test: auto_hpl
# Host: test-host
# Test general meta end
T/V,N,NB,P,Q,Time,Gflops,Start_Date,End_Date
WR12R2R4,77568,256,1,1,552.77,5.6289e+02,2026-05-02T20:12:49Z,2026-05-02T20:24:43Z
"""

    csv_path = tmp_path / "results_auto_hpl.csv"
    csv_path.write_text(csv_content.strip())

    # Create test_info with wrapper version
    test_info = {
        "auto_hpl": {
            "test_name": "auto_hpl",
            "repo_file": "v2.5.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    # Create version file (wrapper version)
    version_path = tmp_path / "version"
    version_path.write_text("v2.5")

    # Create test_results_report
    report_path = tmp_path / "test_results_report"
    report_path.write_text("Ran 1 test")

    return tmp_path


def test_autohpl_version_in_test_version_field(result_dir_with_autohpl_version):
    """Test that Auto HPL benchmark version is stored in test.version."""
    processor = AutoHPLProcessor(str(result_dir_with_autohpl_version))
    extracted_result = {
        "extracted_path": str(result_dir_with_autohpl_version)
    }

    # Mock archive extraction
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs to extract benchmark version (this should populate _autohpl_version)
        processor.parse_runs(extracted_result)

        # Now build_test_info should use the extracted benchmark version
        test_info = processor.build_test_info()

    # Verify test.version contains benchmark version, not wrapper version
    assert test_info.version == "1.0", \
        f"Expected test.version='1.0', got '{test_info.version}'"

    # Verify wrapper_version contains wrapper version
    assert test_info.wrapper_version == "v2.5", \
        f"Expected wrapper_version='v2.5', got '{test_info.wrapper_version}'"


def test_autohpl_version_fallback_to_wrapper_version(result_dir_without_autohpl_version):
    """Test that wrapper version is used when Auto HPL version is missing."""
    processor = AutoHPLProcessor(str(result_dir_without_autohpl_version))
    extracted_result = {
        "extracted_path": str(result_dir_without_autohpl_version)
    }

    # Mock archive extraction
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs (no benchmark version to extract)
        processor.parse_runs(extracted_result)

        # build_test_info should fall back to wrapper version
        test_info = processor.build_test_info()

    # When benchmark version is missing, should fall back to wrapper version
    assert test_info.version == "v2.5", \
        f"Expected test.version='v2.5' (fallback), got '{test_info.version}'"

    assert test_info.wrapper_version == "v2.5", \
        f"Expected wrapper_version='v2.5', got '{test_info.wrapper_version}'"


def test_autohpl_version_no_wrapper_version(tmp_path):
    """Test Auto HPL version extraction when no wrapper version exists."""
    # Create CSV with benchmark version but NO test_info or version file
    csv_content = """# Test general meta start
# Test: auto_hpl
# Results version: 1.0
# Host: test-host
# Test general meta end
T/V,N,NB,P,Q,Time,Gflops,Start_Date,End_Date
WR12R2R4,77568,256,1,1,552.77,5.6289e+02,2026-05-02T20:12:49Z,2026-05-02T20:24:43Z
"""

    csv_path = tmp_path / "results_auto_hpl.csv"
    csv_path.write_text(csv_content.strip())

    # Create test_results_report
    report_path = tmp_path / "test_results_report"
    report_path.write_text("Ran 1 test")

    processor = AutoHPLProcessor(str(tmp_path))
    extracted_result = {
        "extracted_path": str(tmp_path)
    }

    # Mock archive extraction
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs to extract benchmark version
        processor.parse_runs(extracted_result)

        # build_test_info should use extracted benchmark version
        test_info = processor.build_test_info()

    # Should still extract benchmark version
    assert test_info.version == "1.0", \
        f"Expected test.version='1.0', got '{test_info.version}'"

    # Wrapper version should be "unknown" when test_info/version file is missing
    assert test_info.wrapper_version == "unknown", \
        f"Expected wrapper_version='unknown', got '{test_info.wrapper_version}'"


def test_autohpl_version_resets_between_parses(tmp_path):
    """Processor reuse: version state should not leak between parse_runs() calls."""
    # First CSV: WITH benchmark version
    csv_with_version = """# Test general meta start
# Results version: 1.0
# Test general meta end
T/V,N,NB,P,Q,Time,Gflops,Start_Date,End_Date
WR12R2R4,77568,256,1,1,552.77,5.6289e+02,2026-05-02T20:12:49Z,2026-05-02T20:24:43Z
"""

    csv_path1 = tmp_path / "results1.csv"
    csv_path1.write_text(csv_with_version.strip())

    # Create test_info with wrapper version for fallback
    test_info = {
        "auto_hpl": {
            "test_name": "auto_hpl",
            "repo_file": "v2.5.tar.gz"
        }
    }
    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    processor = AutoHPLProcessor(str(tmp_path))

    # First parse
    extracted_result1 = {
        "files": {"results_csv": str(csv_path1)}
    }
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result1
        processor.parse_runs(extracted_result1)
        test_info1 = processor.build_test_info()

    assert test_info1.version == "1.0", "First parse should extract benchmark version"

    # Second CSV: WITHOUT benchmark version (reusing same processor instance)
    csv_without_version = """# Test general meta start
# Test: auto_hpl
# Test general meta end
T/V,N,NB,P,Q,Time,Gflops,Start_Date,End_Date
WR12R2R4,77568,256,1,1,552.77,5.6289e+02,2026-05-02T20:12:49Z,2026-05-02T20:24:43Z
"""

    csv_path2 = tmp_path / "results2.csv"
    csv_path2.write_text(csv_without_version.strip())

    extracted_result2 = {
        "files": {"results_csv": str(csv_path2)}
    }

    # Second parse with same processor instance
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result2
        processor.parse_runs(extracted_result2)
        test_info2 = processor.build_test_info()

    # Should fall back to wrapper version, NOT retain stale "1.0"
    assert test_info2.version == "v2.5", \
        f"Second parse should fall back to wrapper version, not retain stale '1.0'. Got '{test_info2.version}'"
    assert test_info2.wrapper_version == "v2.5"
