"""
Passmark processor: benchmark version extraction.

Tests that Passmark extracts benchmark version from Version block in YML
and stores it in test.version instead of wrapper version.

Validates RPOPC-1319.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.passmark_processor import PassmarkProcessor

pytestmark = pytest.mark.unit


@pytest.fixture
def result_dir_with_passmark_version(tmp_path):
    """Create Passmark result directory with version in YML and wrapper version in test_info."""
    # Create YML with Passmark version
    yml_content = """BaselineInfo:
  WebDBID: -1
  TimeStamp: 20260502203522
Version:
  Major: 11
  Minor: 0
  Build: 1002
Results:
  NumTestProcesses: 16
  CPU_INTEGER_MATH: 73090.879000000001
  SUMM_CPU: 24685.407296119931
  SUMM_ME: 2106.6563041301797
SystemInformation:
  OSName: Red Hat Enterprise Linux 9.8
  Processor: Intel Xeon Platinum 8488C
"""

    yml_path = tmp_path / "results_all_1.yml"
    yml_path.write_text(yml_content)

    # Create test_info with wrapper version
    test_info = {
        "passmark": {
            "test_name": "passmark",
            "repo_file": "v2.1.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    return tmp_path


@pytest.fixture
def result_dir_without_passmark_version(tmp_path):
    """Create Passmark result directory without version in YML."""
    # Create YML WITHOUT version block
    yml_content = """BaselineInfo:
  WebDBID: -1
  TimeStamp: 20260502203522
Results:
  NumTestProcesses: 16
  CPU_INTEGER_MATH: 73090.879000000001
  SUMM_CPU: 24685.407296119931
  SUMM_ME: 2106.6563041301797
SystemInformation:
  OSName: Red Hat Enterprise Linux 9.8
  Processor: Intel Xeon Platinum 8488C
"""

    yml_path = tmp_path / "results_all_1.yml"
    yml_path.write_text(yml_content)

    # Create test_info with wrapper version
    test_info = {
        "passmark": {
            "test_name": "passmark",
            "repo_file": "v2.1.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    return tmp_path


def test_passmark_version_in_test_version_field(result_dir_with_passmark_version):
    """Test that Passmark benchmark version is stored in test.version."""
    processor = PassmarkProcessor(str(result_dir_with_passmark_version))
    extracted_result = {"files": {}, "extracted_path": str(result_dir_with_passmark_version)}

    # Mock archive extraction and call build_test_info
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs to extract Passmark version (this should populate _passmark_version)
        processor.parse_runs(extracted_result)

        # Now build_test_info should use the extracted Passmark version
        test_info = processor.build_test_info()

    # Verify test.version contains benchmark version, not wrapper version
    assert test_info.version == "11.0.1002", \
        f"Expected test.version='11.0.1002', got '{test_info.version}'"

    # Verify wrapper_version contains wrapper version
    assert test_info.wrapper_version == "v2.1", \
        f"Expected wrapper_version='v2.1', got '{test_info.wrapper_version}'"


def test_passmark_version_fallback_to_wrapper_version(result_dir_without_passmark_version):
    """Test that wrapper version is used when Passmark version is missing."""
    processor = PassmarkProcessor(str(result_dir_without_passmark_version))
    extracted_result = {"files": {}, "extracted_path": str(result_dir_without_passmark_version)}

    # Mock archive extraction and call build_test_info
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs (no Passmark version to extract)
        processor.parse_runs(extracted_result)

        # build_test_info should fall back to wrapper version
        test_info = processor.build_test_info()

    # When Passmark version is missing, should fall back to wrapper version
    assert test_info.version == "v2.1", \
        f"Expected test.version='v2.1' (fallback), got '{test_info.version}'"

    assert test_info.wrapper_version == "v2.1", \
        f"Expected wrapper_version='v2.1', got '{test_info.wrapper_version}'"


def test_passmark_version_no_wrapper_version(tmp_path):
    """Test Passmark version extraction when no wrapper version exists."""
    # Create YML with Passmark version but NO test_info file
    yml_content = """BaselineInfo:
  WebDBID: -1
  TimeStamp: 20260502203522
Version:
  Major: 11
  Minor: 0
  Build: 1002
Results:
  NumTestProcesses: 16
  CPU_INTEGER_MATH: 73090.879000000001
  SUMM_CPU: 24685.407296119931
  SUMM_ME: 2106.6563041301797
SystemInformation:
  OSName: Red Hat Enterprise Linux 9.8
"""

    yml_path = tmp_path / "results_all_1.yml"
    yml_path.write_text(yml_content)

    processor = PassmarkProcessor(str(tmp_path))
    extracted_result = {"files": {}, "extracted_path": str(tmp_path)}

    # Mock archive extraction
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result

        # Parse runs to extract Passmark version
        processor.parse_runs(extracted_result)

        # build_test_info should use extracted Passmark version
        test_info = processor.build_test_info()

    # Should still extract Passmark version
    assert test_info.version == "11.0.1002", \
        f"Expected test.version='11.0.1002', got '{test_info.version}'"

    # Wrapper version should be "unknown" when test_info is missing
    assert test_info.wrapper_version == "unknown", \
        f"Expected wrapper_version='unknown', got '{test_info.wrapper_version}'"


def test_passmark_version_resets_between_parses(tmp_path):
    """Processor reuse: version state should not leak between parse_runs() calls."""
    # First parse: YML WITH benchmark version
    yml_content_with_version = """BaselineInfo:
  WebDBID: -1
  TimeStamp: 20260502203522
Version:
  Major: 11
  Minor: 0
  Build: 1002
Results:
  NumTestProcesses: 16
  CPU_INTEGER_MATH: 73090.879000000001
  SUMM_CPU: 24685.407296119931
  SUMM_ME: 2106.6563041301797
SystemInformation:
  OSName: Red Hat Enterprise Linux 9.8
"""

    yml_path1 = tmp_path / "results_all_1.yml"
    yml_path1.write_text(yml_content_with_version)

    # Create test_info file with wrapper version for fallback
    test_info = {
        "passmark": {
            "test_name": "passmark",
            "repo_file": "v2.1.tar.gz"
        }
    }
    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    processor = PassmarkProcessor(str(tmp_path))
    extracted_result1 = {"files": {}, "extracted_path": str(tmp_path)}

    # First parse
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result1
        processor.parse_runs(extracted_result1)
        test_info1 = processor.build_test_info()

    assert test_info1.version == "11.0.1002", "First parse should extract benchmark version"

    # Second parse: YML WITHOUT benchmark version (reusing same processor instance)
    # Create new directory for second parse
    tmp_path2 = tmp_path / "second_parse"
    tmp_path2.mkdir()

    yml_content_without_version = """BaselineInfo:
  WebDBID: -1
  TimeStamp: 20260502204522
Results:
  NumTestProcesses: 16
  CPU_INTEGER_MATH: 75000.0
  SUMM_CPU: 25000.0
  SUMM_ME: 2200.0
SystemInformation:
  OSName: Red Hat Enterprise Linux 9.8
"""

    yml_path2 = tmp_path2 / "results_all_1.yml"
    yml_path2.write_text(yml_content_without_version)

    # Copy test_info to second directory
    test_info_path2 = tmp_path2 / "test_info"
    with open(test_info_path2, "w") as f:
        json.dump(test_info, f)

    extracted_result2 = {"files": {}, "extracted_path": str(tmp_path2)}

    # Update processor result_path for second parse
    processor.result_path = str(tmp_path2)

    # Second parse with same processor instance
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result2
        processor.parse_runs(extracted_result2)
        test_info2 = processor.build_test_info()

    # Should fall back to wrapper version, NOT retain stale "11.0.1002"
    assert test_info2.version == "v2.1", \
        f"Second parse should fall back to wrapper version, not retain stale '11.0.1002'. Got '{test_info2.version}'"
    assert test_info2.wrapper_version == "v2.1"
