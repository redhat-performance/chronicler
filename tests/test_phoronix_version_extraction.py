"""
Phoronix processor: benchmark version extraction.

Tests that Phoronix extracts benchmark version (e.g., "v10.8.1") from CSV comments
and stores it in test.version instead of wrapper version.

Follows pattern from RPOPC-1318 (fio) and RPOPC-1319 (coremark).
"""

import json
import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.phoronix_processor import PhoronixProcessor

pytestmark = pytest.mark.unit


@pytest.fixture
def result_dir_with_phoronix_version(tmp_path):
    """Create Phoronix result directory with version in CSV comments and wrapper version in test_info."""
    # Create subdirectory structure that phoronix processor expects
    results_dir = tmp_path / "results_phoronix_test"
    results_dir.mkdir()

    # Create CSV with benchmark version in comments
    csv_content = """# Test general meta start
# Test: phoronix
# Results version: v10.8.1
# Host: test-host
# Sys environ: test
# Test general meta end
# Subtest: stress-ng
Test,Average,Deviation,Start_Date,End_Date
Hash,1754573.98,0.97,2026-05-02T22:01:59Z,2026-05-03T00:14:35Z
MMAP,648.13,0.79,2026-05-02T22:01:59Z,2026-05-03T00:14:35Z
"""

    csv_path = results_dir / "results.csv"
    csv_path.write_text(csv_content.strip())

    # Create test_info with wrapper version
    test_info = {
        "phoronix": {
            "test_name": "phoronix",
            "repo_file": "v3.2.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    # Create version file (wrapper version)
    version_path = tmp_path / "version"
    version_path.write_text("v3.2")

    return tmp_path


@pytest.fixture
def result_dir_without_phoronix_version(tmp_path):
    """Create Phoronix result directory without version in CSV comments."""
    # Create subdirectory structure
    results_dir = tmp_path / "results_phoronix_test"
    results_dir.mkdir()

    # Create CSV WITHOUT benchmark version in comments
    csv_content = """# Test general meta start
# Test: phoronix
# Host: test-host
# Test general meta end
# Subtest: stress-ng
Test,Average,Deviation,Start_Date,End_Date
Hash,1754573.98,0.97,2026-05-02T22:01:59Z,2026-05-03T00:14:35Z
MMAP,648.13,0.79,2026-05-02T22:01:59Z,2026-05-03T00:14:35Z
"""

    csv_path = results_dir / "results.csv"
    csv_path.write_text(csv_content.strip())

    # Create test_info with wrapper version
    test_info = {
        "phoronix": {
            "test_name": "phoronix",
            "repo_file": "v3.2.tar.gz"
        }
    }

    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    # Create version file (wrapper version)
    version_path = tmp_path / "version"
    version_path.write_text("v3.2")

    return tmp_path


def test_phoronix_version_in_test_version_field(result_dir_with_phoronix_version):
    """Test that Phoronix benchmark version is stored in test.version."""
    processor = PhoronixProcessor(str(result_dir_with_phoronix_version))
    extracted_result = {
        "extracted_path": str(result_dir_with_phoronix_version)
    }

    # Parse runs to extract benchmark version (this should populate _benchmark_version)
    processor.parse_runs(extracted_result)

    # Now build_test_info should use the extracted benchmark version
    test_info = processor.build_test_info()

    # Verify test.version contains benchmark version, not wrapper version
    assert test_info.version == "v10.8.1", \
        f"Expected test.version='v10.8.1', got '{test_info.version}'"

    # Verify wrapper_version contains wrapper version
    assert test_info.wrapper_version == "v3.2", \
        f"Expected wrapper_version='v3.2', got '{test_info.wrapper_version}'"


def test_phoronix_version_fallback_to_wrapper_version(result_dir_without_phoronix_version):
    """Test that wrapper version is used when Phoronix version is missing."""
    processor = PhoronixProcessor(str(result_dir_without_phoronix_version))
    extracted_result = {
        "extracted_path": str(result_dir_without_phoronix_version)
    }

    # Parse runs (no benchmark version to extract)
    processor.parse_runs(extracted_result)

    # build_test_info should fall back to wrapper version
    test_info = processor.build_test_info()

    # When benchmark version is missing, should fall back to wrapper version
    assert test_info.version == "v3.2", \
        f"Expected test.version='v3.2' (fallback), got '{test_info.version}'"

    assert test_info.wrapper_version == "v3.2", \
        f"Expected wrapper_version='v3.2', got '{test_info.wrapper_version}'"


def test_phoronix_version_no_wrapper_version(tmp_path):
    """Test Phoronix version extraction when no wrapper version exists."""
    # Create subdirectory structure
    results_dir = tmp_path / "results_phoronix_test"
    results_dir.mkdir()

    # Create CSV with benchmark version but NO test_info or version file
    csv_content = """# Test general meta start
# Test: phoronix
# Results version: v10.8.1
# Host: test-host
# Test general meta end
# Subtest: stress-ng
Test,Average,Deviation,Start_Date,End_Date
Hash,1754573.98,0.97,2026-05-02T22:01:59Z,2026-05-03T00:14:35Z
MMAP,648.13,0.79,2026-05-02T22:01:59Z,2026-05-03T00:14:35Z
"""

    csv_path = results_dir / "results.csv"
    csv_path.write_text(csv_content.strip())

    processor = PhoronixProcessor(str(tmp_path))
    extracted_result = {
        "extracted_path": str(tmp_path)
    }

    # Parse runs to extract benchmark version
    processor.parse_runs(extracted_result)

    # build_test_info should use extracted benchmark version
    test_info = processor.build_test_info()

    # Should still extract benchmark version
    assert test_info.version == "v10.8.1", \
        f"Expected test.version='v10.8.1', got '{test_info.version}'"

    # Wrapper version should be "unknown" when test_info/version file is missing
    assert test_info.wrapper_version == "unknown", \
        f"Expected wrapper_version='unknown', got '{test_info.wrapper_version}'"


def test_phoronix_version_resets_between_parses(tmp_path):
    """Processor reuse: version state should not leak between parse_runs() calls."""
    # First CSV: WITH benchmark version
    results_dir1 = tmp_path / "results1"
    results_dir1.mkdir()

    csv_with_version = """# Test general meta start
# Results version: v10.8.1
# Test general meta end
# Subtest: stress-ng
Test,Average,Deviation,Start_Date,End_Date
Hash,1754573.98,0.97,2026-05-02T22:01:59Z,2026-05-03T00:14:35Z
"""

    csv_path1 = results_dir1 / "results.csv"
    csv_path1.write_text(csv_with_version.strip())

    # Create test_info with wrapper version for fallback
    test_info = {
        "phoronix": {
            "test_name": "phoronix",
            "repo_file": "v3.2.tar.gz"
        }
    }
    test_info_path = tmp_path / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info, f)

    processor = PhoronixProcessor(str(tmp_path))

    # First parse
    extracted_result1 = {
        "extracted_path": str(results_dir1)
    }
    processor.parse_runs(extracted_result1)
    test_info1 = processor.build_test_info()

    assert test_info1.version == "v10.8.1", "First parse should extract benchmark version"

    # Second CSV: WITHOUT benchmark version (reusing same processor instance)
    results_dir2 = tmp_path / "results2"
    results_dir2.mkdir()

    csv_without_version = """# Test general meta start
# Test: phoronix
# Test general meta end
# Subtest: stress-ng
Test,Average,Deviation,Start_Date,End_Date
Hash,1754573.98,0.97,2026-05-02T22:01:59Z,2026-05-03T00:14:35Z
"""

    csv_path2 = results_dir2 / "results.csv"
    csv_path2.write_text(csv_without_version.strip())

    extracted_result2 = {
        "extracted_path": str(results_dir2)
    }

    # Second parse with same processor instance
    processor.parse_runs(extracted_result2)
    test_info2 = processor.build_test_info()

    # Should fall back to wrapper version, NOT retain stale "v10.8.1"
    assert test_info2.version == "v3.2", \
        f"Second parse should fall back to wrapper version, not retain stale 'v10.8.1'. Got '{test_info2.version}'"
    assert test_info2.wrapper_version == "v3.2"
