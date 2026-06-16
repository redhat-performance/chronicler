"""
CoreMark Pro processor: timestamp validation (valid, missing, invalid, empty).
"""

import json
import pytest
from pathlib import Path

from chronicler.processors.coremark_pro_processor import CoreMarkProProcessor

pytestmark = pytest.mark.unit
from conftest import run_processor_parse

FILE_KEY = "results_csv"
FILENAME = "results_coremark_pro.csv"


def _write_csv(result_dir: Path, content: str) -> Path:
    path = result_dir / FILENAME
    path.write_text(content.strip())
    return path


def test_coremark_pro_valid_timestamps(result_dir):
    """Valid CSV with Start_Date/End_Date parses successfully."""
    csv = """Test,Multi_iterations,Single_iterations,Scaling,Start_Date,End_Date
Score,100.0,50.0,2.0,2026-02-13T20:18:29Z,2026-02-13T20:19:14Z"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        CoreMarkProProcessor,
        result_dir,
        {FILE_KEY: str(path)},
        expect_error=False,
    )


def test_coremark_pro_no_timestamp_columns(result_dir):
    """Legacy colon-delimited format without timestamps raises ProcessorError."""
    csv = """Test:Multi_iterations:Single_iterations:Scaling
Score:100:50:2.0"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        CoreMarkProProcessor,
        result_dir,
        {FILE_KEY: str(path)},
        expect_error=True,
    )


def test_coremark_pro_invalid_timestamp_in_row(result_dir):
    """Malformed Start_Date in a row raises ProcessorError."""
    csv = """Test,Multi_iterations,Single_iterations,Scaling,Start_Date,End_Date
Score,100.0,50.0,2.0,not-a-date,2026-02-13T20:19:14Z"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        CoreMarkProProcessor,
        result_dir,
        {FILE_KEY: str(path)},
        expect_error=True,
    )


def test_coremark_pro_empty_timestamp_in_row(result_dir):
    """Empty Start_Date in a row raises ProcessorError."""
    csv = """Test,Multi_iterations,Single_iterations,Scaling,Start_Date,End_Date
Score,100.0,50.0,2.0,,2026-02-13T20:19:14Z"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        CoreMarkProProcessor,
        result_dir,
        {FILE_KEY: str(path)},
        expect_error=True,
    )


def test_coremark_pro_extracts_benchmark_version_from_csv_comments(result_dir):
    """Benchmark version in CSV comments is extracted to test.version."""
    csv = """# Test general meta start
# Test: coremark_pro
# Results version: v1.1.2743
# Host: test-host
# Test general meta end
Test,Multi_iterations,Single_iterations,Scaling,Start_Date,End_Date
Score,100.0,50.0,2.0,2026-02-13T20:18:29Z,2026-02-13T20:19:14Z"""

    path = _write_csv(result_dir, csv)

    # Create test_info with wrapper version
    test_info_data = {
        "coremark_pro": {
            "test_name": "coremark_pro",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = CoreMarkProProcessor(str(result_dir))
    extracted_result = {
        "files": {FILE_KEY: str(path)},
        "extracted_path": str(result_dir)
    }

    # Parse runs to extract benchmark version
    processor.parse_runs(extracted_result)

    # Now build_test_info should use the extracted benchmark version
    test_info = processor.build_test_info()

    # test.version should be benchmark version
    assert test_info.version == "v1.1.2743"
    # test.wrapper_version should be wrapper version
    assert test_info.wrapper_version == "v2.0"


def test_coremark_pro_fallback_to_wrapper_version_when_benchmark_version_missing(result_dir):
    """When benchmark version is missing from CSV, test.version falls back to wrapper version."""
    csv = """# Test general meta start
# Test: coremark_pro
# Host: test-host
# Test general meta end
Test,Multi_iterations,Single_iterations,Scaling,Start_Date,End_Date
Score,100.0,50.0,2.0,2026-02-13T20:18:29Z,2026-02-13T20:19:14Z"""

    path = _write_csv(result_dir, csv)

    # Create test_info with wrapper version
    test_info_data = {
        "coremark_pro": {
            "test_name": "coremark_pro",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = CoreMarkProProcessor(str(result_dir))
    extracted_result = {
        "files": {FILE_KEY: str(path)},
        "extracted_path": str(result_dir)
    }

    # Parse runs (no benchmark version in CSV)
    processor.parse_runs(extracted_result)

    # build_test_info should fall back to wrapper version
    test_info = processor.build_test_info()

    # Both should fall back to wrapper version
    assert test_info.version == "v2.0"
    assert test_info.wrapper_version == "v2.0"


def test_coremark_pro_resets_benchmark_version_between_parses(result_dir):
    """Benchmark version is reset between parse calls to prevent stale state."""
    csv_with_version = """# Results version: v1.1.2743
Test,Multi_iterations,Single_iterations,Scaling,Start_Date,End_Date
Score,100.0,50.0,2.0,2026-02-13T20:18:29Z,2026-02-13T20:19:14Z"""

    csv_without_version = """Test,Multi_iterations,Single_iterations,Scaling,Start_Date,End_Date
Score,100.0,50.0,2.0,2026-02-13T20:18:29Z,2026-02-13T20:19:14Z"""

    # Create test_info with wrapper version
    test_info_data = {
        "coremark_pro": {
            "test_name": "coremark_pro",
            "repo_file": "v2.0.tar.gz"
        }
    }
    test_info_path = result_dir / "test_info"
    with open(test_info_path, "w") as f:
        json.dump(test_info_data, f)

    processor = CoreMarkProProcessor(str(result_dir))

    # First parse: has benchmark version
    path1 = result_dir / "results1.csv"
    path1.write_text(csv_with_version.strip())
    extracted1 = {
        "files": {FILE_KEY: str(path1)},
        "extracted_path": str(result_dir)
    }
    processor.parse_runs(extracted1)
    test_info1 = processor.build_test_info()
    assert test_info1.version == "v1.1.2743"

    # Second parse: no benchmark version (should reset, not reuse v1.1.2743)
    path2 = result_dir / "results2.csv"
    path2.write_text(csv_without_version.strip())
    extracted2 = {
        "files": {FILE_KEY: str(path2)},
        "extracted_path": str(result_dir)
    }
    processor.parse_runs(extracted2)
    test_info2 = processor.build_test_info()
    assert test_info2.version == "v2.0"  # Should fall back, not reuse v1.1.2743
