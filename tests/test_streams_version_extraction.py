"""
STREAMS processor: benchmark version extraction from CSV metadata comments.
"""

import pytest
from pathlib import Path

from chronicler.processors.streams_processor import StreamsProcessor

pytestmark = pytest.mark.unit
from conftest import run_processor_parse

FILE_KEY = "results_streams_csv"
FILENAME = "results_streams.csv"


def _write_csv(result_dir: Path, content: str) -> Path:
    path = result_dir / FILENAME
    path.write_text(content.strip())
    return path


def test_streams_extracts_version_from_csv_comment(result_dir):
    """Extract benchmark version from '# streams_version_# 5.10' comment."""
    csv = """# streams_version_# 5.10
# Optimization level: O2
Array sizes,16384k,32768k,Start_Date,End_Date
Copy,1.0,2.0,2026-02-04T00:19:56Z,2026-02-04T00:20:00Z"""
    path = _write_csv(result_dir, csv)

    processor = StreamsProcessor(str(result_dir))
    processor.parse_runs({"files": {FILE_KEY: str(path)}})
    test_info = processor.build_test_info()

    assert test_info.version == "5.10", "Should extract benchmark version from CSV comment"
    assert test_info.wrapper_version is not None, "Should preserve wrapper version"


def test_streams_version_with_whitespace_variations(result_dir):
    """Handle various whitespace around version number."""
    csv = """#streams_version_#   5.10
# Optimization level: O2
Array sizes,16384k,Start_Date,End_Date
Copy,1.0,2026-02-04T00:19:56Z,2026-02-04T00:20:00Z"""
    path = _write_csv(result_dir, csv)

    processor = StreamsProcessor(str(result_dir))
    processor.parse_runs({"files": {FILE_KEY: str(path)}})
    test_info = processor.build_test_info()

    assert test_info.version == "5.10"


def test_streams_version_missing_fallback_to_wrapper(result_dir):
    """When no benchmark version comment, fall back to wrapper version."""
    csv = """# Optimization level: O2
Array sizes,16384k,Start_Date,End_Date
Copy,1.0,2026-02-04T00:19:56Z,2026-02-04T00:20:00Z"""
    path = _write_csv(result_dir, csv)

    # Create test_info file with wrapper version
    test_info_file = result_dir / "test_info"
    test_info_file.write_text('{"streams": {"test_name": "streams", "repo_file": "v2.8.tar.gz"}}')

    processor = StreamsProcessor(str(result_dir))
    processor.parse_runs({"files": {FILE_KEY: str(path)}})
    test_info = processor.build_test_info()

    assert test_info.version == "v2.8", "Should fall back to wrapper version when no benchmark version"
    assert test_info.wrapper_version == "v2.8"


def test_streams_version_only_uses_first_occurrence(result_dir):
    """If multiple version comments, use first one."""
    csv = """# streams_version_# 5.10
# Optimization level: O2
# streams_version_# 6.0
Array sizes,16384k,Start_Date,End_Date
Copy,1.0,2026-02-04T00:19:56Z,2026-02-04T00:20:00Z"""
    path = _write_csv(result_dir, csv)

    processor = StreamsProcessor(str(result_dir))
    processor.parse_runs({"files": {FILE_KEY: str(path)}})
    test_info = processor.build_test_info()

    assert test_info.version == "5.10", "Should use first version comment"


def test_streams_version_different_formats(result_dir):
    """Handle different version number formats (x.y, x.y.z, vX.Y, etc)."""
    test_cases = [
        ("5.10", "5.10"),
        ("5.10.1", "5.10.1"),
        ("v5.10", "v5.10"),
        ("2024.1", "2024.1"),
    ]

    for version_str, expected in test_cases:
        csv = f"""# streams_version_# {version_str}
Array sizes,16384k,Start_Date,End_Date
Copy,1.0,2026-02-04T00:19:56Z,2026-02-04T00:20:00Z"""
        path = _write_csv(result_dir, csv)

        processor = StreamsProcessor(str(result_dir))
        processor.parse_runs({"files": {FILE_KEY: str(path)}})
        test_info = processor.build_test_info()

        assert test_info.version == expected, f"Should handle version format: {version_str}"
