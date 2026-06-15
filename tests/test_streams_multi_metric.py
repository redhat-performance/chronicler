"""
STREAMS processor: multi-metric extraction (Copy, Scale, Add, Triad).

Tests that STREAMS extracts all four coequal memory bandwidth operations as primary_metrics.
"""

import pytest
from pathlib import Path

from chronicler.processors.streams_processor import StreamsProcessor
from chronicler.schema import Run

pytestmark = pytest.mark.unit


def _write_csv(result_dir: Path, content: str) -> Path:
    """Write STREAMS CSV file."""
    path = result_dir / "results_streams.csv"
    path.write_text(content.strip())
    return path


def test_streams_extracts_all_four_primary_metrics(result_dir):
    """
    STREAMS should extract Copy, Scale, Add, and Triad as primary_metrics.

    Test validates RPOPC-1305: multi-metric support for STREAM.
    """
    # Create CSV with multiple array sizes and all four operations
    # Run has two array sizes (16384k, 32768k) for averaging
    csv = """# STREAM Benchmark Results
# Optimization level: O2
Array sizes,16384k,32768k,Start_Date,End_Date
Copy,100000.5,110000.7,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Scale,95000.3,105000.4,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Add,90000.8,100000.2,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Triad,85000.6,95000.1,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
"""
    path = _write_csv(result_dir, csv)

    processor = StreamsProcessor(str(result_dir))
    extracted_result = {
        "files": {"results_streams_csv": str(path)},
        "extracted_path": str(result_dir)
    }

    # Parse runs
    runs = processor.parse_runs(extracted_result)

    # Extract primary metrics
    primary_metrics = processor._extract_primary_metrics(runs, None)

    # Verify primary_metrics exists and has 4 metrics
    assert primary_metrics is not None, "primary_metrics should not be None"
    assert len(primary_metrics) == 4, f"Expected 4 metrics, got {len(primary_metrics)}"

    # Extract metric names and values
    metrics_by_name = {m.name: m for m in primary_metrics}

    # Verify all four operations are present
    assert "copy" in metrics_by_name, "copy metric missing"
    assert "scale" in metrics_by_name, "scale metric missing"
    assert "add" in metrics_by_name, "add metric missing"
    assert "triad" in metrics_by_name, "triad metric missing"

    # Verify units are correct (MB/s for all)
    assert metrics_by_name["copy"].unit == "MB/s"
    assert metrics_by_name["scale"].unit == "MB/s"
    assert metrics_by_name["add"].unit == "MB/s"
    assert metrics_by_name["triad"].unit == "MB/s"

    # Verify values are means across both array sizes
    # Copy mean: (100000.5 + 110000.7) / 2 = 105000.6
    assert metrics_by_name["copy"].value == pytest.approx(105000.6, abs=0.01)

    # Scale mean: (95000.3 + 105000.4) / 2 = 100000.35
    assert metrics_by_name["scale"].value == pytest.approx(100000.35, abs=0.01)

    # Add mean: (90000.8 + 100000.2) / 2 = 95000.5
    assert metrics_by_name["add"].value == pytest.approx(95000.5, abs=0.01)

    # Triad mean: (85000.6 + 95000.1) / 2 = 90000.35
    assert metrics_by_name["triad"].value == pytest.approx(90000.35, abs=0.01)


def test_streams_handles_single_array_size(result_dir):
    """
    STREAMS should extract metrics correctly from single array size.

    No averaging needed when only one array size exists.
    """
    csv = """# STREAM Benchmark Results
# Optimization level: O3
Array sizes,266240k,Start_Date,End_Date
Copy,120000.0,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Scale,115000.0,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Add,110000.0,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Triad,105000.0,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
"""
    path = _write_csv(result_dir, csv)

    processor = StreamsProcessor(str(result_dir))
    extracted_result = {
        "files": {"results_streams_csv": str(path)},
        "extracted_path": str(result_dir)
    }

    runs = processor.parse_runs(extracted_result)
    primary_metrics = processor._extract_primary_metrics(runs, None)

    # Should have exactly 4 metrics from single array size
    assert primary_metrics is not None
    assert len(primary_metrics) == 4

    metrics_by_name = {m.name: m for m in primary_metrics}

    # Values should match the single array size (no averaging)
    assert metrics_by_name["copy"].value == 120000.0
    assert metrics_by_name["scale"].value == 115000.0
    assert metrics_by_name["add"].value == 110000.0
    assert metrics_by_name["triad"].value == 105000.0


def test_streams_handles_multiple_runs(result_dir):
    """
    STREAMS should average metrics across multiple optimization runs.

    When there are multiple runs (O2, O3), average across all runs.
    """
    csv = """# STREAM Benchmark Results
# Optimization level: O2
Array sizes,16384k,Start_Date,End_Date
Copy,100000.0,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Scale,95000.0,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Add,90000.0,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z
Triad,85000.0,2026-02-04T00:00:00Z,2026-02-04T00:01:00Z

# Optimization level: O3
Array sizes,16384k,Start_Date,End_Date
Copy,110000.0,2026-02-04T00:02:00Z,2026-02-04T00:03:00Z
Scale,105000.0,2026-02-04T00:02:00Z,2026-02-04T00:03:00Z
Add,100000.0,2026-02-04T00:02:00Z,2026-02-04T00:03:00Z
Triad,95000.0,2026-02-04T00:02:00Z,2026-02-04T00:03:00Z
"""
    path = _write_csv(result_dir, csv)

    processor = StreamsProcessor(str(result_dir))
    extracted_result = {
        "files": {"results_streams_csv": str(path)},
        "extracted_path": str(result_dir)
    }

    runs = processor.parse_runs(extracted_result)
    primary_metrics = processor._extract_primary_metrics(runs, None)

    assert primary_metrics is not None
    assert len(primary_metrics) == 4

    metrics_by_name = {m.name: m for m in primary_metrics}

    # Values should be mean across both runs
    # Copy: (100000 + 110000) / 2 = 105000
    assert metrics_by_name["copy"].value == 105000.0

    # Scale: (95000 + 105000) / 2 = 100000
    assert metrics_by_name["scale"].value == 100000.0

    # Add: (90000 + 100000) / 2 = 95000
    assert metrics_by_name["add"].value == 95000.0

    # Triad: (85000 + 95000) / 2 = 90000
    assert metrics_by_name["triad"].value == 90000.0
