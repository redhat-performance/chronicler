"""
Uperf processor: multi-metric extraction (throughput, latency, IOPS).

Tests that uperf extracts all three coequal metrics as primary_metrics.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.uperf_processor import UperfProcessor
from chronicler.processors.base_processor import ProcessorError
from chronicler.schema import Run

pytestmark = pytest.mark.unit


def test_uperf_extracts_all_three_primary_metrics(result_dir):
    """
    Uperf should extract throughput, latency, and IOPS as primary_metrics.

    Test validates RPOPC-1275: multi-metric support for uperf.
    """
    # Create CSV with multiple data points to get mean values
    csv_content = """number_procs,Gb_Sec,trans_sec,lat_usec,test_type,packet_type,packet_size,Start_Date,End_Date
1,9.5,50000.0,125.3,stream,tcp,16384,2026-02-04T00:10:00Z,2026-02-04T00:10:30Z
2,18.2,95000.0,130.5,stream,tcp,16384,2026-02-04T00:11:00Z,2026-02-04T00:11:30Z
4,32.4,180000.0,142.1,stream,tcp,16384,2026-02-04T00:12:00Z,2026-02-04T00:12:30Z"""

    csv_path = result_dir / "results_uperf.csv"
    csv_path.write_text(csv_content)

    # Create dummy zip file so build_results doesn't fail
    dummy_zip = result_dir / "results_uperf.zip"
    dummy_zip.write_bytes(b"")

    # Process uperf results
    processor = UperfProcessor(str(result_dir))
    extracted_result = {"files": {}, "extracted_path": str(result_dir)}

    # Mock archive extraction and call build_results
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result
        results = processor.build_results()

    # Verify primary_metrics exists and has 3 metrics
    assert results.primary_metrics is not None
    assert len(results.primary_metrics) == 3

    # Extract metric names and values
    metrics_by_name = {m.name: m for m in results.primary_metrics}

    # Verify all three metrics are present
    assert "throughput" in metrics_by_name
    assert "latency" in metrics_by_name
    assert "transaction_rate" in metrics_by_name

    # Verify units are correct
    assert metrics_by_name["throughput"].unit == "Gb/s"
    assert metrics_by_name["latency"].unit == "microseconds"
    assert metrics_by_name["transaction_rate"].unit == "trans/s"

    # Verify values are means (not exact due to potential floating point)
    # Throughput mean: (9.5 + 18.2 + 32.4) / 3 = 20.03333...
    assert abs(metrics_by_name["throughput"].value - 20.033) < 0.01

    # Latency mean: (125.3 + 130.5 + 142.1) / 3 = 132.633...
    assert abs(metrics_by_name["latency"].value - 132.633) < 0.01

    # Transaction rate mean: (50000 + 95000 + 180000) / 3 = 108333.333...
    assert abs(metrics_by_name["transaction_rate"].value - 108333.33) < 1.0


def test_uperf_handles_missing_metric_columns(result_dir):
    """
    Uperf should handle CSV with some missing metric columns gracefully.

    If only Gb_Sec is present, should extract only throughput.
    """
    csv_content = """number_procs,Gb_Sec,test_type,packet_type,packet_size,Start_Date,End_Date
1,9.5,stream,tcp,16384,2026-02-04T00:10:00Z,2026-02-04T00:10:30Z"""

    csv_path = result_dir / "results_uperf.csv"
    csv_path.write_text(csv_content)

    # Create dummy zip
    dummy_zip = result_dir / "results_uperf.zip"
    dummy_zip.write_bytes(b"")

    processor = UperfProcessor(str(result_dir))
    extracted_result = {"files": {}, "extracted_path": str(result_dir)}

    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result
        results = processor.build_results()

    # Should have exactly throughput (no other metrics with missing columns)
    assert results.primary_metrics is not None
    assert len(results.primary_metrics) == 1

    metrics_by_name = {m.name: m for m in results.primary_metrics}
    assert set(metrics_by_name.keys()) == {"throughput"}
