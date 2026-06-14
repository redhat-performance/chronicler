"""
Integration test for uperf multi-metric processing.

Demonstrates end-to-end processing of uperf results with all three metrics.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.uperf_processor import UperfProcessor

pytestmark = pytest.mark.integration


def test_uperf_integration_end_to_end(result_dir):
    """
    End-to-end test: uperf CSV → parsed runs → Results with 3 primary_metrics.

    This demonstrates the complete flow for RPOPC-1275.
    """
    # Create realistic uperf CSV with varying workloads
    csv_content = """number_procs,Gb_Sec,trans_sec,lat_usec,test_type,packet_type,packet_size,Start_Date,End_Date
1,8.2,45000.0,120.5,stream,tcp,1024,2026-06-01T10:00:00Z,2026-06-01T10:00:30Z
2,15.8,88000.0,125.2,stream,tcp,1024,2026-06-01T10:01:00Z,2026-06-01T10:01:30Z
4,29.5,170000.0,135.8,stream,tcp,1024,2026-06-01T10:02:00Z,2026-06-01T10:02:30Z
8,52.3,305000.0,148.3,stream,tcp,1024,2026-06-01T10:03:00Z,2026-06-01T10:03:30Z
1,9.1,51000.0,118.9,stream,tcp,16384,2026-06-01T10:05:00Z,2026-06-01T10:05:30Z
2,17.6,96000.0,122.1,stream,tcp,16384,2026-06-01T10:06:00Z,2026-06-01T10:06:30Z
4,32.8,184000.0,130.5,stream,tcp,16384,2026-06-01T10:07:00Z,2026-06-01T10:07:30Z
8,58.9,325000.0,142.7,stream,tcp,16384,2026-06-01T10:08:00Z,2026-06-01T10:08:30Z"""

    csv_path = result_dir / "results_uperf.csv"
    csv_path.write_text(csv_content)

    # Create dummy zip
    dummy_zip = result_dir / "results_uperf.zip"
    dummy_zip.write_bytes(b"")

    # Process with uperf processor
    processor = UperfProcessor(str(result_dir))
    extracted_result = {"files": {}, "extracted_path": str(result_dir)}

    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result
        results = processor.build_results()

    # Verify structure
    assert results is not None
    assert results.status == "PASS"
    assert results.total_runs == 1
    assert results.runs is not None
    assert "run_0" in results.runs

    # Verify primary_metrics
    assert results.primary_metrics is not None
    assert len(results.primary_metrics) == 3, "Should have exactly 3 primary metrics"

    metrics_by_name = {m.name: m for m in results.primary_metrics}

    # Verify all three metrics present
    assert "throughput" in metrics_by_name, "Missing throughput metric"
    assert "latency" in metrics_by_name, "Missing latency metric"
    assert "transaction_rate" in metrics_by_name, "Missing transaction_rate metric"

    # Verify units
    assert metrics_by_name["throughput"].unit == "Gb/s"
    assert metrics_by_name["latency"].unit == "microseconds"
    assert metrics_by_name["transaction_rate"].unit == "trans/s"

    # Verify values are reasonable (means of all data points)
    # Throughput mean: (8.2+15.8+29.5+52.3+9.1+17.6+32.8+58.9) / 8 = 28.025
    assert 25.0 < metrics_by_name["throughput"].value < 31.0

    # Latency mean: (120.5+125.2+135.8+148.3+118.9+122.1+130.5+142.7) / 8 = 130.5
    assert 125.0 < metrics_by_name["latency"].value < 135.0

    # Transaction rate mean: (45k+88k+170k+305k+51k+96k+184k+325k) / 8 = 158,000
    assert 150000 < metrics_by_name["transaction_rate"].value < 165000

    # Verify timeseries contains all data points
    run = results.runs["run_0"]
    assert run.timeseries is not None
    assert len(run.timeseries) == 8, "Should have 8 timeseries points"

    # Spot-check first timeseries point has all three metrics
    first_point = run.timeseries["sequence_0"]
    assert "throughput_gbps" in first_point.metrics
    assert "latency_usec" in first_point.metrics
    assert "iops" in first_point.metrics

    print("\n✓ Integration test passed:")
    print(f"  Throughput:       {metrics_by_name['throughput'].value:.2f} Gb/s")
    print(f"  Latency:          {metrics_by_name['latency'].value:.2f} microseconds")
    print(f"  Transaction Rate: {metrics_by_name['transaction_rate'].value:.0f} trans/s")
    print(f"  Timeseries points: {len(run.timeseries)}")
