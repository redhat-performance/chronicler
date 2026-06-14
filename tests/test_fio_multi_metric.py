"""
FIO processor: multi-metric extraction (bandwidth, IOPS, latency).

Tests that FIO extracts all three coequal metrics as primary_metrics.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.fio_processor import FioProcessor
from chronicler.processors.base_processor import ProcessorError
from chronicler.schema import Run

pytestmark = pytest.mark.unit


def test_fio_extracts_all_three_primary_metrics(result_dir):
    """
    FIO should extract bandwidth, IOPS, and latency as primary_metrics.

    Test validates RPOPC-1304: multi-metric support for FIO.
    """
    # Create FIO results JSON with multiple runs to test mean calculation
    # Run 0: read-4KiB workload
    fio_data_run0 = {
        "timestamp": 1707004800,  # 2024-02-04 00:00:00 UTC
        "jobs": [
            {
                "jobname": "job0",
                "read": {
                    "bw": 500000,
                    "iops": 125000,
                    "io_bytes": 1024000000,
                    "total_ios": 250000,
                    "lat_ns": {
                        "mean": 120000,
                        "min": 100000,
                        "max": 150000
                    },
                    "clat_ns": {
                        "mean": 118000,
                        "min": 98000,
                        "max": 148000
                    },
                    "slat_ns": {
                        "mean": 2000,
                        "min": 1000,
                        "max": 3000
                    }
                },
                "elapsed": 60
            }
        ]
    }

    # Run 1: read-1024KiB workload (different metrics)
    fio_data_run1 = {
        "timestamp": 1707004920,  # 2024-02-04 00:02:00 UTC
        "jobs": [
            {
                "jobname": "job0",
                "read": {
                    "bw": 800000,
                    "iops": 200000,
                    "io_bytes": 2048000000,
                    "total_ios": 500000,
                    "lat_ns": {
                        "mean": 140000,
                        "min": 120000,
                        "max": 170000
                    },
                    "clat_ns": {
                        "mean": 138000,
                        "min": 118000,
                        "max": 168000
                    },
                    "slat_ns": {
                        "mean": 2000,
                        "min": 1000,
                        "max": 3000
                    }
                },
                "elapsed": 60
            }
        ]
    }

    # Create directory structure for two workloads
    export_dir = result_dir / "export_fio_data_test"
    export_dir.mkdir()

    config_dir = export_dir / "fio_ndisks_1_disksize_10_GiB_njobs_1_ioengine_libaio_iodepth_16_2024.02.04T00.00.00"
    config_dir.mkdir()

    # Workload 0
    workload0_dir = config_dir / "1-read-4KiB"
    workload0_dir.mkdir()
    (workload0_dir / "fio-results.json").write_text(json.dumps(fio_data_run0))

    # Workload 1
    workload1_dir = config_dir / "2-read-1024KiB"
    workload1_dir.mkdir()
    (workload1_dir / "fio-results.json").write_text(json.dumps(fio_data_run1))

    # Create test_results_report (status)
    (export_dir / "test_results_report").write_text("Ran 2 tests")

    # Create dummy zip
    dummy_zip = result_dir / "results_fio.zip"
    dummy_zip.write_bytes(b"")

    # Process FIO results
    processor = FioProcessor(str(result_dir))
    extracted_result = {"files": {}, "extracted_path": str(result_dir)}

    # Mock archive extraction and call build_results
    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result
        results = processor.build_results()

    # Verify primary_metrics exists and has 3 metrics
    assert results.primary_metrics is not None, "primary_metrics should not be None"
    assert len(results.primary_metrics) == 3, f"Expected 3 metrics, got {len(results.primary_metrics)}"

    # Extract metric names and values
    metrics_by_name = {m.name: m for m in results.primary_metrics}

    # Verify all three metrics are present
    assert "bandwidth" in metrics_by_name, "bandwidth metric missing"
    assert "iops" in metrics_by_name, "iops metric missing"
    assert "latency" in metrics_by_name, "latency metric missing"

    # Verify units are correct
    assert metrics_by_name["bandwidth"].unit == "KiB/s"
    assert metrics_by_name["iops"].unit == "IOPS"
    assert metrics_by_name["latency"].unit == "nanoseconds"

    # Verify values are means across both runs
    # Bandwidth mean: (500000 + 800000) / 2 = 650000
    assert metrics_by_name["bandwidth"].value == 650000.0

    # IOPS mean: (125000 + 200000) / 2 = 162500
    assert metrics_by_name["iops"].value == 162500.0

    # Latency mean: (120000 + 140000) / 2 = 130000
    assert metrics_by_name["latency"].value == 130000.0


def test_fio_handles_single_run(result_dir):
    """
    FIO should extract metrics correctly from single run.

    No averaging needed when only one run exists.
    """
    fio_data = {
        "timestamp": 1707004800,
        "jobs": [
            {
                "jobname": "job0",
                "read": {
                    "bw": 600000,
                    "iops": 150000,
                    "io_bytes": 1024000000,
                    "total_ios": 250000,
                    "lat_ns": {
                        "mean": 125000,
                        "min": 110000,
                        "max": 160000
                    },
                    "clat_ns": {
                        "mean": 123000,
                        "min": 108000,
                        "max": 158000
                    },
                    "slat_ns": {
                        "mean": 2000,
                        "min": 1000,
                        "max": 3000
                    }
                },
                "elapsed": 60
            }
        ]
    }

    # Create proper directory structure (needed for operation type detection)
    export_dir = result_dir / "export_fio_data_test"
    export_dir.mkdir()

    config_dir = export_dir / "fio_ndisks_1_disksize_10_GiB_njobs_1_ioengine_libaio_iodepth_16_2024.02.04T00.00.00"
    config_dir.mkdir()

    workload_dir = config_dir / "1-read-4KiB"
    workload_dir.mkdir()
    (workload_dir / "fio-results.json").write_text(json.dumps(fio_data))

    (export_dir / "test_results_report").write_text("Ran 1 test")

    dummy_zip = result_dir / "results_fio.zip"
    dummy_zip.write_bytes(b"")

    processor = FioProcessor(str(result_dir))
    extracted_result = {"files": {}, "extracted_path": str(result_dir)}

    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result
        results = processor.build_results()

    # Should have exactly 3 metrics from single run
    assert results.primary_metrics is not None
    assert len(results.primary_metrics) == 3

    metrics_by_name = {m.name: m for m in results.primary_metrics}

    # Values should match the single run (no averaging)
    assert metrics_by_name["bandwidth"].value == 600000.0
    assert metrics_by_name["iops"].value == 150000.0
    assert metrics_by_name["latency"].value == 125000.0
