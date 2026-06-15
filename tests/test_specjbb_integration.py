"""
Integration test for SpecJBB multi-metric processing.

Demonstrates end-to-end processing of SpecJBB results with both metrics.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.specjbb_processor import SpecJBBProcessor

pytestmark = pytest.mark.integration


def test_specjbb_processor_multi_metric_extraction(result_dir):
    """
    Processor-level integration test: SpecJBB CSV → parsed runs → Results with 2 primary_metrics.

    This demonstrates the complete processing flow for RPOPC-1306.
    SpecJBB reports Critical-jOPS (overall_score_bops) and Max-jOPS (peak_throughput_bops).
    """
    # Create realistic SpecJBB CSV with warehouse configurations
    csv_content = """Warehouses,Bops,Numb_JVMs,Start_Date,End_Date
2,50000,1,2026-06-01T10:00:00Z,2026-06-01T10:05:00Z
4,95000,1,2026-06-01T10:05:00Z,2026-06-01T10:10:00Z
6,135000,1,2026-06-01T10:10:00Z,2026-06-01T10:15:00Z
8,165000,1,2026-06-01T10:15:00Z,2026-06-01T10:20:00Z
10,180000,1,2026-06-01T10:20:00Z,2026-06-01T10:25:00Z
12,185000,1,2026-06-01T10:25:00Z,2026-06-01T10:30:00Z
14,182000,1,2026-06-01T10:30:00Z,2026-06-01T10:35:00Z
16,178000,1,2026-06-01T10:35:00Z,2026-06-01T10:40:00Z"""

    csv_path = result_dir / "results_specjbb.csv"
    csv_path.write_text(csv_content)

    # Create .txt file with overall score (Critical-jOPS)
    txt_content = """SPECjbb2005 Result Report

Test Configuration:
Number of JVMs: 1

Results:
SPECjbb2005 bops = 170000, SPECjbb2005 bops/JVM = 170000

Throughput      170000
"""

    txt_path = result_dir / "SPECjbb2005.001.txt"
    txt_path.write_text(txt_content)

    # Create dummy zip
    dummy_zip = result_dir / "results_specjbb.zip"
    dummy_zip.write_bytes(b"")

    # Process with SpecJBB processor
    processor = SpecJBBProcessor(str(result_dir))
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
    assert len(results.primary_metrics) == 2, "Should have exactly 2 primary metrics"

    metrics_by_name = {m.name: m for m in results.primary_metrics}

    # Verify both metrics present
    assert "Critical-jOPS" in metrics_by_name, "Missing Critical-jOPS metric"
    assert "Max-jOPS" in metrics_by_name, "Missing Max-jOPS metric"

    # Verify units
    assert metrics_by_name["Critical-jOPS"].unit == "Bops"
    assert metrics_by_name["Max-jOPS"].unit == "Bops"

    # Verify values
    # Critical-jOPS = overall_score_bops = 170000 (from .txt file)
    assert metrics_by_name["Critical-jOPS"].value == 170000

    # Max-jOPS = peak_throughput_bops = 185000 (from CSV, warehouses=12)
    assert metrics_by_name["Max-jOPS"].value == 185000


def test_specjbb_processor_multi_metric_without_txt_file(result_dir):
    """
    Test primary_metrics extraction when .txt file is missing.

    Should still extract Max-jOPS from CSV peak throughput.
    Critical-jOPS should be omitted if overall_score is not available.
    """
    # Create CSV without .txt file
    csv_content = """Warehouses,Bops,Numb_JVMs,Start_Date,End_Date
2,50000,1,2026-06-01T10:00:00Z,2026-06-01T10:05:00Z
4,95000,1,2026-06-01T10:05:00Z,2026-06-01T10:10:00Z
6,135000,1,2026-06-01T10:10:00Z,2026-06-01T10:15:00Z"""

    csv_path = result_dir / "results_specjbb.csv"
    csv_path.write_text(csv_content)

    # Create dummy zip
    dummy_zip = result_dir / "results_specjbb.zip"
    dummy_zip.write_bytes(b"")

    # Process with SpecJBB processor
    processor = SpecJBBProcessor(str(result_dir))
    extracted_result = {"files": {}, "extracted_path": str(result_dir)}

    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result
        results = processor.build_results()

    # Verify primary_metrics
    assert results.primary_metrics is not None
    assert len(results.primary_metrics) == 1, "Should have only Max-jOPS when .txt is missing"

    metrics_by_name = {m.name: m for m in results.primary_metrics}

    # Verify only Max-jOPS is present
    assert "Max-jOPS" in metrics_by_name, "Missing Max-jOPS metric"
    assert "Critical-jOPS" not in metrics_by_name, "Critical-jOPS should be omitted without .txt file"

    # Verify Max-jOPS value
    assert metrics_by_name["Max-jOPS"].value == 135000
