"""
Integration test for Passmark multi-metric processing.

Demonstrates end-to-end processing of Passmark results with both CPU and Memory marks.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

from chronicler.processors.passmark_processor import PassmarkProcessor

pytestmark = pytest.mark.integration


def test_passmark_processor_multi_metric_extraction(result_dir):
    """
    Processor-level integration test: Passmark YML → parsed runs → Results with 2 primary_metrics.

    This demonstrates the complete processing flow for RPOPC-1307.
    Passmark reports CPU Mark (SUMM_CPU) and Memory Mark (SUMM_ME).
    """
    # Create realistic Passmark YML with CPU and Memory summary scores
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
  CPU_FLOATINGPOINT_MATH: 52092.075564261577
  CPU_ENCRYPTION: 16095.081709056702
  ME_ALLOC_S: 7821.9859399764991
  ME_READ_S: 26985.984375
  ME_WRITE: 11951.0166015625
  SUMM_CPU: 24685.407296119931
  SUMM_ME: 2106.6563041301797
SystemInformation:
  OSName: Red Hat Enterprise Linux 9.8 (Plow)
  Processor: Intel Xeon Platinum 8488C
"""

    yml_path = result_dir / "results_all_1.yml"
    yml_path.write_text(yml_content)

    # Create dummy zip
    dummy_zip = result_dir / "results_passmark.zip"
    dummy_zip.write_bytes(b"")

    # Process with Passmark processor
    processor = PassmarkProcessor(str(result_dir))
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
    assert "CPU Mark" in metrics_by_name, "Missing CPU Mark metric"
    assert "Memory Mark" in metrics_by_name, "Missing Memory Mark metric"

    # Verify units
    assert metrics_by_name["CPU Mark"].unit == "score"
    assert metrics_by_name["Memory Mark"].unit == "score"

    # Verify values (from SUMM_CPU and SUMM_ME in yml)
    # CPU Mark = SUMM_CPU = 24685.407296119931
    assert metrics_by_name["CPU Mark"].value == pytest.approx(24685.407296119931)

    # Memory Mark = SUMM_ME = 2106.6563041301797
    assert metrics_by_name["Memory Mark"].value == pytest.approx(2106.6563041301797)


def test_passmark_processor_multi_metric_with_multiple_iterations(result_dir):
    """
    Test primary_metrics extraction with multiple iterations.

    Should extract mean values from aggregated metrics (SUMM_CPU_mean, SUMM_ME_mean).
    """
    # Create multiple YML files (simulating multiple iterations)
    yml_content_1 = """BaselineInfo:
  WebDBID: -1
  TimeStamp: 20260502203522
Version:
  Major: 11
Results:
  NumTestProcesses: 16
  CPU_INTEGER_MATH: 70000.0
  SUMM_CPU: 24000.0
  SUMM_ME: 2000.0
SystemInformation:
  OSName: Red Hat Enterprise Linux 9.8
"""

    yml_content_2 = """BaselineInfo:
  WebDBID: -1
  TimeStamp: 20260502204522
Version:
  Major: 11
Results:
  NumTestProcesses: 16
  CPU_INTEGER_MATH: 75000.0
  SUMM_CPU: 25000.0
  SUMM_ME: 2200.0
SystemInformation:
  OSName: Red Hat Enterprise Linux 9.8
"""

    yml_path_1 = result_dir / "results_all_1.yml"
    yml_path_1.write_text(yml_content_1)
    yml_path_2 = result_dir / "results_all_2.yml"
    yml_path_2.write_text(yml_content_2)

    # Create dummy zip
    dummy_zip = result_dir / "results_passmark.zip"
    dummy_zip.write_bytes(b"")

    # Process with Passmark processor
    processor = PassmarkProcessor(str(result_dir))
    extracted_result = {"files": {}, "extracted_path": str(result_dir)}

    with patch.object(processor.archive_handler, "extract_result_archive") as mock_extract:
        mock_extract.return_value = extracted_result
        results = processor.build_results()

    # Verify primary_metrics
    assert results.primary_metrics is not None
    assert len(results.primary_metrics) == 2, "Should have exactly 2 primary metrics"

    metrics_by_name = {m.name: m for m in results.primary_metrics}

    # Verify both metrics present
    assert "CPU Mark" in metrics_by_name
    assert "Memory Mark" in metrics_by_name

    # Verify mean values
    # CPU Mark mean = (24000 + 25000) / 2 = 24500
    assert metrics_by_name["CPU Mark"].value == pytest.approx(24500.0)

    # Memory Mark mean = (2000 + 2200) / 2 = 2100
    assert metrics_by_name["Memory Mark"].value == pytest.approx(2100.0)
