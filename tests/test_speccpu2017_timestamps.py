"""
SPEC CPU 2017 processor: timestamp validation (valid, missing, invalid, empty).
"""

import pytest
from pathlib import Path

from chronicler.processors.speccpu2017_processor import SpecCPU2017Processor

pytestmark = pytest.mark.unit
from conftest import run_processor_parse

FILE_KEY = "results_csv"
FILENAME = "demo_intrate_timestamps.csv"


def _write_csv(result_dir: Path, content: str) -> Path:
    path = result_dir / FILENAME
    path.write_text(content.strip())
    return path


def test_speccpu2017_valid_timestamps(result_dir):
    """Valid CSV with Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date parses successfully."""
    csv = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
500.perlbench_r,1,100.5,9.95,2026-02-26T03:21:32Z,2026-02-26T03:25:00Z"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        SpecCPU2017Processor,
        result_dir,
        {FILE_KEY: str(path)},
        expect_error=False,
        extracted_extra={"extracted_path": str(result_dir)},
    )


def test_speccpu2017_csv_in_nested_results_subdir(result_dir):
    """CSVs in results_speccpu_*/result/ subdirectory (wrapper v2.6+) are discovered."""
    nested = result_dir / "results_speccpu_virtual-guest_2026.06.09" / "result"
    nested.mkdir(parents=True)
    intrate_csv = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
500.perlbench_r,32,494,103,2026-06-09T10:16:39Z,2026-06-09T11:43:31Z"""
    (nested / "CPU2017.003.intrate.refrate.results.csv").write_text(intrate_csv.strip())
    fprate_csv = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
503.bwaves_r,32,726,442,2026-06-09T11:43:49Z,2026-06-09T13:44:59Z"""
    (nested / "CPU2017.004.fprate.refrate.results.csv").write_text(fprate_csv.strip())
    run_processor_parse(
        SpecCPU2017Processor,
        result_dir,
        {},
        expect_error=False,
        extracted_extra={"extracted_path": str(result_dir)},
    )


def test_speccpu2017_no_timestamp_columns(result_dir):
    """Legacy format without Start_Date/End_Date raises ProcessorError."""
    csv = """Benchmark,Base # Copies,Base Run Time,Base Rate
500.perlbench_r,1,100.5,9.95"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        SpecCPU2017Processor,
        result_dir,
        {FILE_KEY: str(path)},
        expect_error=True,
        extracted_extra={"extracted_path": str(result_dir)},
    )


def test_speccpu2017_invalid_timestamp_in_row(result_dir):
    """Malformed Start_Date in a row raises ProcessorError."""
    csv = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
500.perlbench_r,1,100.5,9.95,not-a-date,2026-02-26T03:25:00Z"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        SpecCPU2017Processor,
        result_dir,
        {FILE_KEY: str(path)},
        expect_error=True,
        extracted_extra={"extracted_path": str(result_dir)},
    )


def test_speccpu2017_empty_timestamp_in_row(result_dir):
    """Empty Start_Date in a row raises ProcessorError."""
    csv = """Benchmarks,Base copies,Base Run Time,Base Rate,Start_Date,End_Date
500.perlbench_r,1,100.5,9.95,,2026-02-26T03:25:00Z"""
    path = _write_csv(result_dir, csv)
    run_processor_parse(
        SpecCPU2017Processor,
        result_dir,
        {FILE_KEY: str(path)},
        expect_error=True,
        extracted_extra={"extracted_path": str(result_dir)},
    )
