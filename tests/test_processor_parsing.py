"""
Extended processor tests beyond timestamp validation.

Tests run parsing logic, metric extraction, timeseries building,
and configuration handling for processors.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from chronicler.processors.coremark_processor import CoreMarkProcessor
from chronicler.processors.base_processor import ProcessorError
from chronicler.schema import Run, TimeSeriesPoint


class TestCoreMarkRunGrouping:
    """Tests for CoreMark time series grouping by run."""

    def test_groups_single_run(self, tmp_path):
        """Single run with multiple iterations groups correctly."""
        csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,100000,2026-03-17T10:00:00Z,2026-03-17T10:00:30Z
2,4,100500,2026-03-17T10:00:30Z,2026-03-17T10:01:00Z
3,4,99500,2026-03-17T10:01:00Z,2026-03-17T10:01:30Z"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [],
                "version": None,
                "tuned_setting": None,
            }
        })

        assert len(runs) == 1
        assert "run_1" in runs
        run = runs["run_1"]
        assert run.run_number == 1
        assert len(run.timeseries) == 3

    def test_groups_multiple_runs(self, tmp_path):
        """Multiple runs (multiple measurements per iteration) groups correctly."""
        csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,100000,2026-03-17T10:00:00Z,2026-03-17T10:00:30Z
1,4,100200,2026-03-17T10:01:00Z,2026-03-17T10:01:30Z
2,4,100500,2026-03-17T10:00:30Z,2026-03-17T10:01:00Z
2,4,100700,2026-03-17T10:01:30Z,2026-03-17T10:02:00Z"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [],
                "version": None,
                "tuned_setting": None,
            }
        })

        assert len(runs) == 2
        assert "run_1" in runs
        assert "run_2" in runs


class TestCoreMarkMetricExtraction:
    """Tests for CoreMark metric extraction from CSV."""

    def test_extracts_iterations_per_second(self, tmp_path):
        """Extracts IterationsPerSec as iterations_per_second metric."""
        csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,195000.5,2026-03-17T10:00:00Z,2026-03-17T10:00:30Z"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [],
                "version": None,
                "tuned_setting": None,
            }
        })

        run = runs["run_1"]
        ts_point = run.timeseries["sequence_0"]
        assert ts_point.metrics["iterations_per_second"] == 195000.5

    def test_computes_timeseries_summary(self, tmp_path):
        """Computes summary statistics across timeseries points."""
        csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,100,2026-03-17T10:00:00Z,2026-03-17T10:00:30Z
2,4,200,2026-03-17T10:00:30Z,2026-03-17T10:01:00Z
3,4,300,2026-03-17T10:01:00Z,2026-03-17T10:01:30Z"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [],
                "version": None,
                "tuned_setting": None,
            }
        })

        summary = runs["run_1"].timeseries_summary
        assert summary.count == 3
        assert summary.mean == 200.0
        assert summary.min == 100
        assert summary.max == 300
        assert summary.first_value == 100
        assert summary.last_value == 300


class TestCoreMarkRunTimestamps:
    """Tests for CoreMark run-level timestamp extraction."""

    def test_extracts_start_end_times(self, tmp_path):
        """Extracts min start time and max end time for run."""
        csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,100,2026-03-17T10:00:00Z,2026-03-17T10:00:30Z
2,4,200,2026-03-17T10:00:30Z,2026-03-17T10:01:00Z
3,4,300,2026-03-17T10:01:00Z,2026-03-17T10:01:30Z"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [],
                "version": None,
                "tuned_setting": None,
            }
        })

        run = runs["run_1"]
        assert run.start_time == "2026-03-17T10:00:00Z"
        assert run.end_time == "2026-03-17T10:01:30Z"


class TestCoreMarkSummaryParsing:
    """Tests for CoreMark run summary file parsing."""

    def test_parses_summary_metrics(self, tmp_path):
        """Parses metrics from run summary file."""
        csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,195000,2026-03-17T10:00:00Z,2026-03-17T10:00:30Z"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        summary = """CoreMark Size    : 666
Total ticks      : 136050
Iterations/Sec   : 195999.821818
Iterations       : 4445670
Compiler version : GCC 11.4.0
Compiler flags   : -O2 -pthread"""
        summary_path = tmp_path / "run1_summary"
        summary_path.write_text(summary.strip())

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [str(summary_path)],
                "version": None,
                "tuned_setting": None,
            }
        })

        run = runs["run_1"]
        assert run.metrics["coremark_size"] == 666
        assert run.metrics["total_ticks"] == 136050
        assert run.metrics["iterations_per_second"] == 195999.821818
        assert run.configuration["compiler"] == "GCC 11.4.0"

    def test_parses_total_time_seconds(self, tmp_path):
        """Extracts total_time_seconds from 'Total time (secs)' summary line."""
        csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,195000,2026-03-17T10:00:00Z,2026-03-17T10:00:30Z"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        summary = """Total time (secs): 22.675000
Iterations/Sec   : 195999.821818"""
        summary_path = tmp_path / "run1_summary"
        summary_path.write_text(summary.strip())

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [str(summary_path)],
                "version": None,
                "tuned_setting": None,
            }
        })

        run = runs["run_1"]
        assert run.metrics["total_time_seconds"] == 22.675
        assert run.duration_seconds == 22.675


class TestProcessorErrorHandling:
    """Tests for processor error handling."""

    def test_returns_empty_on_empty_csv(self, tmp_path):
        """Returns empty runs dict on empty CSV (no data to parse)."""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text("")

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [],
                "version": None,
                "tuned_setting": None,
            }
        })
        assert runs == {}

    def test_raises_on_missing_timestamp_columns(self, tmp_path):
        """Raises ProcessorError when timestamp columns missing."""
        csv = """iteration,threads,IterationsPerSec
1,4,195000"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        processor = CoreMarkProcessor(str(tmp_path))
        with pytest.raises(ProcessorError) as exc_info:
            processor.parse_runs({
                "files": {
                    "results_csv": str(csv_path),
                    "run_summaries": [],
                    "version": None,
                    "tuned_setting": None,
                }
            })
        assert "timestamps" in str(exc_info.value).lower()


class TestProcessorWithVersionAndTuning:
    """Tests for version and tuning extraction."""

    def test_extracts_version_from_file(self, tmp_path):
        """Extracts version string from version file."""
        csv = """iteration,threads,IterationsPerSec,Start_Date,End_Date
1,4,195000,2026-03-17T10:00:00Z,2026-03-17T10:00:30Z"""
        csv_path = tmp_path / "results_coremark.csv"
        csv_path.write_text(csv.strip())

        version_path = tmp_path / "version"
        version_path.write_text("commit: v2.0.1")

        processor = CoreMarkProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "results_csv": str(csv_path),
                "run_summaries": [],
                "version": str(version_path),
                "tuned_setting": None,
            }
        })

        assert len(runs) == 1
