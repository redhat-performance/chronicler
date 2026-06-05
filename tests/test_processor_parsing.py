"""
Extended processor tests beyond timestamp validation.

Tests run parsing logic, metric extraction, timeseries building,
and configuration handling for processors.
"""

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from chronicler.processors.coremark_processor import CoreMarkProcessor
from chronicler.processors.fio_processor import FioProcessor
from chronicler.processors.base_processor import ProcessorError
from chronicler.schema import (
    Metadata,
    Results,
    Run,
    SystemUnderTest,
    TestConfiguration,
    TestInfo,
    TimeSeriesPoint,
    ZathrasDocument,
)


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


class TestFioFieldLimitProtection:
    """Tests for FIO OpenSearch field-limit protections."""

    def test_parse_runs_uses_scalar_timeseries_metrics(self, tmp_path):
        workload_dir = tmp_path / "1-read-4KiB"
        workload_dir.mkdir()

        fio_json = workload_dir / "fio-results.json"
        fio_json.write_text(json.dumps(self._fio_results(num_jobs=2)))

        self._write_fio_logs(workload_dir, job_number=1, bandwidth=(100, 120))
        self._write_fio_logs(workload_dir, job_number=2, bandwidth=(200, 220))

        processor = FioProcessor(str(tmp_path))
        runs = processor.parse_runs({
            "files": {
                "fio_results_json": str(fio_json),
            }
        })

        run = runs["run_0"]
        assert "jobs" not in run.metrics

        point = run.timeseries["sequence_0"]
        assert "jobs" not in point.metrics
        assert point.metrics["total_bandwidth_kbps"] == 300
        assert point.metrics["job_0_bandwidth_kbps"] == 100
        assert point.metrics["job_1_bandwidth_kbps"] == 200
        assert all(
            not isinstance(value, (dict, list, tuple))
            for value in point.metrics.values()
        )

    def test_process_multiple_splits_workloads_into_separate_documents(
        self, tmp_path, monkeypatch
    ):
        processor = FioProcessor(str(tmp_path))
        runs = {
            "run_0": Run(
                run_number=0,
                status="PASS",
                metrics={"total_bandwidth_kbps": 300},
                configuration={
                    "operation": "read",
                    "block_size": "4KiB",
                    "iodepth": 16,
                },
                timeseries={
                    "sequence_0": TimeSeriesPoint(
                        timestamp="2026-05-10T12:00:00Z",
                        metrics={"total_bandwidth_kbps": 300},
                    )
                },
            ),
            "run_1": Run(
                run_number=1,
                status="PASS",
                metrics={"total_bandwidth_kbps": 900},
                configuration={
                    "operation": "write",
                    "block_size": "1MiB",
                    "iodepth": 32,
                },
            ),
        }

        monkeypatch.setattr(
            processor,
            "build_metadata",
            lambda: Metadata(
                document_id="fio_base",
                scenario_name="scenario",
                processing_timestamp="2026-05-10T12:00:00Z",
            ),
        )
        monkeypatch.setattr(
            processor,
            "build_test_info",
            lambda: TestInfo(name="fio", version="unknown"),
        )
        monkeypatch.setattr(processor, "build_system_under_test", SystemUnderTest)
        monkeypatch.setattr(processor, "build_test_configuration", TestConfiguration)
        monkeypatch.setattr(
            processor,
            "build_results",
            lambda: Results(status="PASS", total_runs=2, runs=runs),
        )
        monkeypatch.setattr(processor, "build_runtime_info", lambda: None)

        documents = processor.process_multiple()

        assert len(documents) == 2
        assert [list(doc.results.runs.keys()) for doc in documents] == [
            ["run_0"],
            ["run_0"],
        ]
        assert documents[0].metadata.document_id.startswith(
            "fio_read_4kib_iodepth_16_run_0_"
        )
        assert documents[1].metadata.document_id.startswith(
            "fio_write_1mib_iodepth_32_run_1_"
        )
        assert documents[0].metadata.scenario_name == (
            "scenario_read_4kib_iodepth_16_run_0"
        )

    def test_schema_validation_rejects_nested_timeseries_metrics(self):
        document = ZathrasDocument(
            metadata=Metadata(document_id="fio_doc"),
            test=TestInfo(name="fio", version="unknown"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(
                status="PASS",
                runs={
                    "run_0": Run(
                        run_number=0,
                        status="PASS",
                        timeseries={
                            "sequence_0": TimeSeriesPoint(
                                timestamp="2026-05-10T12:00:00Z",
                                metrics={
                                    "total_bandwidth_kbps": 300,
                                    "jobs": [{"job_number": 0}],
                                },
                            )
                        },
                    )
                },
            ),
        )

        is_valid, errors = document.validate()

        assert not is_valid
        assert any("must be a scalar value" in error for error in errors)

    @staticmethod
    def _fio_results(num_jobs):
        jobs = []
        for job_number in range(num_jobs):
            jobs.append({
                "jobname": f"job-{job_number}",
                "elapsed": 2,
                "job options": {
                    "filename": f"/dev/nvme{job_number}n1",
                },
                "usr_cpu": 1.0,
                "sys_cpu": 2.0,
                "read": {
                    "io_bytes": 4096,
                    "total_ios": 4,
                    "bw": 100 * (job_number + 1),
                    "bw_min": 90,
                    "bw_max": 120,
                    "bw_mean": 100,
                    "iops": 10 * (job_number + 1),
                    "iops_min": 8,
                    "iops_max": 12,
                    "iops_mean": 10,
                    "runtime": 2000,
                    "lat_ns": {
                        "mean": 1000,
                        "min": 900,
                        "max": 1100,
                        "stddev": 10,
                    },
                    "clat_ns": {
                        "mean": 800,
                        "min": 700,
                        "max": 900,
                        "stddev": 8,
                        "percentile": {
                            "1.000000": 700,
                            "5.000000": 720,
                            "10.000000": 740,
                            "50.000000": 800,
                            "90.000000": 860,
                            "95.000000": 880,
                            "99.000000": 900,
                            "99.500000": 920,
                            "99.900000": 940,
                        },
                    },
                    "slat_ns": {
                        "mean": 200,
                        "min": 100,
                        "max": 300,
                        "stddev": 2,
                    },
                },
                "write": {
                    "io_bytes": 0,
                },
            })

        return {
            "timestamp": 1780000000,
            "global options": {
                "iodepth": "16",
                "runtime": "2",
                "time_based": "1",
            },
            "jobs": jobs,
        }

    @staticmethod
    def _write_fio_logs(workload_dir, job_number, bandwidth):
        suffix = f".{job_number}.log"
        samples = "\n".join(
            f"{index * 1000},{value},0,0,0"
            for index, value in enumerate(bandwidth)
        )
        iops = "\n".join(
            f"{index * 1000},{value / 10},0,0,0"
            for index, value in enumerate(bandwidth)
        )
        latency = "\n".join(
            f"{index * 1000},{1000 + index},0,0,0"
            for index, _ in enumerate(bandwidth)
        )

        (workload_dir / f"fio_bw{suffix}").write_text(samples)
        (workload_dir / f"fio_iops{suffix}").write_text(iops)
        (workload_dir / f"fio_lat{suffix}").write_text(latency)
        (workload_dir / f"fio_clat{suffix}").write_text(latency)
        (workload_dir / f"fio_slat{suffix}").write_text(latency)


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
