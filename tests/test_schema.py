"""
Unit tests for chronicler.schema dataclasses.

Tests serialization (to_dict), validation, content hashing,
and timeseries extraction.
"""

import json
import pytest

from chronicler.schema import (
    Metadata,
    TestInfo,
    CPUInfo,
    MemoryInfo,
    HardwareInfo,
    OperatingSystemInfo,
    ConfigurationInfo,
    SystemUnderTest,
    TestConfiguration,
    PrimaryMetric,
    StatisticalSummary,
    TimeSeriesPoint,
    TimeSeriesSummary,
    Run,
    Results,
    RuntimeInfo,
    ZathrasDocument,
    TimeSeriesMetadata,
    TimeSeriesRun,
    TimeSeriesResults,
    TimeSeriesDocument,
    create_run_key,
    create_sequence_key,
    parse_timestamp_key,
    validate_json_schema,
)


class TestMetadata:
    """Tests for Metadata dataclass."""

    def test_to_dict_required_fields_only(self):
        meta = Metadata(document_id="doc123")
        d = meta.to_dict()
        assert d["document_id"] == "doc123"
        assert d["document_type"] == "zathras_test_result"
        assert d["zathras_version"] == "1.0"
        assert "processing_timestamp" in d

    def test_to_dict_excludes_none_values(self):
        meta = Metadata(document_id="doc123")
        d = meta.to_dict()
        assert "test_timestamp" not in d
        assert "os_vendor" not in d

    def test_to_dict_includes_optional_when_set(self):
        meta = Metadata(
            document_id="doc123",
            test_timestamp="2026-03-17T10:00:00Z",
            os_vendor="rhel",
            cloud_provider="aws",
            instance_type="m5.large",
            iteration=1,
        )
        d = meta.to_dict()
        assert d["test_timestamp"] == "2026-03-17T10:00:00Z"
        assert d["os_vendor"] == "rhel"
        assert d["cloud_provider"] == "aws"
        assert d["instance_type"] == "m5.large"
        assert d["iteration"] == 1


class TestTestInfo:
    """Tests for TestInfo dataclass."""

    def test_to_dict_required_fields(self):
        info = TestInfo(name="coremark", version="1.0.1")
        d = info.to_dict()
        assert d["name"] == "coremark"
        assert d["version"] == "1.0.1"
        assert "description" not in d

    def test_to_dict_all_fields(self):
        info = TestInfo(
            name="coremark",
            version="1.0.1",
            wrapper_version="2.0",
            description="CPU benchmark",
            url="https://example.com/coremark",
        )
        d = info.to_dict()
        assert d["wrapper_version"] == "2.0"
        assert d["description"] == "CPU benchmark"
        assert d["url"] == "https://example.com/coremark"


class TestCPUInfo:
    """Tests for CPUInfo dataclass."""

    def test_to_dict_with_flags(self):
        cpu = CPUInfo(
            vendor="Intel",
            model="Xeon Gold 6248",
            cores=20,
            flags={"avx2": True, "avx512f": True, "sse4_2": True},
        )
        d = cpu.to_dict()
        assert d["vendor"] == "Intel"
        assert d["cores"] == 20
        assert d["flags"] == {"avx2": True, "avx512f": True, "sse4_2": True}

    def test_to_dict_empty_flags(self):
        cpu = CPUInfo(vendor="AMD")
        d = cpu.to_dict()
        assert d["flags"] == {}


class TestHardwareInfo:
    """Tests for HardwareInfo dataclass."""

    def test_to_dict_nested_cpu_memory(self):
        hw = HardwareInfo(
            cpu=CPUInfo(vendor="Intel", cores=8),
            memory=MemoryInfo(total_gb=32, speed_mhz=3200),
        )
        d = hw.to_dict()
        assert d["cpu"]["vendor"] == "Intel"
        assert d["cpu"]["cores"] == 8
        assert d["memory"]["total_gb"] == 32
        assert d["memory"]["speed_mhz"] == 3200

    def test_to_dict_with_numa_storage_network(self):
        hw = HardwareInfo(
            numa={"node_0": {"cpus": "0-7"}, "node_1": {"cpus": "8-15"}},
            storage={"device_0": {"type": "nvme", "size_gb": 500}},
            network={"interface_0": {"speed": "10Gbps"}},
        )
        d = hw.to_dict()
        assert "node_0" in d["numa"]
        assert d["storage"]["device_0"]["type"] == "nvme"
        assert d["network"]["interface_0"]["speed"] == "10Gbps"


class TestTimeSeriesPoint:
    """Tests for TimeSeriesPoint dataclass."""

    def test_to_dict(self):
        point = TimeSeriesPoint(
            timestamp="2026-03-17T10:00:00Z",
            metrics={"throughput_gbps": 12.5, "latency_ms": 0.8},
        )
        d = point.to_dict()
        assert d["timestamp"] == "2026-03-17T10:00:00Z"
        assert d["metrics"]["throughput_gbps"] == 12.5
        assert d["metrics"]["latency_ms"] == 0.8


class TestTimeSeriesSummary:
    """Tests for TimeSeriesSummary dataclass."""

    def test_to_dict_excludes_none(self):
        summary = TimeSeriesSummary(mean=10.0, min=5.0, max=15.0, count=3)
        d = summary.to_dict()
        assert d["mean"] == 10.0
        assert d["min"] == 5.0
        assert d["max"] == 15.0
        assert d["count"] == 3
        assert "median" not in d
        assert "stddev" not in d


class TestRun:
    """Tests for Run dataclass."""

    def test_to_dict_minimal(self):
        run = Run(run_number=1)
        d = run.to_dict()
        assert d["run_number"] == 1
        assert d["status"] == "UNKNOWN"
        assert "start_time" not in d

    def test_to_dict_with_timeseries(self):
        run = Run(
            run_number=1,
            status="PASS",
            start_time="2026-03-17T10:00:00Z",
            end_time="2026-03-17T10:05:00Z",
            duration_seconds=300.0,
            timeseries={
                "sequence_0": TimeSeriesPoint(
                    timestamp="2026-03-17T10:00:00Z",
                    metrics={"value": 100.0},
                ),
                "sequence_1": TimeSeriesPoint(
                    timestamp="2026-03-17T10:01:00Z",
                    metrics={"value": 110.0},
                ),
            },
            timeseries_summary=TimeSeriesSummary(mean=105.0, count=2),
        )
        d = run.to_dict()
        assert d["status"] == "PASS"
        assert d["duration_seconds"] == 300.0
        assert "sequence_0" in d["timeseries"]
        assert d["timeseries"]["sequence_0"]["metrics"]["value"] == 100.0
        assert d["timeseries_summary"]["mean"] == 105.0


class TestResults:
    """Tests for Results dataclass."""

    def test_to_dict_with_runs(self):
        results = Results(
            status="PASS",
            total_runs=2,
            primary_metric=PrimaryMetric(
                name="iterations_per_sec", value=195000.0, unit="iter/s"
            ),
            runs={
                "run_1": Run(run_number=1, status="PASS"),
                "run_2": Run(run_number=2, status="PASS"),
            },
        )
        d = results.to_dict()
        assert d["status"] == "PASS"
        assert d["total_runs"] == 2
        assert d["primary_metric"]["value"] == 195000.0
        assert "run_1" in d["runs"]
        assert "run_2" in d["runs"]


class TestZathrasDocument:
    """Tests for the main ZathrasDocument dataclass."""

    @pytest.fixture
    def minimal_document(self):
        """Create a minimal valid document."""
        return ZathrasDocument(
            metadata=Metadata(document_id="test-doc-001"),
            test=TestInfo(name="coremark", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(status="PASS"),
        )

    @pytest.fixture
    def full_document(self):
        """Create a document with timeseries data."""
        return ZathrasDocument(
            metadata=Metadata(
                document_id="test-doc-002",
                test_timestamp="2026-03-17T10:00:00Z",
                os_vendor="rhel",
                cloud_provider="aws",
                instance_type="m5.large",
            ),
            test=TestInfo(name="coremark", version="1.0"),
            system_under_test=SystemUnderTest(
                hardware=HardwareInfo(
                    cpu=CPUInfo(vendor="Intel", cores=8),
                    memory=MemoryInfo(total_gb=32),
                ),
                operating_system=OperatingSystemInfo(
                    distribution="RHEL", version="9.2"
                ),
            ),
            test_configuration=TestConfiguration(iterations_requested=5),
            results=Results(
                status="PASS",
                total_runs=1,
                runs={
                    "run_1": Run(
                        run_number=1,
                        status="PASS",
                        start_time="2026-03-17T10:00:00Z",
                        end_time="2026-03-17T10:05:00Z",
                        metrics={"benchmark_name": "coremark"},
                        timeseries={
                            "sequence_0": TimeSeriesPoint(
                                timestamp="2026-03-17T10:00:00Z",
                                metrics={"value_seconds": 0.5},
                            ),
                            "sequence_1": TimeSeriesPoint(
                                timestamp="2026-03-17T10:01:00Z",
                                metrics={"value_seconds": 0.6},
                            ),
                        },
                    ),
                },
            ),
        )

    def test_to_dict(self, minimal_document):
        d = minimal_document.to_dict()
        assert "metadata" in d
        assert "test" in d
        assert "system_under_test" in d
        assert "test_configuration" in d
        assert "results" in d
        assert d["metadata"]["document_id"] == "test-doc-001"

    def test_to_json(self, minimal_document):
        j = minimal_document.to_json()
        parsed = json.loads(j)
        assert parsed["metadata"]["document_id"] == "test-doc-001"
        assert parsed["test"]["name"] == "coremark"

    def test_to_dict_summary_only_removes_timeseries(self, full_document):
        d = full_document.to_dict_summary_only()
        run_data = d["results"]["runs"]["run_1"]
        assert "timeseries" not in run_data

    def test_validate_valid_document(self, minimal_document):
        is_valid, errors = minimal_document.validate()
        assert is_valid
        assert errors == []

    def test_validate_missing_document_id(self):
        doc = ZathrasDocument(
            metadata=Metadata(document_id=""),
            test=TestInfo(name="test", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(status="PASS"),
        )
        is_valid, errors = doc.validate()
        assert not is_valid
        assert "metadata.document_id is required" in errors

    def test_validate_missing_test_name(self):
        doc = ZathrasDocument(
            metadata=Metadata(document_id="doc123"),
            test=TestInfo(name="", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(status="PASS"),
        )
        is_valid, errors = doc.validate()
        assert not is_valid
        assert "test.name is required" in errors

    def test_validate_invalid_run_key(self):
        doc = ZathrasDocument(
            metadata=Metadata(document_id="doc123"),
            test=TestInfo(name="test", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(
                status="PASS",
                runs={"invalid_key": Run(run_number=1)},
            ),
        )
        is_valid, errors = doc.validate()
        assert not is_valid
        assert any("Invalid run key" in e for e in errors)

    def test_validate_invalid_sequence_key(self):
        doc = ZathrasDocument(
            metadata=Metadata(document_id="doc123"),
            test=TestInfo(name="test", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(
                status="PASS",
                runs={
                    "run_1": Run(
                        run_number=1,
                        timeseries={
                            "bad_key": TimeSeriesPoint(
                                timestamp="2026-03-17T10:00:00Z",
                                metrics={},
                            )
                        },
                    )
                },
            ),
        )
        is_valid, errors = doc.validate()
        assert not is_valid
        assert any("Invalid sequence key" in e for e in errors)

    def test_calculate_content_hash_deterministic(self, minimal_document):
        hash1 = minimal_document.calculate_content_hash()
        hash2 = minimal_document.calculate_content_hash()
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex length

    def test_calculate_content_hash_excludes_timestamps(self):
        doc1 = ZathrasDocument(
            metadata=Metadata(
                document_id="doc123",
                test_timestamp="2026-03-17T10:00:00Z",
                processing_timestamp="2026-03-17T10:00:00Z",
            ),
            test=TestInfo(name="test", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(status="PASS"),
        )
        doc2 = ZathrasDocument(
            metadata=Metadata(
                document_id="doc123",
                test_timestamp="2026-03-18T12:00:00Z",
                processing_timestamp="2026-03-18T12:00:00Z",
            ),
            test=TestInfo(name="test", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(status="PASS"),
        )
        assert doc1.calculate_content_hash() == doc2.calculate_content_hash()

    def test_calculate_content_hash_different_for_different_content(self):
        doc1 = ZathrasDocument(
            metadata=Metadata(document_id="doc123"),
            test=TestInfo(name="test_a", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(status="PASS"),
        )
        doc2 = ZathrasDocument(
            metadata=Metadata(document_id="doc123"),
            test=TestInfo(name="test_b", version="1.0"),
            system_under_test=SystemUnderTest(),
            test_configuration=TestConfiguration(),
            results=Results(status="PASS"),
        )
        assert doc1.calculate_content_hash() != doc2.calculate_content_hash()

    def test_extract_timeseries_documents(self, full_document):
        ts_docs = full_document.extract_timeseries_documents()
        assert len(ts_docs) == 2

        ts_doc = ts_docs[0]
        assert ts_doc.metadata.document_id == "test-doc-002"
        assert ts_doc.metadata.sequence == 0
        assert ts_doc.test.name == "coremark"
        assert ts_doc.results.value == 0.5

    def test_extract_timeseries_documents_empty_when_no_timeseries(
        self, minimal_document
    ):
        minimal_document.results.runs = {
            "run_1": Run(run_number=1, status="PASS")
        }
        ts_docs = minimal_document.extract_timeseries_documents()
        assert ts_docs == []


class TestTimeSeriesDocument:
    """Tests for TimeSeriesDocument dataclass."""

    def test_to_dict(self):
        ts_doc = TimeSeriesDocument(
            metadata=TimeSeriesMetadata(
                document_id="parent-doc",
                timeseries_id="parent-doc_run_1_sequence_0",
                timestamp="2026-03-17T10:00:00Z",
                sequence=0,
            ),
            test=TestInfo(name="coremark", version="1.0"),
            system_under_test=SystemUnderTest(),
            results=TimeSeriesResults(
                run=TimeSeriesRun(
                    run_key="run_1", run_number=1, status="PASS"
                ),
                value=100.5,
                unit="iter/s",
            ),
        )
        d = ts_doc.to_dict()
        assert d["metadata"]["timeseries_id"] == "parent-doc_run_1_sequence_0"
        assert d["results"]["value"] == 100.5
        assert d["results"]["unit"] == "iter/s"

    def test_to_json(self):
        ts_doc = TimeSeriesDocument(
            metadata=TimeSeriesMetadata(
                document_id="parent-doc",
                timeseries_id="ts-001",
                timestamp="2026-03-17T10:00:00Z",
                sequence=0,
            ),
            test=TestInfo(name="test", version="1.0"),
            system_under_test=SystemUnderTest(),
            results=TimeSeriesResults(
                run=TimeSeriesRun(run_key="run_1", run_number=1, status="PASS"),
                value=50.0,
                unit="ms",
            ),
        )
        j = ts_doc.to_json()
        parsed = json.loads(j)
        assert parsed["metadata"]["sequence"] == 0
        assert parsed["results"]["value"] == 50.0


class TestUtilityFunctions:
    """Tests for schema utility functions."""

    def test_create_run_key(self):
        assert create_run_key(1) == "run_1"
        assert create_run_key(10) == "run_10"
        assert create_run_key(0) == "run_0"

    def test_create_sequence_key(self):
        assert create_sequence_key(0) == "sequence_0"
        assert create_sequence_key(5) == "sequence_5"
        assert create_sequence_key(100) == "sequence_100"

    def test_parse_timestamp_key(self):
        from datetime import datetime, timezone

        dt = parse_timestamp_key("2026-03-17T10:30:45Z")
        assert dt.year == 2026
        assert dt.month == 3
        assert dt.day == 17
        assert dt.hour == 10
        assert dt.minute == 30
        assert dt.second == 45
        assert dt.tzinfo == timezone.utc


class TestValidateJsonSchema:
    """Tests for validate_json_schema function."""

    def test_valid_document(self):
        doc = {
            "metadata": {"document_id": "test"},
            "test": {"name": "coremark", "version": "1.0"},
            "system_under_test": {},
            "test_configuration": {},
            "results": {"status": "PASS"},
        }
        is_valid, errors = validate_json_schema(doc)
        assert is_valid
        assert errors == []

    def test_missing_required_section(self):
        doc = {
            "metadata": {"document_id": "test"},
            "test": {"name": "coremark"},
            "results": {"status": "PASS"},
        }
        is_valid, errors = validate_json_schema(doc)
        assert not is_valid
        assert any("system_under_test" in e for e in errors)
        assert any("test_configuration" in e for e in errors)

    def test_runs_must_be_object(self):
        doc = {
            "metadata": {},
            "test": {},
            "system_under_test": {},
            "test_configuration": {},
            "results": {"runs": [{"run_number": 1}]},
        }
        is_valid, errors = validate_json_schema(doc)
        assert not is_valid
        assert any("must be an object" in e for e in errors)

    def test_invalid_run_key_in_json(self):
        doc = {
            "metadata": {},
            "test": {},
            "system_under_test": {},
            "test_configuration": {},
            "results": {"runs": {"bad_key": {}}},
        }
        is_valid, errors = validate_json_schema(doc)
        assert not is_valid
        assert any("Invalid run key" in e for e in errors)

    def test_timeseries_must_be_object(self):
        doc = {
            "metadata": {},
            "test": {},
            "system_under_test": {},
            "test_configuration": {},
            "results": {
                "runs": {
                    "run_1": {"timeseries": [{"timestamp": "2026-01-01T00:00:00Z"}]}
                }
            },
        }
        is_valid, errors = validate_json_schema(doc)
        assert not is_valid
        assert any("must be an object with timestamp keys" in e for e in errors)
