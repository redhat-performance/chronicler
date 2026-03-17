"""
Unit tests for chronicler.processors.run_utils.

Tests helpers for building Run timeseries and TimeSeriesSummary.
"""

import pytest

from chronicler.processors.run_utils import (
    run_data_timeseries_to_objects,
    timeseries_summary_from_metric,
)
from chronicler.processors.base_processor import ProcessorError
from chronicler.schema import TimeSeriesPoint, TimeSeriesSummary


class TestRunDataTimeseriesToObjects:
    """Tests for run_data_timeseries_to_objects function."""

    def test_converts_dict_to_timeseriespoint_objects(self):
        run_data_ts = {
            "sequence_0": {
                "timestamp": "2026-03-17T10:00:00Z",
                "metrics": {"value": 100.0},
            },
            "sequence_1": {
                "timestamp": "2026-03-17T10:01:00Z",
                "metrics": {"value": 110.0},
            },
        }
        result = run_data_timeseries_to_objects(run_data_ts)

        assert len(result) == 2
        assert isinstance(result["sequence_0"], TimeSeriesPoint)
        assert result["sequence_0"].timestamp == "2026-03-17T10:00:00Z"
        assert result["sequence_0"].metrics["value"] == 100.0
        assert result["sequence_1"].timestamp == "2026-03-17T10:01:00Z"

    def test_raises_on_missing_timestamp(self):
        run_data_ts = {
            "sequence_0": {
                "metrics": {"value": 100.0},
            },
        }
        with pytest.raises(ProcessorError) as exc_info:
            run_data_timeseries_to_objects(run_data_ts)
        assert "missing a timestamp" in str(exc_info.value)
        assert "sequence_0" in str(exc_info.value)

    def test_raises_on_empty_timestamp(self):
        run_data_ts = {
            "sequence_0": {
                "timestamp": "",
                "metrics": {"value": 100.0},
            },
        }
        with pytest.raises(ProcessorError) as exc_info:
            run_data_timeseries_to_objects(run_data_ts)
        assert "missing a timestamp" in str(exc_info.value)

    def test_handles_empty_metrics(self):
        run_data_ts = {
            "sequence_0": {
                "timestamp": "2026-03-17T10:00:00Z",
                "metrics": {},
            },
        }
        result = run_data_timeseries_to_objects(run_data_ts)
        assert result["sequence_0"].metrics == {}

    def test_handles_missing_metrics_key(self):
        run_data_ts = {
            "sequence_0": {
                "timestamp": "2026-03-17T10:00:00Z",
            },
        }
        result = run_data_timeseries_to_objects(run_data_ts)
        assert result["sequence_0"].metrics == {}

    def test_with_validation_callback(self):
        run_data_ts = {
            "sequence_0": {
                "timestamp": "2026-03-17T10:00:00Z",
                "metrics": {"value": 100.0},
            },
        }
        validated_timestamps = []

        def validator(value, context):
            validated_timestamps.append((value, context))
            return value.upper() if isinstance(value, str) else value

        result = run_data_timeseries_to_objects(
            run_data_ts,
            validate_timestamp=validator,
            run_context="Run 1",
        )

        assert len(validated_timestamps) == 1
        assert validated_timestamps[0][0] == "2026-03-17T10:00:00Z"
        assert "Run 1" in validated_timestamps[0][1]

    def test_validation_callback_raises_error(self):
        run_data_ts = {
            "sequence_0": {
                "timestamp": "invalid",
                "metrics": {},
            },
        }

        def validator(value, context):
            raise ProcessorError(f"Invalid timestamp: {value}")

        with pytest.raises(ProcessorError) as exc_info:
            run_data_timeseries_to_objects(
                run_data_ts,
                validate_timestamp=validator,
            )
        assert "Invalid timestamp" in str(exc_info.value)

    def test_custom_run_context_in_error(self):
        run_data_ts = {
            "sequence_5": {
                "timestamp": None,
                "metrics": {},
            },
        }
        with pytest.raises(ProcessorError) as exc_info:
            run_data_timeseries_to_objects(
                run_data_ts,
                run_context="Run 3",
            )
        assert "Run 3" in str(exc_info.value)
        assert "sequence_5" in str(exc_info.value)


class TestTimeseriesSummaryFromMetric:
    """Tests for timeseries_summary_from_metric function."""

    def test_computes_summary_statistics(self):
        timeseries = {
            "sequence_0": TimeSeriesPoint(
                timestamp="2026-03-17T10:00:00Z",
                metrics={"throughput": 100.0},
            ),
            "sequence_1": TimeSeriesPoint(
                timestamp="2026-03-17T10:01:00Z",
                metrics={"throughput": 200.0},
            ),
            "sequence_2": TimeSeriesPoint(
                timestamp="2026-03-17T10:02:00Z",
                metrics={"throughput": 150.0},
            ),
        }
        summary = timeseries_summary_from_metric(timeseries, "throughput")

        assert summary is not None
        assert summary.count == 3
        assert summary.mean == 150.0
        assert summary.median == 150.0
        assert summary.min == 100.0
        assert summary.max == 200.0
        assert summary.stddev > 0

    def test_returns_none_for_missing_metric(self):
        timeseries = {
            "sequence_0": TimeSeriesPoint(
                timestamp="2026-03-17T10:00:00Z",
                metrics={"other_metric": 100.0},
            ),
        }
        summary = timeseries_summary_from_metric(timeseries, "throughput")
        assert summary is None

    def test_returns_none_for_empty_timeseries(self):
        summary = timeseries_summary_from_metric({}, "throughput")
        assert summary is None

    def test_skips_non_numeric_values(self):
        timeseries = {
            "sequence_0": TimeSeriesPoint(
                timestamp="2026-03-17T10:00:00Z",
                metrics={"value": 100.0},
            ),
            "sequence_1": TimeSeriesPoint(
                timestamp="2026-03-17T10:01:00Z",
                metrics={"value": "not a number"},
            ),
            "sequence_2": TimeSeriesPoint(
                timestamp="2026-03-17T10:02:00Z",
                metrics={"value": 200.0},
            ),
        }
        summary = timeseries_summary_from_metric(timeseries, "value")

        assert summary is not None
        assert summary.count == 2
        assert summary.mean == 150.0

    def test_skips_none_values(self):
        timeseries = {
            "sequence_0": TimeSeriesPoint(
                timestamp="2026-03-17T10:00:00Z",
                metrics={"value": 100.0},
            ),
            "sequence_1": TimeSeriesPoint(
                timestamp="2026-03-17T10:01:00Z",
                metrics={"value": None},
            ),
        }
        summary = timeseries_summary_from_metric(timeseries, "value")

        assert summary is not None
        assert summary.count == 1
        assert summary.mean == 100.0

    def test_handles_integer_values(self):
        timeseries = {
            "sequence_0": TimeSeriesPoint(
                timestamp="2026-03-17T10:00:00Z",
                metrics={"count": 10},
            ),
            "sequence_1": TimeSeriesPoint(
                timestamp="2026-03-17T10:01:00Z",
                metrics={"count": 20},
            ),
        }
        summary = timeseries_summary_from_metric(timeseries, "count")

        assert summary is not None
        assert summary.count == 2
        assert summary.mean == 15.0

    def test_single_value_has_zero_stddev(self):
        timeseries = {
            "sequence_0": TimeSeriesPoint(
                timestamp="2026-03-17T10:00:00Z",
                metrics={"value": 100.0},
            ),
        }
        summary = timeseries_summary_from_metric(timeseries, "value")

        assert summary is not None
        assert summary.count == 1
        assert summary.stddev == 0.0
