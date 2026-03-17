"""
Unit tests for chronicler.processors.timestamp_utils.

Tests ISO 8601 validation and timestamp interpolation.
"""

import pytest
from datetime import datetime, timezone

from chronicler.processors.timestamp_utils import (
    utc_now,
    utc_now_iso,
    ISO8601_PATTERN,
    validate_iso8601_timestamp,
    interpolate_timestamps,
)
from chronicler.processors.base_processor import ProcessorError


class TestUtcNow:
    """Tests for utc_now and utc_now_iso functions."""

    def test_utc_now_returns_timezone_aware_datetime(self):
        dt = utc_now()
        assert isinstance(dt, datetime)
        assert dt.tzinfo == timezone.utc

    def test_utc_now_iso_format(self):
        ts = utc_now_iso()
        assert ISO8601_PATTERN.match(ts)
        assert ts.endswith("Z")

    def test_utc_now_iso_parseable(self):
        ts = utc_now_iso()
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        assert dt.tzinfo == timezone.utc


class TestISO8601Pattern:
    """Tests for the ISO 8601 regex pattern."""

    @pytest.mark.parametrize(
        "valid_ts",
        [
            "2026-03-17T10:30:45Z",
            "2026-01-01T00:00:00Z",
            "2026-12-31T23:59:59Z",
            "2026-03-17T10:30:45.123456Z",
            "2026-03-17T10:30:45.1Z",
        ],
    )
    def test_valid_timestamps(self, valid_ts):
        assert ISO8601_PATTERN.match(valid_ts)

    @pytest.mark.parametrize(
        "invalid_ts",
        [
            "2026-03-17",
            "2026-03-17T10:30:45",
            "2026-03-17T10:30:45+00:00",
            "2026-03-17 10:30:45Z",
            "03-17-2026T10:30:45Z",
            "not-a-timestamp",
            "",
        ],
    )
    def test_invalid_timestamps(self, invalid_ts):
        assert not ISO8601_PATTERN.match(invalid_ts)


class TestValidateISO8601Timestamp:
    """Tests for validate_iso8601_timestamp function."""

    def test_valid_timestamp_returned(self):
        ts = "2026-03-17T10:30:45Z"
        result = validate_iso8601_timestamp(ts, "test context")
        assert result == ts

    def test_valid_timestamp_with_fractional_seconds(self):
        ts = "2026-03-17T10:30:45.123456Z"
        result = validate_iso8601_timestamp(ts, "test context")
        assert result == ts

    def test_strips_whitespace(self):
        ts = "  2026-03-17T10:30:45Z  "
        result = validate_iso8601_timestamp(ts, "test context")
        assert result == "2026-03-17T10:30:45Z"

    def test_raises_on_none(self):
        with pytest.raises(ProcessorError) as exc_info:
            validate_iso8601_timestamp(None, "Row 1:")
        assert "require timestamps" in str(exc_info.value)
        assert "Row 1:" in str(exc_info.value)

    def test_raises_on_empty_string(self):
        with pytest.raises(ProcessorError) as exc_info:
            validate_iso8601_timestamp("", "Row 1:")
        assert "require timestamps" in str(exc_info.value)

    def test_raises_on_blank_string(self):
        with pytest.raises(ProcessorError) as exc_info:
            validate_iso8601_timestamp("   ", "Row 1:")
        assert "cannot be blank" in str(exc_info.value)

    def test_raises_on_invalid_format(self):
        with pytest.raises(ProcessorError) as exc_info:
            validate_iso8601_timestamp("2026-03-17", "Row 1:")
        assert "valid ISO 8601" in str(exc_info.value)
        assert "Expected format" in str(exc_info.value)

    def test_raises_on_invalid_date(self):
        with pytest.raises(ProcessorError) as exc_info:
            validate_iso8601_timestamp("2026-02-30T10:30:45Z", "Row 1:")
        assert "Cannot parse" in str(exc_info.value)

    def test_custom_test_name_in_error(self):
        with pytest.raises(ProcessorError) as exc_info:
            validate_iso8601_timestamp("bad", "Row 1:", test_name="Uperf")
        assert "Uperf" in str(exc_info.value)

    def test_raises_on_non_string(self):
        with pytest.raises(ProcessorError) as exc_info:
            validate_iso8601_timestamp(12345, "Row 1:")
        assert "require timestamps" in str(exc_info.value)


class TestInterpolateTimestamps:
    """Tests for interpolate_timestamps function."""

    def test_single_point_returns_start(self):
        result = interpolate_timestamps(
            "2026-03-17T10:00:00Z",
            "2026-03-17T10:10:00Z",
            num_points=1,
        )
        assert result == ["2026-03-17T10:00:00Z"]

    def test_zero_points_returns_empty(self):
        result = interpolate_timestamps(
            "2026-03-17T10:00:00Z",
            "2026-03-17T10:10:00Z",
            num_points=0,
        )
        assert result == []

    def test_two_points_returns_start_and_end(self):
        result = interpolate_timestamps(
            "2026-03-17T10:00:00Z",
            "2026-03-17T10:10:00Z",
            num_points=2,
        )
        assert len(result) == 2
        assert result[0] == "2026-03-17T10:00:00Z"
        assert result[1] == "2026-03-17T10:10:00Z"

    def test_three_points_evenly_spaced(self):
        result = interpolate_timestamps(
            "2026-03-17T10:00:00Z",
            "2026-03-17T10:10:00Z",
            num_points=3,
        )
        assert len(result) == 3
        assert result[0] == "2026-03-17T10:00:00Z"
        assert result[2] == "2026-03-17T10:10:00Z"
        dt_mid = datetime.fromisoformat(result[1].replace("Z", "+00:00"))
        assert dt_mid.minute == 5

    def test_many_points_evenly_distributed(self):
        result = interpolate_timestamps(
            "2026-03-17T10:00:00Z",
            "2026-03-17T11:00:00Z",
            num_points=7,
        )
        assert len(result) == 7
        assert result[0] == "2026-03-17T10:00:00Z"
        assert result[-1] == "2026-03-17T11:00:00Z"

        dts = [datetime.fromisoformat(ts.replace("Z", "+00:00")) for ts in result]
        intervals = [(dts[i + 1] - dts[i]).total_seconds() for i in range(len(dts) - 1)]
        assert all(abs(i - intervals[0]) < 0.001 for i in intervals)

    def test_fractional_seconds_trimmed(self):
        result = interpolate_timestamps(
            "2026-03-17T10:00:00Z",
            "2026-03-17T10:00:01Z",
            num_points=3,
        )
        assert len(result) == 3
        assert result[1] == "2026-03-17T10:00:00.5Z"

    def test_handles_fractional_input_timestamps(self):
        result = interpolate_timestamps(
            "2026-03-17T10:00:00.500000Z",
            "2026-03-17T10:00:01.500000Z",
            num_points=2,
        )
        assert len(result) == 2
