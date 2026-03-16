"""
Shared timestamp validation and interpolation for processors.

Centralizes ISO 8601 validation and timestamp interpolation used by
multiple benchmark processors to avoid duplication.
"""

import re
from datetime import datetime, timedelta
from typing import List, Callable, Optional, Dict, Any

from .base_processor import ProcessorError

# ISO 8601 pattern (e.g. 2026-02-10T14:41:49Z or with fractional seconds)
ISO8601_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$"
)


def validate_iso8601_timestamp(
    value: str,
    context: str,
    test_name: str = "Results",
) -> str:
    """
    Validate and return an ISO 8601 timestamp string.

    Raises ProcessorError if value is missing, blank, or not valid ISO 8601.
    Error messages use test_name (e.g. "Uperf", "STREAMS") for clarity.

    Args:
        value: Raw timestamp string to validate.
        context: Short context for error messages (e.g. "Row 1:", "run_metadata.csv").
        test_name: Benchmark name for error messages (e.g. "Uperf", "CoreMark Pro").

    Returns:
        Validated timestamp string (stripped).
    """
    if not value or not isinstance(value, str):
        raise ProcessorError(
            f"{test_name} results require timestamps. {context} "
            "Start_Date and End_Date must be non-empty strings."
        )
    value = value.strip()
    if not value:
        raise ProcessorError(
            f"{test_name} results require timestamps. {context} "
            "Start_Date and End_Date cannot be blank."
        )
    if not ISO8601_PATTERN.match(value):
        raise ProcessorError(
            f"{test_name} results require valid ISO 8601 timestamps. {context} "
            f"Got: {value!r}. Expected format: YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DDTHH:MM:SS.ffffffZ"
        )
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as e:
        raise ProcessorError(
            f"{test_name} results require valid ISO 8601 timestamps. {context} "
            f"Cannot parse {value!r}: {e}"
        ) from e
    return value


def interpolate_timestamps(
    start_ts: str,
    end_ts: str,
    num_points: int,
) -> List[str]:
    """
    Interpolate num_points ISO 8601 timestamps between start_ts and end_ts.

    Used when only run-level start/end timestamps exist and per-point
    timestamps must be derived (e.g. STREAMS, Uperf net_results).

    Args:
        start_ts: ISO 8601 start timestamp (e.g. "2026-02-10T14:40:00Z").
        end_ts: ISO 8601 end timestamp.
        num_points: Number of timestamps to generate.

    Returns:
        List of ISO 8601 timestamp strings, evenly spaced from start to end.
        If num_points <= 1, returns [start_ts].
    """
    if num_points <= 1:
        return [start_ts] if num_points == 1 else []

    start_dt = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_ts.replace("Z", "+00:00"))
    total_seconds = (end_dt - start_dt).total_seconds()
    result = []

    for i in range(num_points):
        frac = i / (num_points - 1)
        delta_sec = total_seconds * frac
        point_dt = start_dt + timedelta(seconds=delta_sec)
        ts_str = (
            point_dt.strftime("%Y-%m-%dT%H:%M:%S")
            + (f".{point_dt.microsecond:06d}".rstrip("0").rstrip(".") or "")
            + "Z"
        )
        result.append(ts_str)

    return result
