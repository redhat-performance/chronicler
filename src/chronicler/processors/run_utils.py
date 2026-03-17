"""
Shared helpers for building Run timeseries and TimeSeriesSummary from run data.

Used by processors that have a run_data dict with a "timeseries" section
(timestamp + metrics per sequence) and optionally need a summary statistic.
"""

import statistics
from typing import Dict, Any, Callable, Optional

from ..schema import TimeSeriesPoint, TimeSeriesSummary


def run_data_timeseries_to_objects(
    run_data_timeseries: Dict[str, Any],
    validate_timestamp: Optional[Callable[[str, str], str]] = None,
    run_context: str = "run",
) -> Dict[str, TimeSeriesPoint]:
    """
    Convert run_data["timeseries"] dict to Dict[str, TimeSeriesPoint].

    Each entry in run_data_timeseries should have "timestamp" and "metrics".
    If validate_timestamp is provided, it is called for each timestamp and should
    raise ProcessorError if invalid and return the validated string.

    Args:
        run_data_timeseries: Dict mapping sequence key to {"timestamp": str, "metrics": dict}.
        validate_timestamp: Optional callable (value, context) -> validated str; raises on error.
        run_context: Prefix for context in error messages (e.g. "Run 0", "run_0").

    Returns:
        Dict mapping sequence key to TimeSeriesPoint.
    """
    from .base_processor import ProcessorError

    result = {}
    for seq_key, ts_data in run_data_timeseries.items():
        ts = ts_data.get("timestamp")
        if not ts:
            raise ProcessorError(
                f"{run_context} timeseries point {seq_key} is missing a timestamp. "
                "Timestamps must come from the result file or run-level metadata."
            )
        if validate_timestamp:
            ts = validate_timestamp(ts, f"{run_context}, {seq_key}:")
        result[seq_key] = TimeSeriesPoint(
            timestamp=ts,
            metrics=ts_data.get("metrics", {}),
        )
    return result


def timeseries_summary_from_metric(
    timeseries: Dict[str, TimeSeriesPoint],
    metric_name: str,
) -> Optional[TimeSeriesSummary]:
    """
    Build TimeSeriesSummary from numeric values of one metric across timeseries points.

    Args:
        timeseries: Dict of sequence key -> TimeSeriesPoint.
        metric_name: Key to read from each point's metrics (e.g. "throughput_gbps", "SUMM_CPU").

    Returns:
        TimeSeriesSummary with mean, median, min, max, stddev, count; or None if no values.
    """
    values = []
    for ts_point in timeseries.values():
        v = ts_point.metrics.get(metric_name)
        if v is not None and isinstance(v, (int, float)):
            values.append(float(v))

    if not values:
        return None

    return TimeSeriesSummary(
        mean=statistics.mean(values),
        median=statistics.median(values),
        min=min(values),
        max=max(values),
        stddev=statistics.stdev(values) if len(values) > 1 else 0.0,
        count=len(values),
    )
