# FIO Per-Job Data: OpenSearch vs Raw Archives

## Overview

FIO benchmark results contain both **aggregated metrics** (totals across all jobs/disks) and **per-job breakdown** (individual disk performance). Due to OpenSearch's 5,000 field limit, only aggregated data is exported to the `zathras-results` index. Per-job data remains available in the raw JSON archives.

## What's in OpenSearch (zathras-results)

Each FIO run in OpenSearch contains:

### Aggregated Metrics
- **Bandwidth**: total_bandwidth_kbps (min/max/mean)
- **IOPS**: total_iops (min/max/mean)
- **Latency**: avg_latency_mean_ns, avg_clat_mean_ns, avg_slat_mean_ns (with min/max/stddev)
- **Latency percentiles**: p1, p5, p10, p50, p90, p95, p99, p99.5, p99.9
- **I/O totals**: total_io_bytes, total_ios
- **CPU**: avg_cpu_usr_pct, avg_cpu_sys_pct
- **Metadata**: num_jobs, num_disks

### Timeseries Summary
- Statistical summary of timeseries data: count, mean, min, max, stddev

### Configuration
- All FIO test parameters and settings

**Use OpenSearch for**: Aggregate performance trends, run comparisons, dashboard visualizations

## What's in Raw JSON Archives

The full JSON documents (not exported to OpenSearch) additionally contain:

### Per-Job Details (`metrics.jobs` array)

For **each disk/job**:
- Job metadata: job_number, jobname, device path, elapsed_seconds
- Read metrics (if read test):
  - Bandwidth: kbps, min, max, mean, stddev, aggregate %
  - IOPS: value, min, max, mean, stddev
  - Latency: mean, min, max, stddev (regular + clat + slat)
  - **Latency percentiles**: p1, p5, p10, p50, p90, p95, p99, p99.5, p99.9
  - I/O: bytes, count, runtime
  - CPU: usr%, sys%
  - **I/O depth distribution**: % at depth 1, 2, 4, 8, 16, 32, 64+
  - **Latency distribution buckets**: microsecond and millisecond ranges
- Write metrics (if write test): same structure
- Mixed metrics (if mixed test): both read and write

### Full Timeseries Data
- Every timeseries point with timestamp and metrics
- Available in separate `zathras-timeseries` index (if enabled)

**Use raw JSON for**: Per-disk analysis, identifying slow disks, latency distribution analysis

## Accessing Per-Job Data

### Method 1: Direct File Read

Raw JSON documents are stored in the same location as the benchmark archives:

```python
import json
from pathlib import Path

# Load the full document
json_path = Path("/path/to/archive/fio-results.json")
with open(json_path) as f:
    doc = json.load(f)

# Access per-job data for a specific run
jobs = doc["results"]["runs"]["run_0"]["metrics"]["jobs"]

for job in jobs:
    print(f"Device: {job['device']}")
    print(f"  Bandwidth: {job['read']['bandwidth_kbps']} kbps")
    print(f"  IOPS: {job['read']['iops']}")
    print(f"  P99 Latency: {job['read']['latency_percentiles']['p99']} ns")
```

### Method 2: Programmatic Access (Python API)

```python
from chronicler.processors.fio_processor import FioProcessor

# Process with full detail
processor = FioProcessor("/path/to/benchmark/archive")
document = processor.process()

# Get full dict (includes per-job data)
full_dict = document.to_dict()

# Access per-job data
jobs = full_dict["results"]["runs"]["run_0"]["metrics"]["jobs"]
```

### Method 3: Query Pattern for Analysis

Example script to find slow disks across multiple test runs:

```python
import json
from pathlib import Path

def find_slow_disks(json_path, p99_threshold_ns=500_000):
    """Find disks with p99 latency above threshold."""
    with open(json_path) as f:
        doc = json.load(f)
    
    slow_disks = []
    for run_key, run_data in doc["results"]["runs"].items():
        if "metrics" not in run_data or "jobs" not in run_data["metrics"]:
            continue
        
        for job in run_data["metrics"]["jobs"]:
            device = job.get("device", "unknown")
            read_data = job.get("read", {})
            p99 = read_data.get("latency_percentiles", {}).get("p99")
            
            if p99 and p99 > p99_threshold_ns:
                slow_disks.append({
                    "run": run_key,
                    "device": device,
                    "p99_latency_ns": p99,
                    "bandwidth_kbps": read_data.get("bandwidth_kbps"),
                    "iops": read_data.get("iops"),
                })
    
    return slow_disks

# Usage
slow = find_slow_disks("fio-results.json", p99_threshold_ns=500_000)
for disk in slow:
    print(f"{disk['device']} in {disk['run']}: p99={disk['p99_latency_ns']}ns")
```

## Why Not Store Per-Job Data in OpenSearch?

### Design Decision

FIO was the only benchmark that stored per-instance breakdown in OpenSearch. Other benchmarks (CoreMark, Passmark, Uperf) follow an aggregated approach:
- **CoreMark**: Aggregate across threads, not per-thread
- **Passmark**: Aggregate across iterations, not per-iteration  
- **Uperf**: Aggregate across workers, not per-worker

To maintain consistency and stay within OpenSearch's 5,000 field limit, FIO now follows the same pattern.

### Field Count Impact

**With per-job data** (48 runs, 1 job each):
- Fields: ~6,632
- Status: ❌ Exceeds 5,000 limit

**Without per-job data**:
- Fields: ~3,176  
- Status: ✅ Under 5,000 limit (36% headroom)

### Future: Separate Per-Job Index

If per-job querying in OpenSearch becomes a frequent need, a separate `zathras-fio-job-timeseries` index could be implemented (similar to how general timeseries data is handled). See [GitHub issue #19](https://github.com/redhat-performance/chronicler/issues/19) for discussion.

## Summary

| Data Type | OpenSearch | Raw JSON |
|-----------|------------|----------|
| Aggregated metrics | ✅ | ✅ |
| Timeseries summary | ✅ | ✅ |
| Configuration | ✅ | ✅ |
| Per-job breakdown | ❌ | ✅ |
| Full timeseries | ❌ | ✅ |

**For most analysis**: Use OpenSearch (fast queries, dashboards)  
**For per-disk troubleshooting**: Use raw JSON archives (full granularity)
