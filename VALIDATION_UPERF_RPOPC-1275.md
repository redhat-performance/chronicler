# RPOPC-1275 Validation: Real Uperf Data

## Summary
Successfully validated multi-metric extraction with real uperf benchmark data.

## Test Data
- **Source**: `sample_data/rhel_9.8/rhel/aws/m6a.4xlarge_5/results_uperf.zip`
- **Instance**: AWS m6a.4xlarge
- **Date**: 2026-05-12

## Results

### Primary Metrics Extracted
✅ **Throughput**: 5.9856 Gb/s  
✅ **Latency**: 0.0001 microseconds  
✅ **Transaction Rate**: 1,163,772.5 trans/s

### Validation Details
- Status: PASS
- Total runs: 1
- Timeseries points: 18
- All three expected metrics present: ✓

### Expected vs Actual
```
Expected: ['latency', 'throughput', 'transaction_rate']
Got:      ['latency', 'throughput', 'transaction_rate']
Match:    100%
```

## Conclusion
The implementation correctly extracts all three coequal metrics from real uperf benchmark data, meeting all acceptance criteria for RPOPC-1275.
