# OpenSearch Backup Script - Example Output

## Example: Backing Up Both Indices

```bash
$ ./scripts/opensearch_backup.py backup --index both --output backups/

======================================================================
BACKUP ESTIMATION
======================================================================

Index: zathras-results
  Documents: 12,345
  Index size: 2.34 GB

Index: zathras-timeseries
  Documents: 456,789
  Index size: 15.67 GB

----------------------------------------------------------------------
Total documents: 469,134
Total index size: 18.01 GB
Estimated backup size: 4.50 GB
  (with gzip compression)
======================================================================

Proceed with backup? [y/N]: y

INFO: Starting backup of index 'zathras-results'...
INFO:   Backed up 10,000 / 12,345 documents...
INFO: ✓ Backup complete: 12,345 documents written to backups/zathras-results_20260612_143022.ndjson.gz
INFO:   Backup file size: 587.23 MB

INFO: Starting backup of index 'zathras-timeseries'...
INFO:   Backed up 10,000 / 456,789 documents...
INFO:   Backed up 20,000 / 456,789 documents...
...
INFO:   Backed up 450,000 / 456,789 documents...
INFO: ✓ Backup complete: 456,789 documents written to backups/zathras-timeseries_20260612_143022.ndjson.gz
INFO:   Backup file size: 3.92 GB

======================================================================
BACKUP SUMMARY
======================================================================

Index: zathras-results
  Documents: 12,345
  File: backups/zathras-results_20260612_143022.ndjson.gz
  Size: 587.23 MB

Index: zathras-timeseries
  Documents: 456,789
  File: backups/zathras-timeseries_20260612_143022.ndjson.gz
  Size: 3.92 GB
======================================================================
```

## Example: Listing Backups

```bash
$ ./scripts/opensearch_backup.py list --directory backups/

======================================================================
AVAILABLE BACKUPS
======================================================================

zathras-results_20260612_143022.ndjson.gz
  Size: 587.23 MB
  Path: backups/zathras-results_20260612_143022.ndjson.gz

zathras-timeseries_20260612_143022.ndjson.gz
  Size: 3.92 GB
  Path: backups/zathras-timeseries_20260612_143022.ndjson.gz

zathras-results_20260611_091500.ndjson.gz
  Size: 585.10 MB
  Path: backups/zathras-results_20260611_091500.ndjson.gz

======================================================================
```

## Example: Restoring from Backup

```bash
$ ./scripts/opensearch_backup.py restore --input backups/zathras-results_20260612_143022.ndjson.gz

======================================================================
RESTORE CONFIRMATION
======================================================================

Source file: backups/zathras-results_20260612_143022.ndjson.gz
Target index: zathras-results
File size: 587.23 MB

======================================================================

Proceed with restore? [y/N]: y

INFO: Starting restore to index 'zathras-results' from backups/zathras-results_20260612_143022.ndjson.gz...
INFO: Index 'zathras-results' exists
INFO:   Restored 10,000 documents...
INFO: ✓ Restore complete: 12,345 documents restored

======================================================================
RESTORE SUMMARY
======================================================================

Index: zathras-results
  Documents restored: 12,345
======================================================================
```

## Example: User Cancels Backup

```bash
$ ./scripts/opensearch_backup.py backup

======================================================================
BACKUP ESTIMATION
======================================================================

Index: zathras-results
  Documents: 12,345
  Index size: 2.34 GB

Index: zathras-timeseries
  Documents: 456,789
  Index size: 15.67 GB

----------------------------------------------------------------------
Total documents: 469,134
Total index size: 18.01 GB
Estimated backup size: 4.50 GB
  (with gzip compression)
======================================================================

Proceed with backup? [y/N]: n
Backup cancelled.
```

## Example: Backup Without Compression

```bash
$ ./scripts/opensearch_backup.py backup --index zathras-results --no-compress

======================================================================
BACKUP ESTIMATION
======================================================================

Index: zathras-results
  Documents: 12,345
  Index size: 2.34 GB

----------------------------------------------------------------------
Total documents: 12,345
Total index size: 2.34 GB
Estimated backup size: 2.34 GB
======================================================================

Proceed with backup? [y/N]: y

INFO: Starting backup of index 'zathras-results'...
INFO:   Backed up 10,000 / 12,345 documents...
INFO: ✓ Backup complete: 12,345 documents written to backups/zathras-results_20260612_150000.ndjson
INFO:   Backup file size: 2.28 GB

======================================================================
BACKUP SUMMARY
======================================================================

Index: zathras-results
  Documents: 12,345
  File: backups/zathras-results_20260612_150000.ndjson
  Size: 2.28 GB
======================================================================
```

## Example: Verbose Mode

```bash
$ ./scripts/opensearch_backup.py backup --index zathras-results -v

======================================================================
BACKUP ESTIMATION
======================================================================

Index: zathras-results
  Documents: 12,345
  Index size: 2.34 GB

----------------------------------------------------------------------
Total documents: 12,345
Total index size: 2.34 GB
Estimated backup size: 587.23 MB
  (with gzip compression)
======================================================================

Proceed with backup? [y/N]: y

DEBUG: Request to /zathras-results/_search?scroll=5m succeeded
INFO: Starting backup of index 'zathras-results'...
DEBUG: Request to /_search/scroll succeeded
DEBUG: Request to /_search/scroll succeeded
INFO:   Backed up 10,000 / 12,345 documents...
DEBUG: Request to /_search/scroll succeeded
DEBUG: Request to /_search/scroll succeeded
INFO: ✓ Backup complete: 12,345 documents written to backups/zathras-results_20260612_151000.ndjson.gz
INFO:   Backup file size: 587.23 MB
...
```

## Example: Error - Config File Not Found

```bash
$ ./scripts/opensearch_backup.py backup

ERROR: Failed to load config from config/export_config.yml: [Errno 2] No such file or directory: 'config/export_config.yml'
```

**Solution**: Copy the example config and fill in your credentials:

```bash
cp config/export_config_example.yml config/export_config.yml
# Edit config/export_config.yml with your OpenSearch credentials
```

## Example: Error - Connection Failed

```bash
$ ./scripts/opensearch_backup.py backup

ERROR: Failed to get stats for 'zathras-results': Connection error: [Errno -2] Name or service not known
```

**Solution**: Check that:
1. The OpenSearch URL in `config/export_config.yml` is correct
2. The OpenSearch server is accessible from your network
3. Credentials are correct

## Example: Restore to Different Index

```bash
$ ./scripts/opensearch_backup.py restore \
    --input backups/zathras-results_20260612_143022.ndjson.gz \
    --index zathras-results-restored

======================================================================
RESTORE CONFIRMATION
======================================================================

Source file: backups/zathras-results_20260612_143022.ndjson.gz
Target index: zathras-results-restored
File size: 587.23 MB

======================================================================

Proceed with restore? [y/N]: y

INFO: Starting restore to index 'zathras-results-restored' from backups/zathras-results_20260612_143022.ndjson.gz...
INFO: Creating index 'zathras-results-restored'...
INFO:   Restored 10,000 documents...
INFO: ✓ Restore complete: 12,345 documents restored

======================================================================
RESTORE SUMMARY
======================================================================

Index: zathras-results-restored
  Documents restored: 12,345
======================================================================
```
