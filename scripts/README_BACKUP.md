# OpenSearch Backup and Restore Tool

A utility for backing up and restoring data from OpenSearch indices before schema migrations or other destructive operations.

## Features

- **Estimate before backup**: Shows document count and estimated download size before proceeding
- **User confirmation**: Requires explicit confirmation before backing up or restoring
- **Efficient transfer**: Uses scroll API for backup and bulk API for restore
- **Compression**: Optional gzip compression to reduce backup file sizes
- **Flexible**: Backup single indices or both zathras-results and zathras-timeseries together
- **Resume support**: NDJSON format allows for easy inspection and manual edits

## Prerequisites

1. Python 3.6 or later
2. PyYAML library: `pip install PyYAML`
3. A valid `config/export_config.yml` file with OpenSearch credentials

## Configuration

The tool reads connection settings from `config/export_config.yml`:

```yaml
opensearch:
  url: "https://opensearch.example.com"
  summary_index: "zathras-results"
  timeseries_index: "zathras-timeseries"
  username: "your-username"
  password: "your-password"
  verify_ssl: false
```

## Usage

### Backup Single Index

```bash
# Backup zathras-results only
./scripts/opensearch_backup.py backup --index zathras-results --output backups/

# Backup zathras-timeseries only
./scripts/opensearch_backup.py backup --index zathras-timeseries --output backups/
```

### Backup Both Indices

```bash
# Backup both indices (default)
./scripts/opensearch_backup.py backup --index both --output backups/

# Same as above (both is the default)
./scripts/opensearch_backup.py backup
```

### Backup Without Compression

```bash
# Disable gzip compression (backup will be larger but faster)
./scripts/opensearch_backup.py backup --no-compress
```

### List Available Backups

```bash
# List all backups in the default directory
./scripts/opensearch_backup.py list

# List backups in a specific directory
./scripts/opensearch_backup.py list --directory /path/to/backups/
```

### Restore from Backup

```bash
# Restore from a backup file (index name inferred from filename)
./scripts/opensearch_backup.py restore --input backups/zathras-results_20260612_143022.ndjson.gz

# Restore to a different index name
./scripts/opensearch_backup.py restore --input backups/zathras-results_20260612_143022.ndjson.gz --index new-index-name

# Restore without creating the index (requires index to already exist)
./scripts/opensearch_backup.py restore --input backups/zathras-results_20260612_143022.ndjson.gz --no-create
```

### Verbose Output

```bash
# Enable debug logging
./scripts/opensearch_backup.py backup -v
./scripts/opensearch_backup.py restore --input backups/zathras-results_20260612.ndjson.gz -v
```

## Example Workflow

### Before Schema Migration

1. **Estimate backup size**:
   ```bash
   ./scripts/opensearch_backup.py backup --index both --output backups/
   ```
   
   This will show:
   ```
   ======================================================================
   BACKUP ESTIMATION
   ======================================================================

   Index: zathras-results
     Documents: 12,345
     Index size: 2.3 GB

   Index: zathras-timeseries
     Documents: 456,789
     Index size: 15.7 GB

   ----------------------------------------------------------------------
   Total documents: 469,134
   Total index size: 18.0 GB
   Estimated backup size: 4.5 GB
     (with gzip compression)
   ======================================================================

   Proceed with backup? [y/N]:
   ```

2. **Confirm and backup**: Type `y` to proceed

3. **Wait for completion**: The tool will show progress every 10,000 documents

4. **Verify backup**: Check the `backups/` directory for the generated files

### After Failed Migration (Restore)

1. **List available backups**:
   ```bash
   ./scripts/opensearch_backup.py list --directory backups/
   ```

2. **Restore from backup**:
   ```bash
   ./scripts/opensearch_backup.py restore --input backups/zathras-results_20260612_143022.ndjson.gz
   ```

3. **Confirm restoration**: Type `y` when prompted

## Backup File Format

Backups are stored in **NDJSON** (newline-delimited JSON) format:

```json
{"_id": "doc1", "_source": {"field1": "value1", "field2": "value2"}}
{"_id": "doc2", "_source": {"field1": "value3", "field2": "value4"}}
```

This format is:
- **Human-readable**: Can be inspected with `less`, `head`, or text editors
- **Streamable**: Can be processed line-by-line without loading entire file into memory
- **Editable**: Can be modified with standard Unix tools (`grep`, `sed`, `jq`)
- **Resumable**: If restore fails partway through, you can resume from where it left off

## Performance Tuning

### Backup Performance

- `--batch-size`: Number of documents per scroll batch (default: 1000)
  - Increase for faster backups of small documents
  - Decrease if documents are very large

```bash
./scripts/opensearch_backup.py backup --batch-size 5000
```

### Restore Performance

- `--bulk-size`: Number of documents per bulk request (default: 500)
  - Increase for faster restores if OpenSearch can handle the load
  - Decrease if getting timeout errors

```bash
./scripts/opensearch_backup.py restore --input backup.ndjson.gz --bulk-size 1000
```

## Troubleshooting

### Connection Errors

If you get SSL certificate errors:

```yaml
# In config/export_config.yml
opensearch:
  verify_ssl: false
```

### Timeout Errors

For large indices, the default timeout (60 seconds) may be too short:

Edit the script and increase `timeout` in the `OpenSearchBackup` initialization.

### Out of Memory

If backing up very large indices:

1. The script uses streaming, so it shouldn't run out of memory
2. If it does, try reducing `--batch-size`

### Partial Backups

If a backup is interrupted:
- The partial file will be deleted automatically
- Simply re-run the backup command

### Failed Restores

If a restore fails partway through:
- Check the error message for details
- Some documents may already be restored
- Use `--no-create` flag to continue restoring to existing index
- Consider reducing `--bulk-size` if getting timeout errors

## File Naming Convention

Backup files are automatically named with:
- Index name
- Timestamp (YYYYMMDD_HHMMSS format)
- Extension (.ndjson or .ndjson.gz)

Example: `zathras-results_20260612_143022.ndjson.gz`

## Security Considerations

1. **Credentials**: The tool reads credentials from `config/export_config.yml`
   - Keep this file secure and never commit it to version control
   - Use `.gitignore` to exclude it

2. **Backup files**: May contain sensitive data
   - Store in a secure location
   - Consider encrypting backups for long-term storage
   - Set appropriate file permissions

3. **Network security**: 
   - Use HTTPS for OpenSearch connections when possible
   - Set `verify_ssl: true` in production environments

## Advanced Usage

### Backup to Custom Location

```bash
./scripts/opensearch_backup.py backup --output /mnt/external/opensearch-backups/
```

### Using a Different Config File

```bash
./scripts/opensearch_backup.py --config /path/to/custom_config.yml backup
```

### Manual NDJSON Inspection

```bash
# View first 10 documents
zcat backups/zathras-results_20260612.ndjson.gz | head -10 | jq

# Count documents in backup
zcat backups/zathras-results_20260612.ndjson.gz | wc -l

# Search for specific documents
zcat backups/zathras-results_20260612.ndjson.gz | grep "search-term" | jq
```

## Limitations

- Does not backup index mappings or settings (only document data)
- Does not preserve index aliases
- Single-threaded (no parallel scroll/restore)
- No incremental backup support

For production use cases requiring these features, consider using OpenSearch's built-in snapshot and restore functionality.
