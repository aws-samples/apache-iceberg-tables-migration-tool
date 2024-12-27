## Apache Iceberg Tables Migration Tool

A tool for migrating Apache Iceberg tables from S3 bucket to target S3 Tables bucket. Supports complete migration of table structures, data, and snapshots.

## Features

- Complete migration of Apache Iceberg tables
- Preservation of table snapshot history
- Support for point-in-time table state migration
- Automatic collection and validation of table schemas
- Support for large-scale data migration
- Error handling and logging

## Prerequisites

- Python 3.7+
- Apache Spark 3.4+
- AWS access configuration
- Required Python packages:
  - pyspark
  - boto3
  - pandas

## Usage Steps

### 1. Collect Snapshot Information

Collect all snapshot information from source tables:

python 1_collect_snapshots_info.py \
--catalog-name <source-catalog-name> \
--warehouse-uri <source-s3-uri> \
--database <database-name>


### 2. Collect Database Schema Information

Collect table structure information at a specific timestamp:

python 2_collect_database_schema_info.py \
--catalog-name <source-catalog-name> \
--warehouse-uri <source-s3-uri> \
--database <database-name> \
--snapshot-info-file <path-to-snapshot-info.json> \
--timestamp "<timestamp-in-iso-format>"


### 3. Create Target Tables

Create table structures at the target location:

python 3_create_target_tables.py \
--catalog-name <target-catalog-name> \
--warehouse-uri <target-s3-uri> \
--database <database-name> \
--schema-info-file <path-to-schema-info.json>


### 4. Verify Table Schema

Validate schema consistency between source and target tables:

python 4_verify_table_schema.py \
--source-catalog <source-catalog-name> \
--target-catalog <target-catalog-name> \
--database <database-name>


### 5. Migrate Table Data

Execute data migration for a specific point in time:

python 5_migrate_tables_data.py \
--source-catalog <source-catalog-name> \
--target-catalog <target-catalog-name> \
--database <database-name> \
--snapshot-info-file <path-to-snapshot-info.json>


## Configuration Parameters

- `catalog-name`: Name of the Iceberg catalog
- `warehouse-uri`: S3 warehouse URI
- `database`: Name of the database to migrate
- `snapshot-info-file`: Path to snapshot information file
- `schema-info-file`: Path to schema information file
- `timestamp`: Migration timestamp (ISO format)

## Important Notes

1. This tool performs point-in-time migration rather than incremental synchronization
2. Ensure sufficient S3 storage space
3. Test migration process in a non-production environment first
4. Configure appropriate Spark settings for large table migration
5. Maintain stable network connection during migration
6. Backup important data before migration

## Migration Process Flow

1. **Snapshot Collection**
   - Collects all snapshot information from source tables
   - Records snapshot timestamps and IDs

2. **Schema Collection**
   - Gathers table structures and metadata
   - Records partition information and column details

3. **Target Creation**
   - Creates tables in target location
   - Maintains original schema and properties

4. **Schema Verification**
   - Validates schema consistency
   - Ensures proper table creation

5. **Data Migration**
   - Transfers data using specified snapshots
   - Maintains data integrity and history

## Limitations

1. No support for incremental synchronization
2. Migrates table state at a specific point in time
3. Requires re-running the entire process for updating to a newer state

## Troubleshooting

Common issues and solutions:
- Network connectivity issues: Check AWS credentials and network settings
- Memory errors: Adjust Spark configuration parameters
- Schema mismatch: Verify source and target table structures
- Migration interruption: Restart the migration process

## Contributing

Contributions are welcome! Please feel free to submit issues and pull requests.

## Support

For support and questions, please create an issue in the GitHub repository.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

