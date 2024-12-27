from pyspark.sql import SparkSession
import os
from dataclasses import dataclass
from typing import List, Dict
import json
import argparse
from datetime import datetime

@dataclass
class SnapshotInfo:
    database: str
    table_name: str
    snapshots: List[Dict]

def collect_source_snapshots(catalog_name, warehouse_uri, database):
    """Collect snapshot information from a single source database"""
    print(f"Starting to collect table snapshot information for database: {database}...")
    source_spark = None
    snapshots_info = []
    
    try:
        source_spark = SparkSession.builder \
            .appName("Iceberg Snapshots Collector") \
            .config("spark.jars.packages", 
                    "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,"
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "software.amazon.awssdk:bundle:2.17.178,"
                    "software.amazon.awssdk:url-connection-client:2.17.178,"
                    "software.amazon.awssdk:glue:2.17.178") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_uri) \
            .getOrCreate()

        tables = list_tables_in_database(source_spark, database, catalog_name)
        for table in tables:
            snapshot_info = collect_table_snapshots(source_spark, database, table, catalog_name)
            if snapshot_info:
                snapshots_info.append(snapshot_info)
                    
        save_snapshots_info(snapshots_info)
            
    finally:
        if source_spark:
            source_spark.stop()

def list_tables_in_database(spark, database, catalog_name):
    """List all tables in the database"""
    tables = []
    result = spark.sql(f"SHOW TABLES IN {catalog_name}.{database}").collect()
    for row in result:
        print(row)
        tables.append(row.tableName)
    return tables

def collect_table_snapshots(spark, database, table, catalog_name):
    """Collect snapshot information for a specific table"""
    try:
        snapshots_df = spark.read.format("iceberg") \
            .load(f"{catalog_name}.{database}.{table}.snapshots")
        
        # Convert snapshot information to a list of dictionaries
        snapshots = [{
            'snapshot_id': row.snapshot_id,
            'committed_at': row.committed_at.isoformat() if row.committed_at else None,
            'operation': row.operation,
            'summary': row.summary
        } for row in snapshots_df.orderBy("committed_at").collect()]

        return SnapshotInfo(
            database=database,
            table_name=table,
            snapshots=snapshots
        )
    except Exception as e:
        print(f"Error collecting snapshots for table {database}.{table}: {str(e)}")
        return None

def save_snapshots_info(snapshots_info):
    """Save snapshot information to file"""
    # 创建嵌套的目录结构
    info_dir = os.path.join("migration_info", "snapshot_info")
    os.makedirs(info_dir, exist_ok=True)
    
    # Use single database name and current timestamp for the filename
    if snapshots_info:
        database = snapshots_info[0].database
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(info_dir, f"snapshots_info_{database}_{timestamp}.json")
        
        with open(filename, 'w') as f:
            json.dump([vars(info) for info in snapshots_info], f, indent=2)
        
        print(f"Snapshot information saved to: {filename}")
    else:
        print("No snapshot information to save")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect snapshot information from Iceberg tables')
    parser.add_argument('--catalog-name', required=True,
                      help='Name of the Iceberg catalog')
    parser.add_argument('--warehouse-uri', required=True,
                      help='S3 URI for the warehouse location')
    parser.add_argument('--database', required=True,
                      help='Database to process')
    
    args = parser.parse_args()
    collect_source_snapshots(
        catalog_name=args.catalog_name,
        warehouse_uri=args.warehouse_uri,
        database=args.database
    )
