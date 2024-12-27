from pyspark.sql import SparkSession
import json
import os
from datetime import datetime
import time
from dataclasses import dataclass
from typing import List, Dict



@dataclass
class TableInfo:
    database: str
    table_name: str
    columns: List[Dict[str, str]]
    partition_cols: List[Dict[str, str]]
    location: str
    properties: Dict[str, str]
    files: List[str]

def load_tables_info(info_file):
    """Load table information"""
    with open(info_file, 'r') as f:
        data = json.load(f)
        # 从每个字典中移除 snapshot_id 字段
        cleaned_data = []
        for item in data:
            if 'snapshot_id' in item:
                del item['snapshot_id']
            cleaned_data.append(item)
    return [TableInfo(**item) for item in cleaned_data]

def execute_migration(info_file, target_catalog, target_warehouse):
    """Execute migration"""
    print("Starting to create test tables...")
    target_spark = None
    
    try:
        target_spark = SparkSession.builder \
            .appName("Generate Large Small Parquet Files") \
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.0") \
            .config(f"spark.sql.catalog.{target_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{target_catalog}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
            .config(f"spark.sql.catalog.{target_catalog}.warehouse", target_warehouse) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .getOrCreate()

        # Load table information
        tables_info = load_tables_info(info_file)
        
        # Execute migration
        results = []
        start_time = time.time()
        
        for table_info in tables_info:
            result = migrate_table(target_spark, target_catalog, table_info)
            results.append(result)
        
        # Save migration report
        save_table_creation_report(results, start_time)
        
    finally:
        if target_spark:
            target_spark.stop()

def migrate_table(spark, target_catalog, table_info):
    """Create table structure (without data migration)"""
    print(f"Creating table structure: {table_info.database}.{table_info.table_name}")
    start_time = time.time()
    
    try:
        # Create target database
        create_db_sql = f"""
        CREATE NAMESPACE IF NOT EXISTS {target_catalog}.{table_info.database}
        """
        print(f"Executing SQL: {create_db_sql}")
        spark.sql(create_db_sql)
        
        # Build column definitions
        columns = [f"{col['name']} {col['type']}" for col in table_info.columns]
        columns_str = ",\n    ".join(columns)
        
        # Create table structure
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {target_catalog}.{table_info.database}.{table_info.table_name} (
            {columns_str}
        )
        USING iceberg
        """
        
        print(f"Executing SQL: {create_table_sql}")
        spark.sql(create_table_sql)
        
        duration = time.time() - start_time
        print(f"Table {table_info.database}.{table_info.table_name} structure created successfully, Time taken: {duration:.2f} seconds")
        
        return {
            'database': table_info.database,
            'table': table_info.table_name,
            'status': 'success',
            'time': duration,
            'files_count': len(table_info.files)
        }
        
    except Exception as e:
        print(f"Error creating table structure {table_info.database}.{table_info.table_name}: {str(e)}")
        return {
            'database': table_info.database,
            'table': table_info.table_name,
            'status': 'failed',
            'error': str(e)
        }

def save_table_creation_report(results, start_time):
    """Save table creation report"""
    # 创建嵌套的目录结构
    report_dir = os.path.join("migration_reports", "table_creation_reports")
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(report_dir, f"migration_report_{timestamp}.txt")
    
    with open(report_file, 'w') as f:
        f.write("Table Creation Report\n")
        f.write("=" * 50 + "\n")
        f.write(f"Start Time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Time: {time.time() - start_time:.2f} seconds\n")
        f.write("=" * 50 + "\n\n")
        
        # Group results by database
        db_results = {}
        for result in results:
            db = result['database']
            if db not in db_results:
                db_results[db] = []
            db_results[db].append(result)
        
        for db, db_results in db_results.items():
            f.write(f"\n📁 Database: {db}\n")
            f.write(f"{'Table Name':<30} {'Status':<10} {'Time(s)':<15} {'Files Count':<10}\n")
            f.write("-" * 70 + "\n")
            
            for result in db_results:
                status_icon = "✅" if result['status'] == 'success' else "❌"
                time_str = f"{result['time']:.2f}" if 'time' in result else "N/A"
                files_count = result.get('files_count', 'N/A')
                
                f.write(f"{result['table']:<30} {status_icon} {result['status']:<8} {time_str:<15} {files_count:<10}\n")
                if result['status'] == 'failed':
                    f.write(f"  Error: {result['error']}\n")
            
            success_count = len([r for r in db_results if r['status'] == 'success'])
            f.write(f"\nDatabase Statistics:\n")
            f.write(f"Total Tables: {len(db_results)}, Success: {success_count}, Failed: {len(db_results) - success_count}\n")
            f.write("=" * 70 + "\n")
    
    print(f"\nTable creation report saved to: {report_file}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Create table structures in target catalog')
    parser.add_argument('--info-file', required=True, help='Path to the table info JSON file')
    parser.add_argument('--target-catalog', required=True, help='Name of the target catalog')
    parser.add_argument('--target-warehouse', required=True, help='Warehouse URI/ARN for the target catalog')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.info_file):
        print(f"Error: Table info file not found: {args.info_file}")
        exit(1)
    
    execute_migration(args.info_file, args.target_catalog, args.target_warehouse)
