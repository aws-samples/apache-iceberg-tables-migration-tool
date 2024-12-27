from pyspark.sql import SparkSession
import json
import os
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime

@dataclass
class ValidationResult:
    database: str
    table_name: str
    columns_match: bool
    partition_cols_match: bool
    missing_columns: List[str]
    extra_columns: List[str]
    missing_partition_cols: List[str]
    extra_partition_cols: List[str]
    error: Optional[str] = None

def get_spark_session(target_catalog: str, target_warehouse: str) -> SparkSession:
    """Create Spark session"""
    return SparkSession.builder \
        .appName("Verify Table Creation") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.0") \
        .config(f"spark.sql.catalog.{target_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{target_catalog}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        .config(f"spark.sql.catalog.{target_catalog}.warehouse", target_warehouse) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def load_source_tables_info(info_file: str) -> List[dict]:
    """Load source table information"""
    with open(info_file, 'r') as f:
        data = json.load(f)
    return data

def get_target_table_info(spark: SparkSession, catalog: str, database: str, table: str) -> dict:
    """Get target table information"""
    try:
        # Get table structure
        df = spark.sql(f"DESCRIBE TABLE {catalog}.{database}.{table}")
        columns_info = df.collect()
        
        # Parse column information
        columns = []
        partition_cols = []
        
        for row in columns_info:
            col_name = row['col_name']
            col_type = row['data_type']
            
            # Skip additional table information
            if col_name.startswith('#') or not col_type:
                continue
                
            # Check if it's a partition column
            if 'Partition Keys' in str(row):
                partition_cols.append({"name": col_name, "type": col_type})
            else:
                columns.append({"name": col_name, "type": col_type})
        
        return {
            "database": database,
            "table_name": table,
            "columns": columns,
            "partition_cols": partition_cols
        }
    except Exception as e:
        return {"error": str(e)}

def compare_tables(source_info: dict, target_info: dict) -> ValidationResult:
    """Compare source and target table information"""
    if "error" in target_info:
        return ValidationResult(
            database=source_info["database"],
            table_name=source_info["table_name"],
            columns_match=False,
            partition_cols_match=False,
            missing_columns=[],
            extra_columns=[],
            missing_partition_cols=[],
            extra_partition_cols=[],
            error=target_info["error"]
        )

    # Get column name set
    source_cols = {col["name"] for col in source_info["columns"]}
    target_cols = {col["name"] for col in target_info["columns"]}
    
    source_part_cols = {col["name"] for col in source_info["partition_cols"]}
    target_part_cols = {col["name"] for col in target_info["partition_cols"]}
    
    # Compare differences
    missing_columns = list(source_cols - target_cols)
    extra_columns = list(target_cols - source_cols)
    missing_partition_cols = list(source_part_cols - target_part_cols)
    extra_partition_cols = list(target_part_cols - source_part_cols)
    
    return ValidationResult(
        database=source_info["database"],
        table_name=source_info["table_name"],
        columns_match=len(missing_columns) == 0 and len(extra_columns) == 0,
        partition_cols_match=len(missing_partition_cols) == 0 and len(extra_partition_cols) == 0,
        missing_columns=missing_columns,
        extra_columns=extra_columns,
        missing_partition_cols=missing_partition_cols,
        extra_partition_cols=extra_partition_cols
    )

def save_validation_report(results: List[ValidationResult], output_dir: str):
    """Save validation report"""
    # Change to nested directory structure
    report_dir = os.path.join("migration_reports", "tables_creation_verification_reports")
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(report_dir, f"validation_report_{timestamp}.txt")
    
    with open(report_file, 'w') as f:
        f.write("Table Creation Validation Report\n")
        f.write("=" * 80 + "\n\n")
        
        # Group by database
        db_results = {}
        for result in results:
            if result.database not in db_results:
                db_results[result.database] = []
            db_results[result.database].append(result)
        
        total_success = 0
        total_tables = len(results)
        
        for db, db_results in db_results.items():
            f.write(f"\n📁 Database: {db}\n")
            f.write("-" * 80 + "\n")
            
            for result in db_results:
                status = "✅" if result.columns_match and result.partition_cols_match else "❌"
                f.write(f"\n{status} Table: {result.table_name}\n")
                
                if result.error:
                    f.write(f"  Error: {result.error}\n")
                    continue
                
                if not result.columns_match:
                    if result.missing_columns:
                        f.write("  Missing columns: " + ", ".join(result.missing_columns) + "\n")
                    if result.extra_columns:
                        f.write("  Extra columns: " + ", ".join(result.extra_columns) + "\n")
                
                if not result.partition_cols_match:
                    if result.missing_partition_cols:
                        f.write("  Missing partition columns: " + ", ".join(result.missing_partition_cols) + "\n")
                    if result.extra_partition_cols:
                        f.write("  Extra partition columns: " + ", ".join(result.extra_partition_cols) + "\n")
                
                if result.columns_match and result.partition_cols_match:
                    total_success += 1
            
            f.write("\n" + "-" * 80 + "\n")
        
        # Write total
        f.write(f"\nSummary:\n")
        f.write(f"Total tables: {total_tables}\n")
        f.write(f"Successfully matched: {total_success}\n")
        f.write(f"Failed: {total_tables - total_success}\n")
        
    print(f"\nValidation report saved to: {report_file}")
    return report_file

def main(info_file: str, target_catalog: str, target_warehouse: str):
    """Main function"""
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session(target_catalog, target_warehouse)
        
        # Load source table information
        source_tables = load_source_tables_info(info_file)
        
        # Validate each table
        results = []
        for source_info in source_tables:
            print(f"Validating table: {source_info['database']}.{source_info['table_name']}")
            
            # Get target table information
            target_info = get_target_table_info(
                spark, 
                target_catalog, 
                source_info['database'], 
                source_info['table_name']
            )
            
            # Compare table structures
            result = compare_tables(source_info, target_info)
            results.append(result)
        
        # Save validation report
        report_file = save_validation_report(results, "validation-reports")
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Verify table creation in target catalog')
    parser.add_argument('--info-file', required=True, help='Path to the source table info JSON file')
    parser.add_argument('--target-catalog', required=True, help='Name of the target catalog')
    parser.add_argument('--target-warehouse', required=True, help='Warehouse URI/ARN for the target catalog')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.info_file):
        print(f"Error: Table info file not found: {args.info_file}")
        exit(1)
    
    main(args.info_file, args.target_catalog, args.target_warehouse) 