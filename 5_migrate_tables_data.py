from pyspark.sql import SparkSession
import json
import os
from datetime import datetime
import time
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class MigrationResult:
    database: str
    table_name: str
    snapshot_id: str
    records_count: int
    start_time: float
    end_time: float
    status: str
    error: Optional[str] = None

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

def get_spark_session(source_catalog: str, source_warehouse: str,
                     target_catalog: str, target_warehouse: str) -> SparkSession:
    """创建支持源和目标 catalog 的 Spark session"""
    return SparkSession.builder \
        .appName("Migrate Table Data") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.0") \
        .config(f"spark.sql.catalog.{source_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{source_catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{source_catalog}.warehouse", source_warehouse) \
        .config(f"spark.sql.catalog.{target_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{target_catalog}.catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog") \
        .config(f"spark.sql.catalog.{target_catalog}.warehouse", target_warehouse) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()

def load_tables_info(info_file: str) -> List[dict]:
    """加载表信息"""
    with open(info_file, 'r') as f:
        return json.load(f)

def migrate_table_data(spark: SparkSession, 
                      source_catalog: str,
                      target_catalog: str,
                      table_info: dict) -> MigrationResult:
    """迁移单个表的数据"""
    database = table_info['database']
    table_name = table_info['table_name']
    snapshot_id = table_info['snapshot_id']
    
    start_time = time.time()
    
    try:
        # 构建源表和目标表的完整名称
        source_table = f"{source_catalog}.{database}.{table_name}"
        target_table = f"{target_catalog}.{database}.{table_name}"
        
        print(f"Migrating data for table {source_table} (snapshot: {snapshot_id}) to {target_table}")
        
        # 首先验证快照是否存在
        snapshots_table = f"{source_table}.snapshots"
        snapshot_exists = spark.table(snapshots_table) \
            .where(f"snapshot_id = {snapshot_id}") \
            .count() > 0
        
        if not snapshot_exists:
            raise Exception(f"Cannot find snapshot with ID {snapshot_id}")
        
        # 从特定 snapshot 读取数据
        df = spark.read \
            .option("snapshot-id", snapshot_id) \
            .table(source_table)
        
        records_count = df.count()
        print(f"Found {records_count} records to migrate")
        
        # 写入目标表
        df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(target_table)
        
        end_time = time.time()
        print(f"Successfully migrated {records_count} records in {end_time - start_time:.2f} seconds")
        
        return MigrationResult(
            database=database,
            table_name=table_name,
            snapshot_id=snapshot_id,
            records_count=records_count,
            start_time=start_time,
            end_time=end_time,
            status="success"
        )
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error migrating table {database}.{table_name}: {error_msg}")
        return MigrationResult(
            database=database,
            table_name=table_name,
            snapshot_id=snapshot_id,
            records_count=0,
            start_time=start_time,
            end_time=time.time(),
            status="failed",
            error=error_msg
        )

def save_migration_report(results: List[MigrationResult], output_dir: str):
    """保存迁移报告"""
    # 创建更具体的子目录路径
    report_dir = os.path.join(output_dir, "tables_data_migration_reports")
    os.makedirs(report_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(report_dir, f"data_migration_report_{timestamp}.txt")
    
    with open(report_file, 'w') as f:
        f.write("Table Data Migration Report\n")
        f.write("=" * 80 + "\n\n")
        
        # 按数据库分组统计
        db_results = {}
        for result in results:
            if result.database not in db_results:
                db_results[result.database] = []
            db_results[result.database].append(result)
        
        total_success = sum(1 for r in results if r.status == "success")
        total_records = sum(r.records_count for r in results if r.status == "success")
        total_tables = len(results)
        
        for db, db_results in db_results.items():
            f.write(f"\n📁 Database: {db}\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Table Name':<40} {'Status':<10} {'Records':<10} {'Duration(s)':<10}\n")
            f.write("-" * 80 + "\n")
            
            db_records = 0
            for result in db_results:
                status_icon = "✅" if result.status == "success" else "❌"
                duration = f"{result.duration:.2f}" if result.status == "success" else "N/A"
                
                f.write(f"{result.table_name:<40} {status_icon} {result.status:<8} {result.records_count:<10} {duration:<10}\n")
                if result.error:
                    f.write(f"  Error: {result.error}\n")
                
                if result.status == "success":
                    db_records += result.records_count
            
            f.write(f"\nDatabase Summary:\n")
            f.write(f"Successfully migrated tables: {sum(1 for r in db_results if r.status == 'success')}/{len(db_results)}\n")
            f.write(f"Total records migrated: {db_records:,}\n")
            f.write("\n" + "-" * 80 + "\n")
        
        # 写入总结
        f.write(f"\nOverall Summary:\n")
        f.write(f"Total tables: {total_tables}\n")
        f.write(f"Successfully migrated: {total_success}\n")
        f.write(f"Failed: {total_tables - total_success}\n")
        f.write(f"Total records migrated: {total_records:,}\n")
        
        # 如果有失败的表，在报告末尾添加警告
        if total_success < total_tables:
            f.write("\n⚠️ WARNING: Some tables failed to migrate! Please check the error messages above.\n")
        
    print(f"\nMigration report saved to: {report_file}")
    return report_file

def main(info_file: str, source_catalog: str, source_warehouse: str,
         target_catalog: str, target_warehouse: str):
    """主函数"""
    spark = None
    try:
        # 创建 Spark session
        spark = get_spark_session(source_catalog, source_warehouse,
                                target_catalog, target_warehouse)
        
        # 加载表信息
        tables_info = load_tables_info(info_file)
        
        # 迁移每个表的数据
        results = []
        for table_info in tables_info:
            result = migrate_table_data(
                spark,
                source_catalog,
                target_catalog,
                table_info
            )
            results.append(result)
        
        # 保存迁移报告
        report_file = save_migration_report(results, "migration_reports")
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate table data from source to target catalog')
    parser.add_argument('--info-file', required=True,
                      help='Path to the table info JSON file')
    parser.add_argument('--source-catalog', required=True,
                      help='Name of the source catalog')
    parser.add_argument('--source-warehouse', required=True,
                      help='Warehouse URI for the source catalog')
    parser.add_argument('--target-catalog', required=True,
                      help='Name of the target catalog')
    parser.add_argument('--target-warehouse', required=True,
                      help='Warehouse URI/ARN for the target catalog')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.info_file):
        print(f"Error: Table info file not found: {args.info_file}")
        exit(1)
    
    main(args.info_file, args.source_catalog, args.source_warehouse,
         args.target_catalog, args.target_warehouse)
