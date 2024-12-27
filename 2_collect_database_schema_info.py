from pyspark.sql import SparkSession
import time
from datetime import datetime
import os
from dataclasses import dataclass
from typing import List, Dict
import json
from pyspark.sql.functions import lit

@dataclass
class TableInfo:
    database: str
    table_name: str
    columns: List[Dict[str, str]]
    partition_cols: List[Dict[str, str]]
    location: str
    properties: Dict[str, str]
    files: List[str]
    snapshot_id: str

def load_latest_snapshots(snapshot_info_file: str, target_timestamp: str) -> Dict[str, str]:
    """加载每个表在指定时间点之前的最新快照ID
    
    Args:
        snapshot_info_file: 快照信息文件路径
        target_timestamp: 目标时间点 (ISO格式，如 "2024-01-01T00:00:00Z")
    
    Returns:
        Dict[str, str]: 表名到快照ID的映射
    """
    with open(snapshot_info_file, 'r') as f:
        snapshots_data = json.load(f)
    
    latest_snapshots = {}
    target_dt = datetime.fromisoformat(target_timestamp.replace('Z', '+00:00'))
    
    for table_info in snapshots_data:
        database = table_info['database']
        table_name = table_info['table_name']
        table_key = f"{database}.{table_name}"
        
        # 过滤并排序快照
        valid_snapshots = []
        for snapshot in table_info['snapshots']:
            snapshot_dt = datetime.fromisoformat(snapshot['committed_at'].replace('Z', '+00:00'))
            if snapshot_dt <= target_dt:
                valid_snapshots.append(snapshot)
        
        if valid_snapshots:
            # 按时间戳排序，获取最新的快照
            latest_snapshot = max(valid_snapshots, 
                                key=lambda x: datetime.fromisoformat(x['committed_at'].replace('Z', '+00:00')))
            latest_snapshots[table_key] = str(latest_snapshot['snapshot_id'])
    
    return latest_snapshots

def collect_source_info(catalog_name: str, warehouse_uri: str, database: str, 
                       snapshot_info_file: str, target_timestamp: str):
    """收集源数据信息
    
    Args:
        catalog_name: Catalog名称
        warehouse_uri: 仓库URI
        database: 数据库名称
        snapshot_info_file: 快照信息文件路径
        target_timestamp: 目标时间点
    """
    if not all([catalog_name, warehouse_uri, database, snapshot_info_file, target_timestamp]):
        raise ValueError("All parameters must be provided")
        
    print(f"Starting to collect data information for catalog {catalog_name}, "
          f"database {database} before {target_timestamp}...")
    
    # 加载指定时间点之前的最新快照信息
    latest_snapshots = load_latest_snapshots(snapshot_info_file, target_timestamp)
    if not latest_snapshots:
        raise ValueError(f"No valid snapshots found before {target_timestamp}")
    
    source_spark = None
    tables_info = []
    
    try:
        source_spark = SparkSession.builder \
            .appName("Iceberg Source Info Collector") \
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

        # 获取数据库中的所有表
        tables = list_tables_in_database(source_spark, catalog_name, database)
        print(f"Found {len(tables)} tables in database {database}")
        
        # 收集每个表在其最新快照时的信息
        for table in tables:
            table_key = f"{database}.{table}"
            if table_key not in latest_snapshots:
                print(f"Warning: No valid snapshot found for table {table_key} before {target_timestamp}")
                continue
                
            snapshot_id = latest_snapshots[table_key]
            try:
                table_info = collect_table_info(
                    source_spark, catalog_name, database, table, 
                    snapshot_id, warehouse_uri
                )
                if table_info:
                    tables_info.append(table_info)
                    print(f"Successfully collected information for table: {table_key} "
                          f"at snapshot {snapshot_id}")
            except Exception as e:
                print(f"Failed to collect information for table {table_key}: {str(e)}")
                continue
                
        if tables_info:
            save_collected_info(tables_info)
            print(f"Successfully collected information for {len(tables_info)} out of {len(tables)} tables")
        else:
            print("No table information was collected successfully")
            
    finally:
        if source_spark:
            source_spark.stop()

def list_tables_in_database(spark, catalog_name, database):
    """List all tables in database"""
    tables = []
    result = spark.sql(f"SHOW TABLES IN {catalog_name}.{database}").collect()
    for row in result:
        tables.append(row.tableName)
    return tables

def collect_table_info(spark, catalog_name, database, table, snapshot_id, warehouse_uri):
    """Collect detailed table information for a specific snapshot"""
    try:
        # 处理带有破折号的表名
        if '-' in table:
            quoted_table = f"`{table}`"
        else:
            quoted_table = table
            
        # 使用DataFrame API读取特定快照的数据，这样可以获取该快照的schema
        df = spark.read \
            .option("as_of_snapshot_id", snapshot_id) \
            .table(f"{catalog_name}.{database}.{quoted_table}")
        
        # 获取schema信息
        schema = df.schema
        if database == "ns1" and quoted_table == "large_table":
            print("================================================")
            print(f"schema: {schema.fields}")
        
        # 检查是否为 Iceberg 表
        table_desc = spark.sql(f"DESCRIBE TABLE EXTENDED {catalog_name}.{database}.{quoted_table}").collect()
        is_iceberg = False
        for row in table_desc:
            if 'Provider' in str(row.col_name) and 'iceberg' in str(row.data_type).lower():
                is_iceberg = True
                break
                
        if not is_iceberg:
            print(f"Table {database}.{table} is not an Iceberg table, skipping...")
            return None
            
        # 从schema中提取列信息
        columns = []
        for field in schema.fields:
            columns.append({
                'name': field.name,
                'type': str(field.dataType)
            })
            
        # 获取分区信息和其他表属性
        partition_cols = []
        properties = {}
        location = None
        
        for row in table_desc:
            col_name = str(row.col_name).strip()
            if col_name == '# Partition Information':
                # 下一行开始是分区列信息
                continue
            elif col_name.startswith('# Partitioning'):
                # 解析分区列信息
                if row.data_type:
                    partition_info = str(row.data_type).strip()
                    if partition_info.startswith('['):
                        partition_names = [p.strip() for p in partition_info[1:-1].split(',')]
                        for p_name in partition_names:
                            # 从columns���找到对应的类型
                            for col in columns:
                                if col['name'] == p_name:
                                    partition_cols.append({
                                        'name': p_name,
                                        'type': col['type']
                                    })
                                    break
            elif ':' in col_name:
                key, value = col_name.split(':', 1)
                if key.strip() == 'Location':
                    value = value.strip()
                    if not value.startswith('s3://'):
                        location = warehouse_uri.rstrip('/') + '/' + value
                    else:
                        location = value
                properties[key.strip()] = value.strip()

        if not location:
            location = warehouse_uri

        # 获取文件信息
        snapshots_table = f"{catalog_name}.{database}.{quoted_table}.snapshots"
        files_df = spark.table(snapshots_table).where(f"snapshot_id = {snapshot_id}")
        files_df = files_df.select("file_path")
        
        files = [row.file_path for row in files_df.collect()]

        return TableInfo(
            database=database,
            table_name=table,
            columns=columns,
            partition_cols=partition_cols,
            location=location,
            properties=properties,
            files=files,
            snapshot_id=snapshot_id
        )
    except Exception as e:
        print(f"Error collecting information for table {database}.{table}: {str(e)}")
        return None

def save_collected_info(tables_info):
    """Save collected information"""
    info_dir = "migration_info/database_schema"
    os.makedirs(info_dir, exist_ok=True)
    
    if not tables_info:
        print("No table information to save")
        return
        
    database = tables_info[0].database
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(info_dir, f"tables_info_{database}_{timestamp}.json")
    
    with open(filename, 'w') as f:
        json.dump([vars(info) for info in tables_info], f, indent=2)
    
    print(f"Table information saved to: {filename}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect database schema information for specific timestamp')
    parser.add_argument('--catalog-name', required=True, 
                      help='Name of the catalog')
    parser.add_argument('--warehouse-uri', required=True, 
                      help='S3 URI of the warehouse')
    parser.add_argument('--database', required=True, 
                      help='Database to collect information from')
    parser.add_argument('--snapshot-info-file', required=True, 
                      help='Path to the snapshot info JSON file from step 1')
    parser.add_argument('--timestamp', required=True, 
                      help='Target timestamp in ISO format (e.g., 2024-01-01T00:00:00Z)')
    
    args = parser.parse_args()
    
    collect_source_info(
        catalog_name=args.catalog_name,
        warehouse_uri=args.warehouse_uri,
        database=args.database,
        snapshot_info_file=args.snapshot_info_file,
        target_timestamp=args.timestamp
    )
