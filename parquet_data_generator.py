#!/usr/bin/env python3
"""
Parquet数据生成器 - 为InfluxDB 3分布式集群生成测试数据
生成包含traceid、starttime、blockinfo三列的Parquet文件
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import uuid
import random
from datetime import datetime, timedelta
import json
import argparse
import os

def generate_trace_data(num_records=100000, start_date=None):
    """
    生成追踪数据
    
    Args:
        num_records: 记录数量
        start_date: 开始日期，默认为当前时间前7天
    
    Returns:
        pandas.DataFrame: 包含traceid、starttime、blockinfo的数据框
    """
    if start_date is None:
        start_date = datetime.now() - timedelta(days=7)
    
    print(f"生成 {num_records} 条记录，开始时间: {start_date}")
    
    # 生成traceid - 使用UUID4格式
    trace_ids = [str(uuid.uuid4()) for _ in range(num_records)]
    
    # 生成starttime - 在过去7天内随机分布
    time_range_seconds = 7 * 24 * 60 * 60  # 7天的秒数
    start_times = []
    for _ in range(num_records):
        random_seconds = random.randint(0, time_range_seconds)
        timestamp = start_date + timedelta(seconds=random_seconds)
        start_times.append(timestamp)
    
    # 生成blockinfo - 模拟区块链或分布式系统的块信息
    block_infos = []
    for i in range(num_records):
        block_info = {
            "block_id": f"block_{random.randint(1000000, 9999999)}",
            "block_height": random.randint(1, 1000000),
            "transaction_count": random.randint(1, 1000),
            "block_size": random.randint(1024, 1024*1024),  # 1KB to 1MB
            "validator": f"validator_{random.randint(1, 100)}",
            "gas_used": random.randint(21000, 8000000),
            "gas_limit": 8000000,
            "difficulty": random.randint(1000000, 10000000),
            "network": random.choice(["mainnet", "testnet", "devnet"]),
            "status": random.choice(["confirmed", "pending", "failed"]),
            "fees": round(random.uniform(0.001, 1.0), 6)
        }
        block_infos.append(json.dumps(block_info))
    
    # 创建DataFrame
    df = pd.DataFrame({
        'traceid': trace_ids,
        'starttime': start_times,
        'blockinfo': block_infos
    })
    
    # 按时间排序
    df = df.sort_values('starttime').reset_index(drop=True)
    
    return df

def save_to_parquet(df, filename, compression='snappy'):
    """
    保存DataFrame到Parquet文件
    
    Args:
        df: pandas.DataFrame
        filename: 输出文件名
        compression: 压缩算法 ('snappy', 'gzip', 'brotli', 'lz4')
    """
    print(f"保存到Parquet文件: {filename}")
    print(f"使用压缩算法: {compression}")
    
    # 转换为PyArrow表以获得更好的控制
    table = pa.Table.from_pandas(df)
    
    # 写入Parquet文件
    pq.write_table(
        table, 
        filename,
        compression=compression,
        use_dictionary=True,  # 使用字典编码以减少文件大小
        row_group_size=10000,  # 每个行组10000行
        write_statistics=True  # 写入统计信息以优化查询
    )
    
    # 显示文件信息
    file_size = os.path.getsize(filename)
    print(f"文件大小: {file_size / (1024*1024):.2f} MB")
    
    # 读取并显示Parquet文件元数据
    parquet_file = pq.ParquetFile(filename)
    print(f"行组数量: {parquet_file.num_row_groups}")
    print(f"总行数: {len(df)}")
    print(f"列数: {len(df.columns)}")
    
    return filename

def generate_multiple_files(base_filename, num_files=3, records_per_file=50000):
    """
    生成多个Parquet文件以模拟分布式数据
    
    Args:
        base_filename: 基础文件名
        num_files: 文件数量
        records_per_file: 每个文件的记录数
    """
    files = []
    start_date = datetime.now() - timedelta(days=7)
    
    for i in range(num_files):
        print(f"\n=== 生成文件 {i+1}/{num_files} ===")
        
        # 为每个文件生成不同时间段的数据
        file_start_date = start_date + timedelta(days=i * 2)
        df = generate_trace_data(records_per_file, file_start_date)
        
        # 生成文件名
        filename = f"{base_filename}_part_{i+1:02d}.parquet"
        save_to_parquet(df, filename)
        files.append(filename)
        
        print(f"文件 {filename} 生成完成")
    
    return files

def main():
    parser = argparse.ArgumentParser(description='生成InfluxDB 3测试用的Parquet数据文件')
    parser.add_argument('--records', type=int, default=100000, help='每个文件的记录数 (默认: 100000)')
    parser.add_argument('--files', type=int, default=3, help='生成的文件数量 (默认: 3)')
    parser.add_argument('--output', type=str, default='trace_data', help='输出文件基础名称 (默认: trace_data)')
    parser.add_argument('--compression', type=str, default='snappy', 
                       choices=['snappy', 'gzip', 'brotli', 'lz4'], 
                       help='压缩算法 (默认: snappy)')
    
    args = parser.parse_args()
    
    print("=== InfluxDB 3 Parquet数据生成器 ===")
    print(f"记录数/文件: {args.records:,}")
    print(f"文件数量: {args.files}")
    print(f"总记录数: {args.records * args.files:,}")
    print(f"压缩算法: {args.compression}")
    print()
    
    # 生成多个文件
    files = generate_multiple_files(args.output, args.files, args.records)
    
    print(f"\n=== 生成完成 ===")
    print("生成的文件:")
    total_size = 0
    for file in files:
        size = os.path.getsize(file)
        total_size += size
        print(f"  {file}: {size / (1024*1024):.2f} MB")
    
    print(f"\n总文件大小: {total_size / (1024*1024):.2f} MB")
    print(f"平均压缩比: {(args.records * args.files * 200) / total_size:.2f}:1")  # 假设每行约200字节
    
    # 显示示例数据
    print(f"\n=== 示例数据 (来自 {files[0]}) ===")
    sample_df = pd.read_parquet(files[0])
    print(sample_df.head(3).to_string())
    
    print(f"\n=== 数据类型信息 ===")
    print(sample_df.dtypes)
    
    print(f"\n=== 时间范围 ===")
    print(f"最早时间: {sample_df['starttime'].min()}")
    print(f"最晚时间: {sample_df['starttime'].max()}")

if __name__ == "__main__":
    main()
