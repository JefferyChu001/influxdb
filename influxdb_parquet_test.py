#!/usr/bin/env python3
"""
InfluxDB 3 Parquet数据导入和查询测试工具
支持将Parquet文件导入到InfluxDB 3分布式集群，并执行复杂查询测试
"""

import pandas as pd
import pyarrow.parquet as pq
import requests
import json
import time
import argparse
import os
from datetime import datetime
import numpy as np
from urllib.parse import quote

class InfluxDBParquetTester:
    def __init__(self, nodes=None):
        """
        初始化InfluxDB测试器
        
        Args:
            nodes: InfluxDB节点列表，格式为 [{"host": "127.0.0.1", "port": 8181}, ...]
        """
        if nodes is None:
            self.nodes = [
                {"host": "127.0.0.1", "port": 8181, "name": "node1"},
                {"host": "127.0.0.1", "port": 8182, "name": "node2"}
            ]
        else:
            self.nodes = nodes
        
        self.database = "tracedb"
        self.table = "traces"
        
    def check_nodes_health(self):
        """检查所有节点的健康状态"""
        print("=== 检查节点健康状态 ===")
        healthy_nodes = []
        
        for node in self.nodes:
            url = f"http://{node['host']}:{node['port']}/health"
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    print(f"✅ {node['name']} ({node['host']}:{node['port']}) - 健康")
                    healthy_nodes.append(node)
                else:
                    print(f"❌ {node['name']} ({node['host']}:{node['port']}) - 状态码: {response.status_code}")
            except Exception as e:
                print(f"❌ {node['name']} ({node['host']}:{node['port']}) - 错误: {e}")
        
        return healthy_nodes
    
    def convert_parquet_to_line_protocol_batches(self, parquet_file, batch_size=1000):
        """
        将Parquet文件转换为InfluxDB Line Protocol格式的批次

        Args:
            parquet_file: Parquet文件路径
            batch_size: 每批次的记录数

        Yields:
            str: Line Protocol格式的数据批次
        """
        print(f"读取Parquet文件: {parquet_file}")
        df = pd.read_parquet(parquet_file)

        print(f"文件包含 {len(df)} 条记录，将分为 {len(df)//batch_size + 1} 个批次")

        for start_idx in range(0, len(df), batch_size):
            end_idx = min(start_idx + batch_size, len(df))
            batch_df = df.iloc[start_idx:end_idx]

            lines = []
            for _, row in batch_df.iterrows():
                # 解析blockinfo JSON
                try:
                    block_info = json.loads(row['blockinfo'])
                except:
                    continue

                # 转换时间戳为纳秒
                timestamp_ns = int(pd.Timestamp(row['starttime']).timestamp() * 1_000_000_000)

                # 构建Line Protocol行
                # 格式: measurement,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
                block_id = block_info.get('block_id', '')
                line = (
                    f"{self.table},"
                    f"traceid={row['traceid']},"
                    f"network={block_info.get('network', 'unknown')},"
                    f"status={block_info.get('status', 'unknown')},"
                    f"validator={block_info.get('validator', 'unknown')} "
                    f"block_height={block_info.get('block_height', 0)}i,"
                    f"transaction_count={block_info.get('transaction_count', 0)}i,"
                    f"block_size={block_info.get('block_size', 0)}i,"
                    f"gas_used={block_info.get('gas_used', 0)}i,"
                    f"gas_limit={block_info.get('gas_limit', 0)}i,"
                    f"difficulty={block_info.get('difficulty', 0)}i,"
                    f"fees={block_info.get('fees', 0.0)},"
                    f'block_id="{block_id}" '
                    f"{timestamp_ns}"
                )
                lines.append(line)

            yield '\n'.join(lines), len(lines)
    
    def write_data_to_node(self, node, line_protocol_data):
        """
        将Line Protocol数据写入指定节点
        
        Args:
            node: 节点信息
            line_protocol_data: Line Protocol格式的数据
            
        Returns:
            tuple: (成功标志, 响应时间, 错误信息)
        """
        url = f"http://{node['host']}:{node['port']}/api/v3/write_lp?db={self.database}"
        
        headers = {
            'Content-Type': 'text/plain'
        }
        
        start_time = time.time()
        try:
            response = requests.post(url, data=line_protocol_data, headers=headers, timeout=30)
            end_time = time.time()
            
            if response.status_code == 204:  # No Content - 成功
                return True, end_time - start_time, None
            else:
                return False, end_time - start_time, f"状态码: {response.status_code}, 响应: {response.text}"
        except Exception as e:
            end_time = time.time()
            return False, end_time - start_time, str(e)
    
    def import_parquet_files(self, parquet_files, distribute=True, batch_size=1000):
        """
        导入Parquet文件到InfluxDB集群

        Args:
            parquet_files: Parquet文件列表
            distribute: 是否分布式导入到不同节点
            batch_size: 每批次的记录数
        """
        print(f"\n=== 导入 {len(parquet_files)} 个Parquet文件 ===")

        healthy_nodes = self.check_nodes_health()
        if not healthy_nodes:
            print("❌ 没有健康的节点可用")
            return False

        total_records = 0
        total_time = 0

        for i, parquet_file in enumerate(parquet_files):
            if not os.path.exists(parquet_file):
                print(f"❌ 文件不存在: {parquet_file}")
                continue

            # 选择目标节点
            if distribute:
                target_node = healthy_nodes[i % len(healthy_nodes)]
            else:
                target_node = healthy_nodes[0]

            print(f"\n导入文件 {i+1}/{len(parquet_files)}: {parquet_file}")
            print(f"目标节点: {target_node['name']}")

            file_records = 0
            file_time = 0

            # 批量处理文件
            for batch_data, batch_count in self.convert_parquet_to_line_protocol_batches(parquet_file, batch_size):
                # 写入数据
                success, write_time, error = self.write_data_to_node(target_node, batch_data)

                if success:
                    print(f"  ✅ 批次成功: {batch_count:,} 条记录，耗时: {write_time:.2f}秒")
                    file_records += batch_count
                    file_time += write_time
                else:
                    print(f"  ❌ 批次失败: {error}")
                    break

            if file_records > 0:
                print(f"✅ 文件导入完成: {file_records:,} 条记录，总耗时: {file_time:.2f}秒")
                print(f"   平均写入速度: {file_records/file_time:.0f} 记录/秒")
                total_records += file_records
                total_time += file_time

        if total_records > 0:
            print(f"\n=== 导入总结 ===")
            print(f"总记录数: {total_records:,}")
            print(f"总耗时: {total_time:.2f}秒")
            print(f"平均写入速度: {total_records/total_time:.0f} 记录/秒")

        return total_records > 0
    
    def execute_query(self, node, query, query_name="查询"):
        """
        在指定节点执行SQL查询
        
        Args:
            node: 节点信息
            query: SQL查询语句
            query_name: 查询名称
            
        Returns:
            tuple: (成功标志, 结果, 查询时间, 错误信息)
        """
        url = f"http://{node['host']}:{node['port']}/api/v3/query_sql"
        
        params = {
            'db': self.database,
            'q': query
        }
        
        start_time = time.time()
        try:
            response = requests.get(url, params=params, timeout=60)
            end_time = time.time()
            query_time = end_time - start_time
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    return True, result, query_time, None
                except:
                    return True, response.text, query_time, None
            else:
                return False, None, query_time, f"状态码: {response.status_code}, 响应: {response.text}"
        except Exception as e:
            end_time = time.time()
            query_time = end_time - start_time
            return False, None, query_time, str(e)
    
    def run_complex_queries(self):
        """运行复杂查询测试"""
        print(f"\n=== 复杂查询测试 ===")
        
        healthy_nodes = self.check_nodes_health()
        if not healthy_nodes:
            print("❌ 没有健康的节点可用")
            return
        
        # 定义复杂查询
        queries = [
            {
                "name": "基础统计查询",
                "sql": f"SELECT COUNT(*) as total_traces, AVG(block_height) as avg_height, MAX(gas_used) as max_gas FROM {self.table}"
            },
            {
                "name": "按网络分组统计",
                "sql": f"SELECT network, COUNT(*) as trace_count, AVG(fees) as avg_fees, SUM(transaction_count) as total_txns FROM {self.table} GROUP BY network ORDER BY trace_count DESC"
            },
            {
                "name": "高Gas使用量查询",
                "sql": f"SELECT traceid, block_height, gas_used, fees FROM {self.table} WHERE gas_used > 5000000 ORDER BY gas_used DESC LIMIT 10"
            },
            {
                "name": "时间范围聚合查询",
                "sql": f"SELECT DATE_TRUNC('hour', time) as hour, COUNT(*) as traces_per_hour, AVG(block_size) as avg_block_size FROM {self.table} GROUP BY hour ORDER BY hour LIMIT 24"
            },
            {
                "name": "复杂条件过滤",
                "sql": f"SELECT validator, network, COUNT(*) as blocks, AVG(difficulty) as avg_difficulty FROM {self.table} WHERE status = 'confirmed' AND fees > 0.1 GROUP BY validator, network HAVING COUNT(*) > 100 ORDER BY blocks DESC"
            },
            {
                "name": "百分位数分析",
                "sql": f"SELECT network, APPROX_PERCENTILE(gas_used, 0.5) as median_gas, APPROX_PERCENTILE(gas_used, 0.95) as p95_gas, APPROX_PERCENTILE(fees, 0.99) as p99_fees FROM {self.table} GROUP BY network"
            }
        ]
        
        # 在每个节点上执行查询
        for node in healthy_nodes:
            print(f"\n--- 在节点 {node['name']} 上执行查询 ---")
            
            for query in queries:
                print(f"\n🔍 {query['name']}")
                print(f"SQL: {query['sql']}")
                
                success, result, query_time, error = self.execute_query(node, query['sql'], query['name'])
                
                if success:
                    print(f"✅ 查询成功，耗时: {query_time:.3f}秒")
                    
                    # 显示结果摘要
                    if isinstance(result, list) and len(result) > 0:
                        print(f"   返回行数: {len(result)}")
                        if len(result) <= 5:
                            print("   结果:")
                            for row in result:
                                print(f"     {row}")
                        else:
                            print("   前3行结果:")
                            for row in result[:3]:
                                print(f"     {row}")
                            print(f"   ... (还有 {len(result)-3} 行)")
                    else:
                        print(f"   结果: {result}")
                else:
                    print(f"❌ 查询失败: {error}")
                    print(f"   耗时: {query_time:.3f}秒")

def main():
    parser = argparse.ArgumentParser(description='InfluxDB 3 Parquet数据导入和查询测试')
    parser.add_argument('--import-files', nargs='+', help='要导入的Parquet文件列表')
    parser.add_argument('--query-only', action='store_true', help='只执行查询，不导入数据')
    parser.add_argument('--nodes', nargs='+', help='InfluxDB节点地址，格式: host:port')
    
    args = parser.parse_args()
    
    # 解析节点配置
    nodes = None
    if args.nodes:
        nodes = []
        for i, node_addr in enumerate(args.nodes):
            host, port = node_addr.split(':')
            nodes.append({
                "host": host,
                "port": int(port),
                "name": f"node{i+1}"
            })
    
    # 创建测试器
    tester = InfluxDBParquetTester(nodes)
    
    if not args.query_only and args.import_files:
        # 导入数据
        success = tester.import_parquet_files(args.import_files)
        if not success:
            print("❌ 数据导入失败")
            return
    
    # 执行查询测试
    tester.run_complex_queries()

if __name__ == "__main__":
    main()
