#!/bin/bash

# 分布式查询测试脚本

set -e

echo "🚀 分布式查询测试"
echo "=================="
echo ""

# 清理旧数据
echo "📁 清理旧数据..."
rm -rf /tmp/influxdb_node1 /tmp/influxdb_node2 /tmp/influxdb_node3
mkdir -p /tmp/influxdb_node1 /tmp/influxdb_node2 /tmp/influxdb_node3

# 检查 influxdb3 是否存在
if [ ! -f "./target/debug/influxdb3" ]; then
    echo "❌ 未找到 influxdb3 binary，正在编译..."
    cargo build -p influxdb3
fi

echo "✅ 准备完成"
echo ""

# 启动 etcd (如果需要)
echo "🔧 检查 etcd..."
if ! curl -s http://127.0.0.1:2379/version > /dev/null 2>&1; then
    echo "❌ etcd 未运行，请先启动 etcd"
    echo "提示: etcd --listen-client-urls http://127.0.0.1:2379 --advertise-client-urls http://127.0.0.1:2379"
    exit 1
fi
echo "✅ etcd 运行中"
echo ""

# 启动节点函数
start_node() {
    local node_id=$1
    local http_port=$2
    local data_dir=$3
    
    echo "🖥️  启动 Node ${node_id} (HTTP: ${http_port})..."
    
    ./target/debug/influxdb3 serve \
        --object-store file \
        --data-dir ${data_dir} \
        --http-bind 127.0.0.1:${http_port} \
        --node-id node${node_id} \
        --without-auth > /tmp/node${node_id}.log 2>&1 &
    
    local pid=$!
    echo ${pid} > /tmp/node${node_id}.pid
    
    # 等待节点启动
    for i in {1..10}; do
        if curl -s http://127.0.0.1:${http_port}/health > /dev/null 2>&1; then
            echo "✅ Node ${node_id} 已启动 (PID: ${pid})"
            return 0
        fi
        sleep 1
    done
    
    echo "❌ Node ${node_id} 启动失败"
    return 1
}

# 启动 3 个节点
echo "🚀 启动节点..."
start_node 1 8181 /tmp/influxdb_node1
start_node 2 8182 /tmp/influxdb_node2
start_node 3 8183 /tmp/influxdb_node3

echo ""
echo "✅ 所有节点启动成功"
echo ""

# 等待节点完全就绪
sleep 3

# 写入测试数据到不同节点
echo "📝 写入测试数据..."
echo ""

# Node 1 数据
echo "写入数据到 Node 1..."
curl -s -X POST "http://127.0.0.1:8181/api/v3/write_lp?db=mydb" \
  -H "Content-Type: text/plain" \
  --data-binary "
cpu,host=server1,region=us-east usage=85.0,cores=8 1609459200000000000
cpu,host=server1,region=us-east usage=90.5,cores=8 1609459260000000000
cpu,host=server1,region=us-east usage=78.2,cores=8 1609459320000000000
memory,host=server1,region=us-east used=4096,total=8192 1609459200000000000
memory,host=server1,region=us-east used=4512,total=8192 1609459260000000000
"

# Node 2 数据
echo "写入数据到 Node 2..."
curl -s -X POST "http://127.0.0.1:8182/api/v3/write_lp?db=mydb" \
  -H "Content-Type: text/plain" \
  --data-binary "
cpu,host=server2,region=us-west usage=65.3,cores=16 1609459200000000000
cpu,host=server2,region=us-west usage=72.1,cores=16 1609459260000000000
cpu,host=server2,region=us-west usage=68.9,cores=16 1609459320000000000
memory,host=server2,region=us-west used=8192,total=16384 1609459200000000000
memory,host=server2,region=us-west used=9000,total=16384 1609459260000000000
"

# Node 3 数据
echo "写入数据到 Node 3..."
curl -s -X POST "http://127.0.0.1:8183/api/v3/write_lp?db=mydb" \
  -H "Content-Type: text/plain" \
  --data-binary "
cpu,host=server3,region=eu-west usage=55.7,cores=4 1609459200000000000
cpu,host=server3,region=eu-west usage=61.2,cores=4 1609459260000000000
cpu,host=server3,region=eu-west usage=58.4,cores=4 1609459320000000000
memory,host=server3,region=eu-west used=2048,total=4096 1609459200000000000
memory,host=server3,region=eu-west used=2500,total=4096 1609459260000000000
"

echo ""
echo "✅ 数据写入完成"
echo ""

# 等待数据持久化
sleep 2

# 查询测试
echo "🔍 执行查询测试..."
echo ""

# 测试 1: 查询单个节点的数据
echo "测试 1: 查询 Node 1 的 CPU 数据"
echo "-----------------------------------"
curl -s -X POST "http://127.0.0.1:8181/api/v3/query_sql?db=mydb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT * FROM cpu WHERE host = '\''server1'\'' LIMIT 5"}' | jq .

echo ""

# 测试 2: 跨节点查询
echo "测试 2: 跨所有节点查询 CPU 平均值"
echo "-----------------------------------"
curl -s -X POST "http://127.0.0.1:8181/api/v3/query_sql?db=mydb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT host, AVG(usage) as avg_usage FROM cpu GROUP BY host"}' | jq .

echo ""

# 测试 3: 聚合查询
echo "测试 3: 全局 CPU 统计"
echo "-----------------------------------"
curl -s -X POST "http://127.0.0.1:8182/api/v3/query_sql?db=mydb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT COUNT(*) as count, AVG(usage) as avg_usage, MAX(usage) as max_usage FROM cpu"}' | jq .

echo ""

# 测试 4: 过滤和排序
echo "测试 4: 高 CPU 使用率主机"
echo "-----------------------------------"
curl -s -X POST "http://127.0.0.1:8183/api/v3/query_sql?db=mydb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT host, usage FROM cpu WHERE usage > 70 ORDER BY usage DESC"}' | jq .

echo ""
echo "✅ 查询测试完成"
echo ""

# 显示节点信息
echo "📊 节点状态"
echo "==========="
for i in 1 2 3; do
    pid=$(cat /tmp/node${i}.pid 2>/dev/null || echo "未运行")
    echo "Node ${i}: PID ${pid}, HTTP 818${i}"
done

echo ""
echo "💡 提示:"
echo "  - 查看节点日志: tail -f /tmp/node1.log"
echo "  - 停止所有节点: kill \$(cat /tmp/node*.pid)"
echo "  - 手动查询: curl http://127.0.0.1:8181/api/v3/query_sql?db=mydb -d '{\"q\":\"SELECT * FROM cpu\"}'"
echo ""
echo "按 Ctrl+C 停止节点并退出"

# 等待用户中断
trap "echo ''; echo '🛑 停止所有节点...'; kill \$(cat /tmp/node*.pid 2>/dev/null) 2>/dev/null; rm -f /tmp/node*.pid; echo '✅ 已清理'; exit 0" INT TERM

# 保持脚本运行，显示日志
echo ""
echo "📜 实时日志 (Node 1):"
echo "===================="
tail -f /tmp/node1.log

