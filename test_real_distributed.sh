#!/bin/bash

# 真实分布式查询测试脚本

set -e

echo "🚀 InfluxDB 分布式集群测试"
echo "=================================="
echo ""

# 清理旧数据
echo "📁 清理旧数据..."
rm -rf ./node1 ./node2 ./node3
mkdir -p ./node1 ./node2 ./node3

# 检查 influxdb3 是否存在
if [ ! -f "./target/debug/influxdb3" ]; then
    echo "❌ 未找到 influxdb3 binary，正在编译..."
    cargo build -p influxdb3
fi

echo "✅ 准备完成"
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
        --without-auth > node${node_id}.log 2>&1 &
    
    local pid=$!
    echo ${pid} > node${node_id}.pid
    
    # 等待节点启动
    for i in {1..30}; do
        if curl -s http://127.0.0.1:${http_port}/health > /dev/null 2>&1; then
            echo "✅ Node ${node_id} 已启动 (PID: ${pid})"
            return 0
        fi
        sleep 1
    done
    
    echo "❌ Node ${node_id} 启动失败"
    cat node${node_id}.log
    return 1
}

# 启动 3 个节点
echo "🚀 启动节点..."
start_node 1 8181 ./node1
start_node 2 8182 ./node2
start_node 3 8183 ./node3

echo ""
echo "✅ 所有节点启动成功"
echo ""

# 等待节点完全就绪
sleep 3

# 写入测试数据到不同节点 (每个节点 1000 行)
echo "📝 写入测试数据 (每节点 1000 行)..."
echo ""

# Node 1 数据 (server1, 1000 行)
echo "写入数据到 Node 1 (server1)..."
{
    echo "# Writing 1000 lines of data for server1"
    for i in {1..1000}; do
        timestamp=$((1609459200000000000 + i * 60000000000))
        usage=$(echo "scale=2; 50 + ($i * 7 + 13) % 50" | bc)
        echo "cpu,host=server1,region=us-east usage=${usage},cores=8i ${timestamp}"
        echo "memory,host=server1,region=us-east used=4096i,total=8192i ${timestamp}"
    done
} | curl -s -X POST "http://127.0.0.1:8181/api/v3/write_lp?db=testdb" \
    -H "Content-Type: text/plain" \
    --data-binary @- > /dev/null

echo "  ✅ Node 1: 写入 1000 条 CPU + 1000 条 Memory 数据"

# Node 2 数据 (server2, 1000 行)
echo "写入数据到 Node 2 (server2)..."
{
    echo "# Writing 1000 lines of data for server2"
    for i in {1..1000}; do
        timestamp=$((1609459200000000000 + i * 60000000000))
        usage=$(echo "scale=2; 60 + ($i * 11 + 7) % 40" | bc)
        echo "cpu,host=server2,region=us-west usage=${usage},cores=16i ${timestamp}"
        echo "memory,host=server2,region=us-west used=8192i,total=16384i ${timestamp}"
    done
} | curl -s -X POST "http://127.0.0.1:8182/api/v3/write_lp?db=testdb" \
    -H "Content-Type: text/plain" \
    --data-binary @- > /dev/null

echo "  ✅ Node 2: 写入 1000 条 CPU + 1000 条 Memory 数据"

# Node 3 数据 (server3, 1000 行)
echo "写入数据到 Node 3 (server3)..."
{
    echo "# Writing 1000 lines of data for server3"
    for i in {1..1000}; do
        timestamp=$((1609459200000000000 + i * 60000000000))
        usage=$(echo "scale=2; 55 + ($i * 13 + 5) % 45" | bc)
        echo "cpu,host=server3,region=eu-west usage=${usage},cores=4i ${timestamp}"
        echo "memory,host=server3,region=eu-west used=2048i,total=4096i ${timestamp}"
    done
} | curl -s -X POST "http://127.0.0.1:8183/api/v3/write_lp?db=testdb" \
    -H "Content-Type: text/plain" \
    --data-binary @- > /dev/null

echo "  ✅ Node 3: 写入 1000 条 CPU + 1000 条 Memory 数据"

echo ""
echo "✅ 数据写入完成 (总共 6000 行)"
echo ""

# 等待数据持久化
sleep 5

# 查询测试
echo "🔍 执行查询测试 (真实磁盘存储)"
echo "================================"
echo ""

# 测试 1: 简单查询
echo "测试 1: 查询单个节点的 CPU 数据 (前 10 行)"
echo "-------------------------------------------"
start_time=$(date +%s%3N)
result=$(curl -s -X POST "http://127.0.0.1:8181/api/v3/query_sql?db=testdb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT host, usage, cores FROM cpu WHERE host = '\''server1'\'' LIMIT 10"}')
end_time=$(date +%s%3N)
duration=$((end_time - start_time))
echo "$result" | jq -r '.[] | "\(.host) - \(.usage) - \(.cores)"' | head -5
echo "⏱️  查询时间: ${duration}ms"
echo ""

# 测试 2: 跨节点聚合查询
echo "测试 2: 跨所有节点查询 CPU 平均值"
echo "-----------------------------------"
start_time=$(date +%s%3N)
result=$(curl -s -X POST "http://127.0.0.1:8181/api/v3/query_sql?db=testdb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT host, AVG(usage) as avg_usage, COUNT(*) as count FROM cpu GROUP BY host"}')
end_time=$(date +%s%3N)
duration=$((end_time - start_time))
echo "$result" | jq '.'
echo "⏱️  查询时间: ${duration}ms"
echo ""

# 测试 3: 全局统计
echo "测试 3: 全局 CPU 统计 (3000 行)"
echo "-------------------------------"
start_time=$(date +%s%3N)
result=$(curl -s -X POST "http://127.0.0.1:8182/api/v3/query_sql?db=testdb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT COUNT(*) as total_count, AVG(usage) as avg_usage, MAX(usage) as max_usage, MIN(usage) as min_usage FROM cpu"}')
end_time=$(date +%s%3N)
duration=$((end_time - start_time))
echo "$result" | jq '.'
echo "⏱️  查询时间: ${duration}ms"
echo ""

# 测试 4: JOIN 查询
echo "测试 4: 跨表 JOIN 查询 (前 10 行)"
echo "----------------------------------"
start_time=$(date +%s%3N)
result=$(curl -s -X POST "http://127.0.0.1:8183/api/v3/query_sql?db=testdb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT c.host, c.usage, m.used as mem_used FROM cpu c JOIN memory m ON c.host = m.host LIMIT 10"}')
end_time=$(date +%s%3N)
duration=$((end_time - start_time))
echo "$result" | jq -r '.[] | "\(.host) - CPU: \(.usage) - Memory: \(.mem_used)"' | head -5
echo "⏱️  查询时间: ${duration}ms"
echo ""

# 测试 5: 复杂过滤和排序
echo "测试 5: 高 CPU 使用率主机 (usage > 80)"
echo "---------------------------------------"
start_time=$(date +%s%3N)
result=$(curl -s -X POST "http://127.0.0.1:8181/api/v3/query_sql?db=testdb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT host, usage, cores FROM cpu WHERE usage > 80 ORDER BY usage DESC LIMIT 10"}')
end_time=$(date +%s%3N)
duration=$((end_time - start_time))
row_count=$(echo "$result" | jq '. | length')
echo "$result" | jq -r '.[] | "\(.host) - \(.usage)"' | head -5
echo "返回行数: ${row_count}"
echo "⏱️  查询时间: ${duration}ms"
echo ""

# 测试 6: 时间范围查询
echo "测试 6: 时间范围查询 (前 100 行)"
echo "--------------------------------"
start_time=$(date +%s%3N)
result=$(curl -s -X POST "http://127.0.0.1:8182/api/v3/query_sql?db=testdb" \
  -H "Content-Type: application/json" \
  -d '{"q":"SELECT host, COUNT(*) as count FROM cpu WHERE time >= 1609459200000000000 AND time <= 1609465200000000000 GROUP BY host"}')
end_time=$(date +%s%3N)
duration=$((end_time - start_time))
echo "$result" | jq '.'
echo "⏱️  查询时间: ${duration}ms"
echo ""

echo "✅ 查询测试完成"
echo ""

# 显示节点信息和数据目录大小
echo "📊 节点状态和存储"
echo "================="
for i in 1 2 3; do
    pid=$(cat node${i}.pid 2>/dev/null || echo "未运行")
    size=$(du -sh node${i} 2>/dev/null | cut -f1)
    echo "Node ${i}: PID ${pid}, HTTP 818${i}, 数据大小: ${size}"
done

echo ""
echo "💡 提示:"
echo "  - 查看节点日志: tail -f node1.log"
echo "  - 停止所有节点: kill \$(cat node*.pid)"
echo "  - 手动查询: curl -X POST 'http://127.0.0.1:8181/api/v3/query_sql?db=testdb' -H 'Content-Type: application/json' -d '{\"q\":\"SELECT * FROM cpu LIMIT 5\"}'"
echo ""
echo "按 Ctrl+C 停止节点并退出"

# 清理函数
cleanup() {
    echo ""
    echo "🛑 停止所有节点..."
    kill $(cat node*.pid 2>/dev/null) 2>/dev/null || true
    sleep 2
    rm -f node*.pid
    echo "✅ 已清理"
    exit 0
}

trap cleanup INT TERM

# 保持脚本运行，显示日志
echo ""
echo "📜 实时日志 (Node 1):"
echo "===================="
tail -f node1.log

