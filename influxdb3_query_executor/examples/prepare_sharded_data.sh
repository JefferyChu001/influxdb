#!/bin/bash

# 分片数据准备脚本 - 为真正的分布式测试准备数据
# 该脚本会：
# 1. 在3个节点上分片写入 cpu 表数据（基于 host 哈希分片）
# 2. 在3个节点上分片写入 mem 表数据（基于 host 哈希分片）
#
# 数据分布（基于简单哈希）：
#   节点1: server01-03 的数据
#   节点2: server04-06 的数据  
#   节点3: server07-10 的数据

set -e

# 参数配置
TOTAL_ROWS=${1:-10000}  # 默认10000行（每张表）
NUM_SERVERS=10          # 服务器数量
POINTS_PER_SERVER=$((TOTAL_ROWS / NUM_SERVERS))

echo "========================================"
echo "  准备分片分布式测试数据"
echo "========================================"
echo ""
echo "配置参数："
echo "  • 每表总行数: $TOTAL_ROWS"
echo "  • 服务器数: $NUM_SERVERS"
echo "  • 每服务器数据点: $POINTS_PER_SERVER"
echo ""
echo "数据分布策略（基于 host 哈希）："
echo "  • 节点1 (8181): server01, server02, server03"
echo "  • 节点2 (8182): server04, server05, server06"
echo "  • 节点3 (8183): server07, server08, server09, server10"
echo ""

# 节点配置
NODE1_PORT=8181
NODE2_PORT=8182
NODE3_PORT=8183
DB_NAME="testdb"

# 颜色输出
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 简单哈希函数：根据 server 编号分配节点
# server01-03 -> 节点1
# server04-06 -> 节点2
# server07-10 -> 节点3
get_node_port() {
    local server_num=$1
    if [ $server_num -le 3 ]; then
        echo $NODE1_PORT
    elif [ $server_num -le 6 ]; then
        echo $NODE2_PORT
    else
        echo $NODE3_PORT
    fi
}

echo -e "${BLUE}Step 1: 清理旧数据（每个节点清理各自的表）${NC}"
for port in $NODE1_PORT $NODE2_PORT $NODE3_PORT; do
    echo "清理节点 $port 的 cpu 表..."
    curl -s -X POST "http://127.0.0.1:${port}/api/v3/query_sql" \
      -H "Content-Type: application/json" \
      -d "{\"db\":\"${DB_NAME}\",\"query\":\"DROP TABLE IF EXISTS cpu\"}" > /dev/null 2>&1 || true
    
    echo "清理节点 $port 的 mem 表..."
    curl -s -X POST "http://127.0.0.1:${port}/api/v3/query_sql" \
      -H "Content-Type: application/json" \
      -d "{\"db\":\"${DB_NAME}\",\"query\":\"DROP TABLE IF EXISTS mem\"}" > /dev/null 2>&1 || true
done
echo -e "${GREEN}✓ 旧数据已清理${NC}"
echo ""

sleep 1

echo -e "${BLUE}Step 2: 确保所有节点的数据库存在${NC}"
for port in $NODE1_PORT $NODE2_PORT $NODE3_PORT; do
    curl -s -X POST "http://127.0.0.1:${port}/api/v3/configure/db" \
      -H "Content-Type: application/json" \
      -d "{\"db_name\":\"${DB_NAME}\"}" > /dev/null 2>&1 || true
done
echo -e "${GREEN}✓ 数据库已准备就绪${NC}"
echo ""

sleep 1

TIMESTAMP=1704067200000000000  # 2024-01-01 00:00:00 UTC

echo -e "${BLUE}Step 3: 分片写入 CPU 表数据${NC}"
echo ""

for server_num in $(seq 1 $NUM_SERVERS); do
    server=$(printf "server%02d" $server_num)
    port=$(get_node_port $server_num)
    
    # 根据服务器编号分配区域
    if [ $server_num -le 3 ]; then
        region="us-east"
    elif [ $server_num -le 6 ]; then
        region="us-west"
    else
        region="eu-central"
    fi
    
    echo "生成 $server 的 CPU 数据 (写入节点端口 $port)..."
    CPU_DATA=""
    
    for point in $(seq 1 $POINTS_PER_SERVER); do
        # 生成随机的 CPU 使用率和负载
        cpu_usage=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*100}')
        cpu_load=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')
        cpu_cores=$((8 + (server_num % 3) * 8))  # 8, 16, or 24 cores

        ts=$((TIMESTAMP + point * 60000000000))  # 每分钟一个点
        CPU_DATA+="cpu,host=${server},region=${region} value=${cpu_usage},load=${cpu_load},cores=${cpu_cores}i ${ts}\n"
    done
    
    # 写入到对应的节点
    echo -n -e "$CPU_DATA" | curl -s -X POST "http://127.0.0.1:${port}/api/v3/write_lp?db=${DB_NAME}" \
      --data-binary @- > /dev/null
    
    echo "  ✓ $server 的 $POINTS_PER_SERVER 行 CPU 数据已写入节点 $port"
done

echo ""
echo -e "${GREEN}✓ CPU 数据分片写入完成 (总计 $TOTAL_ROWS 行)${NC}"
echo ""

sleep 1

echo -e "${BLUE}Step 4: 分片写入 MEM 表数据${NC}"
echo ""

for server_num in $(seq 1 $NUM_SERVERS); do
    server=$(printf "server%02d" $server_num)
    port=$(get_node_port $server_num)

    # 根据服务器编号分配区域
    if [ $server_num -le 3 ]; then
        region="us-east"
    elif [ $server_num -le 6 ]; then
        region="us-west"
    else
        region="eu-central"
    fi

    echo "生成 $server 的 MEM 数据 (写入节点端口 $port)..."
    MEM_DATA=""

    for point in $(seq 1 $POINTS_PER_SERVER); do
        # 生成随机的内存使用情况
        total_mem=$((32 + (server_num % 4) * 32))  # 32, 64, 96, or 128 GB
        used_mem=$(awk -v seed=$RANDOM -v total=$total_mem 'BEGIN{srand(seed); printf "%.2f", total * (0.3 + rand()*0.5)}')
        available_mem=$(awk -v total=$total_mem -v used=$used_mem 'BEGIN{printf "%.2f", total - used}')
        cached_mem=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')

        ts=$((TIMESTAMP + point * 60000000000))  # 每分钟一个点
        MEM_DATA+="mem,host=${server},region=${region} total=${total_mem},used=${used_mem},available=${available_mem},cached=${cached_mem} ${ts}\n"
    done

    # 写入到对应的节点
    echo -n -e "$MEM_DATA" | curl -s -X POST "http://127.0.0.1:${port}/api/v3/write_lp?db=${DB_NAME}" \
      --data-binary @- > /dev/null

    echo "  ✓ $server 的 $POINTS_PER_SERVER 行 MEM 数据已写入节点 $port"
done

echo ""
echo -e "${GREEN}✓ MEM 数据分片写入完成 (总计 $TOTAL_ROWS 行)${NC}"
echo ""

sleep 2

echo -e "${BLUE}Step 5: 验证分片数据分布${NC}"
echo ""

for port in $NODE1_PORT $NODE2_PORT $NODE3_PORT; do
    echo "节点 $port 的数据统计:"

    CPU_COUNT=$(curl -s -X POST "http://127.0.0.1:${port}/api/v3/query_sql" \
      -H "Content-Type: application/json" \
      -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT COUNT(*) as count FROM cpu\"}" 2>/dev/null | grep -o '"count":[0-9]*' | grep -o '[0-9]*' || echo "0")

    MEM_COUNT=$(curl -s -X POST "http://127.0.0.1:${port}/api/v3/query_sql" \
      -H "Content-Type: application/json" \
      -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT COUNT(*) as count FROM mem\"}" 2>/dev/null | grep -o '"count":[0-9]*' | grep -o '[0-9]*' || echo "0")

    echo "  • CPU 表: $CPU_COUNT 行"
    echo "  • MEM 表: $MEM_COUNT 行"
    echo ""
done

echo "========================================"
echo -e "${GREEN}  分片数据准备完成！${NC}"
echo "========================================"
echo ""
echo -e "${YELLOW}数据分布摘要：${NC}"
echo "  • CPU 表总行数: $TOTAL_ROWS (分布在 3 个节点)"
echo "  • MEM 表总行数: $TOTAL_ROWS (分布在 3 个节点)"
echo "  • 服务器: $NUM_SERVERS 台"
echo "  • 分片策略: 基于 host 哈希"
echo ""
echo -e "${GREEN}现在可以运行分布式测试了：${NC}"
echo "  cargo run --example test_distributed_join_sharded"
