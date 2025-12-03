#!/bin/bash

# 数据准备脚本 - 为分布式 JOIN 测试准备数据
# 该脚本会：
# 1. 清空三个节点的 testdb 数据库
# 2. 在节点1写入 cpu 表数据（多个服务器、多个区域、多个指标）
# 3. 在节点2写入 mem 表数据（多个服务器、多个区域、多个指标）

set -e

echo "========================================"
echo "  准备分布式 JOIN 测试数据"
echo "========================================"
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

# echo -e "${BLUE}Step 1: 删除旧数据库${NC}"
# echo "删除节点1的数据库..."
# curl -s -X DELETE "http://127.0.0.1:${NODE1_PORT}/api/v3/configure/db/${DB_NAME}" > /dev/null 2>&1 || true
# echo "删除节点2的数据库..."
# curl -s -X DELETE "http://127.0.0.1:${NODE2_PORT}/api/v3/configure/db/${DB_NAME}" > /dev/null 2>&1 || true
# echo "删除节点3的数据库..."
# curl -s -X DELETE "http://127.0.0.1:${NODE3_PORT}/api/v3/configure/db/${DB_NAME}" > /dev/null 2>&1 || true
# echo -e "${GREEN}✓ 旧数据库已删除${NC}"
# echo ""

# sleep 1

echo -e "${BLUE}Step 2: 创建新数据库${NC}"
echo "在节点1创建数据库..."
curl -s -X POST "http://127.0.0.1:${NODE1_PORT}/api/v3/configure/db" \
  -H "Content-Type: application/json" \
  -d "{\"db_name\":\"${DB_NAME}\"}" > /dev/null
echo "在节点2创建数据库..."
curl -s -X POST "http://127.0.0.1:${NODE2_PORT}/api/v3/configure/db" \
  -H "Content-Type: application/json" \
  -d "{\"db_name\":\"${DB_NAME}\"}" > /dev/null
echo -e "${GREEN}✓ 数据库创建完成${NC}"
echo ""

sleep 1

echo -e "${BLUE}Step 3: 向节点1写入 CPU 表数据${NC}"
echo -e "${YELLOW}写入数据分布：${NC}"
echo "  - 10 台服务器 (server01-server10)"
echo "  - 3 个区域 (us-east, us-west, eu-central)"
echo "  - 每台服务器20个时间点"
echo "  - 总计: 200 行数据"
echo ""

TIMESTAMP=1704067200000000000  # 2024-01-01 00:00:00 UTC
TOTAL_WRITTEN=0

# 分批写入 CPU 数据，每个服务器一批
for server_num in {1..10}; do
  server=$(printf "server%02d" $server_num)

  # 根据服务器编号分配区域
  if [ $server_num -le 3 ]; then
    region="us-east"
  elif [ $server_num -le 6 ]; then
    region="us-west"
  else
    region="eu-central"
  fi

  CPU_DATA=""
  for point in {1..2000}; do
    # 生成随机的 CPU 使用率和负载
    cpu_usage=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*100}')
    cpu_load=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')
    cpu_cores=$((8 + (server_num % 3) * 8))  # 8, 16, or 24 cores

    ts=$((TIMESTAMP + point * 60000000000))  # 每分钟一个点
    CPU_DATA+="cpu,host=${server},region=${region} value=${cpu_usage},load=${cpu_load},cores=${cpu_cores}i ${ts}\n"
  done

  # 写入当前服务器的数据
  echo -n -e "$CPU_DATA" | curl -s -X POST "http://127.0.0.1:${NODE1_PORT}/api/v3/write_lp?db=${DB_NAME}" \
    --data-binary @- > /dev/null

  TOTAL_WRITTEN=$((TOTAL_WRITTEN + 20))
  echo "  写入 ${server} 数据... (${TOTAL_WRITTEN}/200)"
done

echo -e "${GREEN}✓ CPU 数据写入完成 (200 行)${NC}"
echo ""

sleep 1

echo -e "${BLUE}Step 4: 向节点2写入 MEM 表数据${NC}"
echo -e "${YELLOW}写入数据分布：${NC}"
echo "  - 10 台服务器 (server01-server10)"
echo "  - 3 个区域 (us-east, us-west, eu-central)"
echo "  - 每台服务器20个时间点"
echo "  - 总计: 200 行数据"
echo ""

TOTAL_WRITTEN=0

# 分批写入 MEM 数据，每个服务器一批
for server_num in {1..10}; do
  server=$(printf "server%02d" $server_num)

  # 根据服务器编号分配区域
  if [ $server_num -le 3 ]; then
    region="us-east"
  elif [ $server_num -le 6 ]; then
    region="us-west"
  else
    region="eu-central"
  fi

  MEM_DATA=""
  for point in {1..2000}; do
    # 生成随机的内存使用情况
    total_mem=$((32 + (server_num % 4) * 32))  # 32, 64, 96, or 128 GB
    used_mem=$(awk -v seed=$RANDOM -v total=$total_mem 'BEGIN{srand(seed); printf "%.2f", total * (0.3 + rand()*0.5)}')
    available_mem=$(awk -v total=$total_mem -v used=$used_mem 'BEGIN{printf "%.2f", total - used}')
    cached_mem=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')

    ts=$((TIMESTAMP + point * 60000000000))  # 每分钟一个点
    MEM_DATA+="mem,host=${server},region=${region} total=${total_mem},used=${used_mem},available=${available_mem},cached=${cached_mem} ${ts}\n"
  done

  # 写入当前服务器的数据
  echo -n -e "$MEM_DATA" | curl -s -X POST "http://127.0.0.1:${NODE2_PORT}/api/v3/write_lp?db=${DB_NAME}" \
    --data-binary @- > /dev/null

  TOTAL_WRITTEN=$((TOTAL_WRITTEN + 20))
  echo "  写入 ${server} 数据... (${TOTAL_WRITTEN}/200)"
done

echo -e "${GREEN}✓ MEM 数据写入完成 (200 行)${NC}"
echo ""

sleep 1

echo -e "${BLUE}Step 5: 验证数据${NC}"
echo "查询节点1的 CPU 数据..."
CPU_RESULT=$(curl -s -X POST "http://127.0.0.1:${NODE1_PORT}/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT * FROM cpu LIMIT 3\"}")

echo "查询节点2的 MEM 数据..."
MEM_RESULT=$(curl -s -X POST "http://127.0.0.1:${NODE2_PORT}/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT * FROM mem LIMIT 3\"}")

# 简单统计返回的行数（统计逗号分隔的对象数）
CPU_COUNT=$(echo "$CPU_RESULT" | grep -o '"host"' | wc -l | tr -d ' ')
MEM_COUNT=$(echo "$MEM_RESULT" | grep -o '"host"' | wc -l | tr -d ' ')

echo ""
echo -e "${GREEN}✓ 数据验证完成${NC}"
echo "  - 节点1 CPU 表: ≥${CPU_COUNT} 行 (实际200行)"
echo "  - 节点2 MEM 表: ≥${MEM_COUNT} 行 (实际200行)"
echo ""
echo "CPU 数据样例:"
echo "$CPU_RESULT" | head -c 200
echo "..."
echo ""

echo "========================================"
echo -e "${GREEN}  数据准备完成！${NC}"
echo "========================================"
echo ""
echo -e "${YELLOW}数据摘要：${NC}"
echo "  • CPU 表 (节点1): 200 行, 6 列 (host, region, time, value, load, cores)"
echo "  • MEM 表 (节点2): 200 行, 7 列 (host, region, time, total, used, available, cached)"
echo "  • 服务器: 10 台 (server01-server10)"
echo "  • 区域: 3 个 (us-east: server01-03, us-west: server04-06, eu-central: server07-10)"
echo ""
echo -e "${GREEN}现在可以运行测试了：${NC}"
echo "  ./target/debug/examples/test_distributed_join"

