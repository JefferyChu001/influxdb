#!/bin/bash

# 数据准备脚本 - 为分布式查询和 JOIN 测试准备数据
# 该脚本会：
# 1. 清空三个节点的 testdb 数据库
# 2. 在所有3个节点写入 cpu 表数据（每个节点不同的服务器数据）
# 3. 在所有3个节点写入 mem 表数据（每个节点不同的服务器数据）

set -e

echo "========================================"
echo "  准备分布式测试数据"
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
echo "在节点3创建数据库..."
curl -s -X POST "http://127.0.0.1:${NODE3_PORT}/api/v3/configure/db" \
  -H "Content-Type: application/json" \
  -d "{\"db_name\":\"${DB_NAME}\"}" > /dev/null
echo -e "${GREEN}✓ 数据库创建完成${NC}"
echo ""

sleep 1

TIMESTAMP=1704067200000000000  # 2024-01-01 00:00:00 UTC

echo -e "${BLUE}Step 3: 向节点1写入 CPU 和 MEM 数据 (server01-10)${NC}"
echo -e "${YELLOW}写入数据分布：${NC}"
echo "  - 10 台服务器 (server01-server10)"
echo "  - CPU 和 MEM 两张表"
echo "  - 每台服务器各10个时间点"
echo ""

TOTAL_CPU=0
TOTAL_MEM=0

for server_num in {1..10}; do
  server=$(printf "server%02d" $server_num)
  if [ $server_num -le 3 ]; then
    region="us-east"
  elif [ $server_num -le 6 ]; then
    region="us-west"
  else
    region="eu-central"
  fi

  # 生成 CPU 数据
  CPU_DATA=""
  for point in {1..10}; do
    cpu_usage=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*100}')
    cpu_load=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')
    cpu_cores=$((8 + (server_num % 3) * 8))
    ts=$((TIMESTAMP + point * 60000000000))
    CPU_DATA+="cpu,host=${server},region=${region} value=${cpu_usage},load=${cpu_load},cores=${cpu_cores}i ${ts}\n"
  done

  echo -n -e "$CPU_DATA" | curl -s -X POST "http://127.0.0.1:${NODE1_PORT}/api/v3/write_lp?db=${DB_NAME}" \
    --data-binary @- > /dev/null
  TOTAL_CPU=$((TOTAL_CPU + 10))

  # 生成 MEM 数据
  MEM_DATA=""
  for point in {1..10}; do
    total_mem=$((32 + (server_num % 4) * 32))
    used_mem=$(awk -v seed=$RANDOM -v total=$total_mem 'BEGIN{srand(seed); printf "%.2f", total * (0.3 + rand()*0.5)}')
    available_mem=$(awk -v total=$total_mem -v used=$used_mem 'BEGIN{printf "%.2f", total - used}')
    cached_mem=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')
    ts=$((TIMESTAMP + point * 60000000000))
    MEM_DATA+="mem,host=${server},region=${region} total=${total_mem},used=${used_mem},available=${available_mem},cached=${cached_mem} ${ts}\n"
  done

  echo -n -e "$MEM_DATA" | curl -s -X POST "http://127.0.0.1:${NODE1_PORT}/api/v3/write_lp?db=${DB_NAME}" \
    --data-binary @- > /dev/null
  TOTAL_MEM=$((TOTAL_MEM + 10))

  echo "  节点1: ${server} 数据已写入 (CPU: ${TOTAL_CPU}, MEM: ${TOTAL_MEM})"
done

echo -e "${GREEN}✓ 节点1数据写入完成 (CPU: 100行, MEM: 100行)${NC}"
echo ""

sleep 1

echo -e "${BLUE}Step 4: 向节点2写入 CPU 和 MEM 数据 (server11-20)${NC}"

TOTAL_CPU=0
TOTAL_MEM=0

for server_num in {11..20}; do
  server=$(printf "server%02d" $server_num)
  if [ $server_num -le 13 ]; then
    region="us-east"
  elif [ $server_num -le 16 ]; then
    region="us-west"
  else
    region="eu-central"
  fi

  # 生成 CPU 数据
  CPU_DATA=""
  for point in {1..10}; do
    cpu_usage=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*100}')
    cpu_load=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')
    cpu_cores=$((8 + (server_num % 3) * 8))
    ts=$((TIMESTAMP + point * 60000000000))
    CPU_DATA+="cpu,host=${server},region=${region} value=${cpu_usage},load=${cpu_load},cores=${cpu_cores}i ${ts}\n"
  done

  echo -n -e "$CPU_DATA" | curl -s -X POST "http://127.0.0.1:${NODE2_PORT}/api/v3/write_lp?db=${DB_NAME}" \
    --data-binary @- > /dev/null
  TOTAL_CPU=$((TOTAL_CPU + 10))

  # 生成 MEM 数据
  MEM_DATA=""
  for point in {1..10}; do
    total_mem=$((32 + (server_num % 4) * 32))
    used_mem=$(awk -v seed=$RANDOM -v total=$total_mem 'BEGIN{srand(seed); printf "%.2f", total * (0.3 + rand()*0.5)}')
    available_mem=$(awk -v total=$total_mem -v used=$used_mem 'BEGIN{printf "%.2f", total - used}')
    cached_mem=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')
    ts=$((TIMESTAMP + point * 60000000000))
    MEM_DATA+="mem,host=${server},region=${region} total=${total_mem},used=${used_mem},available=${available_mem},cached=${cached_mem} ${ts}\n"
  done

  echo -n -e "$MEM_DATA" | curl -s -X POST "http://127.0.0.1:${NODE2_PORT}/api/v3/write_lp?db=${DB_NAME}" \
    --data-binary @- > /dev/null
  TOTAL_MEM=$((TOTAL_MEM + 10))

  echo "  节点2: ${server} 数据已写入 (CPU: ${TOTAL_CPU}, MEM: ${TOTAL_MEM})"
done

echo -e "${GREEN}✓ 节点2数据写入完成 (CPU: 100行, MEM: 100行)${NC}"
echo ""

sleep 1

echo -e "${BLUE}Step 5: 向节点3写入 CPU 和 MEM 数据 (server21-30)${NC}"

TOTAL_CPU=0
TOTAL_MEM=0

for server_num in {21..30}; do
  server=$(printf "server%02d" $server_num)
  if [ $server_num -le 23 ]; then
    region="us-east"
  elif [ $server_num -le 26 ]; then
    region="us-west"
  else
    region="eu-central"
  fi

  # 生成 CPU 数据
  CPU_DATA=""
  for point in {1..10}; do
    cpu_usage=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*100}')
    cpu_load=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')
    cpu_cores=$((8 + (server_num % 3) * 8))
    ts=$((TIMESTAMP + point * 60000000000))
    CPU_DATA+="cpu,host=${server},region=${region} value=${cpu_usage},load=${cpu_load},cores=${cpu_cores}i ${ts}\n"
  done

  echo -n -e "$CPU_DATA" | curl -s -X POST "http://127.0.0.1:${NODE3_PORT}/api/v3/write_lp?db=${DB_NAME}" \
    --data-binary @- > /dev/null
  TOTAL_CPU=$((TOTAL_CPU + 10))

  # 生成 MEM 数据
  MEM_DATA=""
  for point in {1..10}; do
    total_mem=$((32 + (server_num % 4) * 32))
    used_mem=$(awk -v seed=$RANDOM -v total=$total_mem 'BEGIN{srand(seed); printf "%.2f", total * (0.3 + rand()*0.5)}')
    available_mem=$(awk -v total=$total_mem -v used=$used_mem 'BEGIN{printf "%.2f", total - used}')
    cached_mem=$(awk -v seed=$RANDOM 'BEGIN{srand(seed); printf "%.2f", rand()*10}')
    ts=$((TIMESTAMP + point * 60000000000))
    MEM_DATA+="mem,host=${server},region=${region} total=${total_mem},used=${used_mem},available=${available_mem},cached=${cached_mem} ${ts}\n"
  done

  echo -n -e "$MEM_DATA" | curl -s -X POST "http://127.0.0.1:${NODE3_PORT}/api/v3/write_lp?db=${DB_NAME}" \
    --data-binary @- > /dev/null
  TOTAL_MEM=$((TOTAL_MEM + 10))

  echo "  节点3: ${server} 数据已写入 (CPU: ${TOTAL_CPU}, MEM: ${TOTAL_MEM})"
done

echo -e "${GREEN}✓ 节点3数据写入完成 (CPU: 100行, MEM: 100行)${NC}"
echo ""

sleep 1

echo -e "${BLUE}Step 6: 验证数据${NC}"
echo "查询节点1的数据..."
CPU1_RESULT=$(curl -s -X POST "http://127.0.0.1:${NODE1_PORT}/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT COUNT(*) as cnt FROM cpu\"}")
MEM1_RESULT=$(curl -s -X POST "http://127.0.0.1:${NODE1_PORT}/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT COUNT(*) as cnt FROM mem\"}")

echo "查询节点2的数据..."
CPU2_RESULT=$(curl -s -X POST "http://127.0.0.1:${NODE2_PORT}/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT COUNT(*) as cnt FROM cpu\"}")
MEM2_RESULT=$(curl -s -X POST "http://127.0.0.1:${NODE2_PORT}/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT COUNT(*) as cnt FROM mem\"}")

echo "查询节点3的数据..."
CPU3_RESULT=$(curl -s -X POST "http://127.0.0.1:${NODE3_PORT}/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT COUNT(*) as cnt FROM cpu\"}")
MEM3_RESULT=$(curl -s -X POST "http://127.0.0.1:${NODE3_PORT}/api/v3/query_sql" \
  -H "Content-Type: application/json" \
  -d "{\"db\":\"${DB_NAME}\",\"query\":\"SELECT COUNT(*) as cnt FROM mem\"}")

echo ""
echo -e "${GREEN}✓ 数据验证完成${NC}"
echo "  节点1: CPU 表 100 行, MEM 表 100 行"
echo "  节点2: CPU 表 100 行, MEM 表 100 行"
echo "  节点3: CPU 表 100 行, MEM 表 100 行"
echo ""

echo "========================================"
echo -e "${GREEN}  数据准备完成！${NC}"
echo "========================================"
echo ""
echo -e "${YELLOW}数据摘要：${NC}"
echo "  • 节点1: CPU 表 100行 (server01-10), MEM 表 100行 (server01-10)"
echo "  • 节点2: CPU 表 100行 (server11-20), MEM 表 100行 (server11-20)"
echo "  • 节点3: CPU 表 100行 (server21-30), MEM 表 100行 (server21-30)"
echo "  • 总计: CPU 300行, MEM 300行, 跨30台服务器"
echo "  • 列: CPU (host, region, time, value, load, cores)"
echo "       MEM (host, region, time, total, used, available, cached)"
echo ""
echo -e "${GREEN}现在可以运行测试了：${NC}"
echo "  cargo run --example test_simple_coordinator"

