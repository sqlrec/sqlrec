# 性能测试

本文档介绍 SQLRec 的性能测试方法和测试结果。

## 测试环境

**硬件配置**：
- CPU: AMD Ryzen 5600H
- 内存: 32GB DDR4

**软件环境**：
- 操作系统: Debian 12
- Kubernetes: Minikube
- SQLRec: 单实例部署

## 测试数据

默认测试配置如下：

| 配置项 | 值 |
|--------|-----|
| 用户数量 | 10万 |
| 物品数量 | 10万 |
| 向量维度 | 8维 |
| User Embedding | 固定值 |

## 推荐流程

测试的推荐流程包含以下环节：

### 召回阶段

| 召回策略 | 说明 | 召回数量 |
|----------|------|----------|
| 全局高热召回 | 基于全局物品热度排序 | 300 |
| 用户兴趣类目召回 | 基于用户兴趣类目召回高热物品 | 300 |
| ItemCF 召回 | 基于物品协同过滤召回 | 300 |
| 向量检索召回 | 基于向量相似度检索 | 300 |

### 过滤阶段

| 过滤策略 | 说明 |
|----------|------|
| 曝光去重 | 过滤用户已曝光的物品 |
| 类目打散 | 每个类目最多展示 N 个物品 |

## 测试脚本

### 初始化测试环境

```bash
cd benchmark
bash init.sh
```

`init.sh` 脚本执行以下操作：

1. **创建 Milvus 向量集合**
   - 创建 `item_embedding` 集合
   - 定义向量维度为 8 维
   - 创建 COSINE 相似度索引

2. **创建数据表**
   - 用户表 (`user_table`)
   - 物品表 (`item_table`)
   - 全局高热物品表 (`global_hot_item`)
   - 用户兴趣类目表 (`user_interest_category1`)
   - 类目高热物品表 (`category1_hot_item`)
   - 用户最近点击表 (`user_recent_click_item`)
   - 用户曝光表 (`user_exposure_item`)
   - ItemCF I2I 表 (`itemcf_i2i`)
   - 物品向量表 (`item_embedding`)
   - 推荐日志表 (`rec_log_kafka`)

3. **生成模拟数据**
   - 使用 Python 脚本生成 10 万用户和 10 万物品数据
   - 生成用户行为数据并上传到 HDFS

4. **安装测试工具**
   - 安装 wrk HTTP 压测工具

### 执行性能测试

```bash
bash benchmark.sh
```

`benchmark.sh` 脚本执行以下操作：

1. **预热阶段**
   - 单线程、单连接运行 10 秒
   - 预热系统缓存

2. **正式测试**
   - 并发数: 10
   - 持续时间: 30 秒
   - 测试 URL: `/api/v1/main_rec`

### 测试请求脚本

`request.lua` 是 wrk 的自定义请求脚本：

```lua
-- Set random seed
math.randomseed(os.time())

function request()
    -- Generate random ID between 0-99999
    local random_id = math.random(0, 99999)
    
    -- Construct request body
    local request_body = string.format(
        '{"inputs":{"user_info":[{"id":%d}]},"params":{"recall_fun":"recall_fun"}}',
        random_id
    )
    
    -- Configure HTTP request
    wrk.method = "POST"
    wrk.headers["Content-Type"] = "application/json"
    wrk.body = request_body
    
    return wrk.format()
end
```

## 测试结果

在 AMD Ryzen 5600H、32GB DDR4 内存机器上的测试结果：

```
Running 30s test @ http://192.168.49.2:30301/api/v1/main_rec
  10 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    10.80ms    4.81ms  53.85ms   92.68%
    Req/Sec    95.12     17.61   121.00     82.63%
  28464 requests in 30.02s, 49.80MB read
  Socket errors: connect 0, read 28463, write 0, timeout 0
Requests/sec:    948.09
Transfer/sec:      1.66MB
```

**性能指标**：

| 指标 | 值 |
|------|-----|
| 平均延迟 | 10.80ms |
| 延迟标准差 | 4.81ms |
| 最大延迟 | 53.85ms |
| 平均 QPS | 95.12 |
| 总请求数 | 28,464 |
| 总 QPS | 948.09 |
| 吞吐量 | 1.66MB/s |
