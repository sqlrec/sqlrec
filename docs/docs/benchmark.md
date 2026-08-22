# 性能测试

本文档介绍 SQLRec 的性能测试方法和测试结果。测试基于 [MovieLens-1M](https://grouplens.org/datasets/movielens/) 数据集，对应的脚本位于 `benchmark/movielens/` 目录。

## 测试环境

**硬件配置**：
- CPU: AMD Ryzen 5600H
- 内存: 32GB DDR4

**软件环境**：
- 操作系统: Debian 12
- Kubernetes: Minikube
- SQLRec: 单实例部署

## 测试数据

测试使用 MovieLens-1M 数据集，默认测试配置如下：

| 配置项 | 值 |
|--------|-----|
| 数据集 | MovieLens-1M |
| 用户数量 | 6040 |
| 物品数量 | 3706（电影） |
| 评分数据 | 约 100 万条 |
| 向量维度 | 64 维 |
| User Embedding | 每次请求通过 `random_vec` 随机生成（未启用召回模型服务时） |

## 推荐流程

测试的推荐流程为 `main_rec` 函数（定义见 `benchmark/movielens/init_sqlrec_sql.sql`），包含以下环节：

### 召回阶段

| 召回策略 | 说明 | 召回数量 |
|----------|------|----------|
| 全局高热召回 | 基于全局物品热度排序（`global_hot_item`） | 300 |
| 用户兴趣类目召回 | 基于用户兴趣类目召回高热物品（`user_interest_genre` + `genre_hot_item`） | 300 |
| ItemCF 召回 | 基于用户最近点击物品的协同过滤召回（`user_recent_click_item` + `itemcf_i2i`） | 300 |
| 向量检索召回 | 基于用户向量与物品向量（Milvus）的内积相似度检索 | 300 |

### 过滤与重排阶段

| 策略 | 说明 |
|------|------|
| 曝光去重 | 过滤用户最近 1 小时内已曝光的物品（`user_exposure_item`） |
| 排序 | 默认使用 `rank_fun_simple` 关联物品元信息；可通过 API 参数 `rank_fun` 指定基于 wide_and_deep 模型的排序函数 |
| 类目打散 | `window_diversify`，窗口大小 3，窗口内每个类目最多出现 1 次，最终返回 10 条 |

### 其他环节

- 生成请求元信息（`req_time`、`req_id`）
- 推荐日志异步写入 Kafka（`rec_log_kafka`）
- 推荐结果写入曝光表用于后续去重

## 测试脚本

### 初始化测试环境

```bash
cd benchmark/movielens
bash init.sh
```

`init.sh` 脚本执行以下操作：

1. **部署 Kyuubi**：用于后续离线特征计算

2. **安装测试工具**
   - 安装 wrk HTTP 压测工具

3. **创建 Milvus 向量集合**
   - 创建 `item_embedding` 集合
   - 定义向量维度为 64 维
   - 创建 COSINE 相似度索引（AUTOINDEX）

4. **下载并处理测试数据**
   - 下载 MovieLens-1M 数据集
   - 转换为 Parquet 格式（用户表、电影表、评分表）
   - 上传到 HDFS

5. **创建数据表**
   - 用户表 (`user_table`)、物品表 (`item_table`)：Redis
   - 全局高热物品表 (`global_hot_item`)：Redis
   - 用户兴趣类目表 (`user_interest_genre`)：Redis
   - 类目高热物品表 (`genre_hot_item`)：Redis
   - 用户最近点击表 (`user_recent_click_item`)：Redis
   - 用户曝光表 (`user_exposure_item`)：Redis
   - ItemCF I2I 表 (`itemcf_i2i`)：Redis
   - 物品向量表 (`item_embedding`)：Milvus
   - 推荐日志表 (`rec_log_kafka`)：Kafka

6. **计算离线特征**：通过 Kyuubi 执行 Spark SQL，计算全局高热物品、用户兴趣类目、类目高热物品、ItemCF I2I 等离线特征表

7. **训练模型**：创建并训练 wide_and_deep 排序模型（`rank_model`）和 DSSM 双塔召回模型（`recall_model`），导出并部署为在线服务

8. **加载特征数据**：将离线特征加载到 Redis，通过 `batch_call_service` 调用召回模型服务生成物品向量并写入 Milvus

9. **注册 SQL 函数和 API**：注册召回、排序、打散、落库等 SQL 函数，并创建 `main_rec` API

10. **测试推荐**：通过 beeline 调用 `main_rec` 验证推荐流程

### 执行性能测试

```bash
cd benchmark/movielens
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

`request.lua` 是 wrk 的自定义请求脚本，为每次请求生成随机用户 ID，并随机打印部分响应用于校验：

```lua
-- Set random seed
math.randomseed(os.time())

function request()
    -- Generate random ID between 0-5000
    local random_id = math.random(0, 5000)

    -- Construct request body
    local request_body = string.format('{"data":{"user_info":[{"user_id":%d}]},"params":{"recall_fun":"recall_fun"}}', random_id)

    -- Configure HTTP request
    wrk.method = "POST"
    wrk.headers["Content-Type"] = "application/json"
    wrk.body = request_body

    return wrk.format()
end

-- Response handler to print response if the corresponding request was logged
function response(status, headers, body)
    current_request_log = (math.random(1, 100) == 1)
    if current_request_log then
        print("Response:")
        print("Status: " .. status)
        print("Body: " .. body)
    end
end
```

请求体中的 `params` 会设置为执行上下文变量，例如 `recall_fun` 指定召回函数名，`rank_fun` 指定排序函数名。

## 测试结果

在 AMD Ryzen 5600H、32GB DDR4 内存机器上的测试结果：

```
Running 30s test @ http://192.168.49.2:30001/api/v1/main_rec
  10 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     9.23ms    5.04ms  48.96ms   90.50%
    Req/Sec   111.59     17.07   151.00     59.47%
  33370 requests in 30.02s, 57.91MB read
  Socket errors: connect 0, read 33369, write 0, timeout 0
Requests/sec:   1111.47
Transfer/sec:      1.93MB
```

**性能指标**：

| 指标 | 值 |
|------|-----|
| 平均延迟 | 9.23ms |
| 延迟标准差 | 5.04ms |
| 最大延迟 | 48.96ms |
| 平均 QPS | 111.59 |
| 总请求数 | 33,370 |
| 总 QPS | 1111.47 |
| 吞吐量 | 1.93MB/s |
