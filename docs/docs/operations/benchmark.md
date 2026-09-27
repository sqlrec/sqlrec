# 性能测试

本页记录仓库中 `benchmark/movielens/` 压测的执行方法和一次历史测试结果。数字只反映特定硬件、数据和部署配置，不应视为其他环境的性能保证。

## 测试覆盖什么

压测调用 `main_rec` API，覆盖以下在线处理：

- 全局热门、用户兴趣类目、ItemCF 和 Milvus 向量检索四路召回；
- 曝光去重和多路结果合并；
- 关联物品信息并按类目打散；
- 异步写入 Kafka 推荐日志；
- 写入 Redis 曝光记录。

`init.sh` 会执行 `init_model.sql`，完成召回和排序模型的训练、导出及服务部署。加载物品特征时调用 DSSM 物品塔服务并将向量写入 Milvus。这些准备步骤不计入 `wrk` 压测时间；默认压测请求不调用在线模型服务。

## 测试数据和流程

测试使用 MovieLens-1M 数据集：

| 项目 | 规模 |
|------|------|
| 用户 | 6,040 |
| 物品 | 3,706 部电影 |
| 评分 | 约 100 万条 |
| 向量维度 | 64 |

推荐流程定义在 `benchmark/movielens/init_sqlrec_sql.sql`。

| 环节 | 默认行为 |
|------|----------|
| 全局热门召回 | 从 `global_hot_item` 取 300 条 |
| 兴趣类目召回 | 根据 `user_interest_genre` 查询 `genre_hot_item`，取 300 条 |
| ItemCF 召回 | 根据 `user_recent_click_item` 查询 `itemcf_i2i`，取 300 条 |
| 向量召回 | 用本地随机用户向量查询 Milvus，取 300 条 |
| 曝光去重 | 过滤最近 1 小时内已曝光的物品 |
| 排序 | 默认使用 `rank_fun_simple` 关联物品信息，不进行模型推理 |
| 打散 | 窗口大小 3，同类目最多 1 条，最终返回 10 条 |

`request.lua` 显式设置 `use_recall_service=false` 和 `rank_fun=rank_fun_simple`，固定压测口径。需要测试在线模型时，将请求 `params` 设为 `{"use_recall_service":"true","rank_fun":"rank_fun"}`，此时会调用 DSSM 用户塔和 Wide & Deep 排序服务。默认流程中的随机用户向量用于性能测试，不用于评估推荐质量。

## 环境前提

先按[服务部署](./deployment.md)启动完整测试环境。`init.sh` 还会使用或安装 `wrk`、启动 Kyuubi，并从网络下载 MovieLens 数据和 Python 依赖。

历史测试所用环境：

| 项目 | 配置 |
|------|------|
| CPU | AMD Ryzen 5600H |
| 内存 | 32 GB DDR4 |
| 操作系统 | Debian 12 |
| 部署 | Minikube、SQLRec 单实例 |

## 初始化数据

在项目根目录执行：

```bash
cd benchmark/movielens
bash init.sh
```

初始化脚本与 `mvn test` 共用仓库根目录的 `.venv`。脚本会在需要时创建环境并安装 benchmark 依赖；如需指定创建环境所用的 Python（需 3.10 或更新），可设置 `PYTHON_BOOTSTRAP`。

脚本会执行：

1. 部署 Kyuubi 并准备 `wrk`。
2. 创建 Milvus `item_embedding` collection 和索引。
3. 下载 MovieLens-1M，转换为 Parquet 并上传到 HDFS。
4. 创建离线表和在线 Connector 表。
5. 通过 Spark SQL 计算热门、兴趣类目和 ItemCF 特征。
6. 训练、导出并部署 Wide & Deep 和 DSSM 模型。
7. 将特征写入 Redis，将模型生成的物品向量写入 Milvus。
8. 注册 SQL 函数和 `main_rec` API。
9. 通过 Beeline 分别调用默认流程和模型流程，做基本验证。

`init.sh` 会修改测试环境中的 HDFS、Redis、Milvus、Kafka 和 SQLRec 元数据，不要对共享或生产环境执行。

## 执行压测

```bash
cd benchmark/movielens
bash benchmark.sh
```

当前脚本配置为：

- 预热：1 个线程、1 个连接、10 秒；
- 正式测试：10 个线程、10 个连接、30 秒；
- 接口：`/api/v1/main_rec`；
- 用户 ID：每次请求在有效的 MovieLens 用户 ID 1–6040 中随机生成。

以仓库中 `benchmark.sh` 和 `request.lua` 的当前内容为准。如果修改并发数或持续时间，应在保存结果时一并记录。

## 历史测试结果

以下结果来自此前使用模拟物品向量、且用户 ID 范围不同的配置。当前初始化会导入模型生成的物品向量，因此两者不能直接比较。

```text
Running 30s test @ http://192.168.49.2:30001/api/v1/main_rec
  10 threads and 10 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     6.73ms    3.16ms  90.29ms   94.46%
    Req/Sec   151.20     16.58   191.00     73.67%
  45231 requests in 30.02s, 87.90MB read
Requests/sec:   1506.47
Transfer/sec:      2.93MB
```

| 指标 | 值 | 说明 |
|------|----|------|
| 平均延迟 | 6.73 ms | wrk 请求延迟 |
| 延迟标准差 | 3.16 ms | wrk 统计 |
| 最大延迟 | 90.29 ms | 该次测试观测值 |
| 每线程 Req/Sec | 151.20 | `Thread Stats` 中的平均值，不是总 QPS |
| 总请求数 | 45,231 | 30.02 秒内 |
| 总 QPS | 1,506.47 | `Requests/sec` |
| 传输速率 | 2.93 MB/s | `Transfer/sec` |

比较两次结果时，至少保证数据、SQLRec 版本、JVM、并发配置、Connector 部署和是否启用模型服务一致。
