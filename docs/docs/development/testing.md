# 开发与测试

本文面向修改源码或验证扩展的开发者。部署服务见[服务部署](../operations/deployment.md)，日常 SQL 使用见[客户端连接](../guides/clients.md)。以下命令在仓库根目录执行。

## 构建与本地运行

构建服务及依赖模块：

```bash
mvn package -pl sqlrec-frontend -am -DskipTests
SQL_SCHEMA_DIR="$PWD/sqlrec-demo/src/main/sql" bash bin/cli.sh
```

文件模式不依赖 HMS 或 Minikube；执行具体查询时仍需可用的 Connector 和 UDF JAR。CLI 日志默认位于 `sqlrec-frontend/target/logs`。

使用远程元数据时，设置 `HIVE_METASTORE_URI`、`META_DB_URL` 及数据库凭证。源码 CLI 未配置这些地址时，会尝试使用 `DEFAULT_TEST_IP`、`NODE_IP` 或 Minikube 地址生成开发环境默认值；显式设置（包括空值）会保留。

## 测试已有环境

```bash
bash bin/test.sh -pl sqlrec-core -am
```

脚本运行 Maven 集成测试，其余参数原样传给 Maven。测试地址优先使用 `DEFAULT_TEST_IP`，其次 `NODE_IP`，均未设置时从 Minikube 获取。可覆盖各组件连接配置和 `HADOOP_CONF_DIR`。Maven 使用当前终端的 JDK，Hive/Hadoop 客户端使用各自配置。

## 重建环境并执行完整测试

```bash
bash bin/test.sh -all
```

`--all` 与 `-all` 等效。此模式需要已有可访问的 Minikube 环境保存镜像缓存，随后会**删除并重建名为 `minikube` 的集群及其节点动态卷数据**。宿主机镜像缓存和客户端目录保留；镜像保存失败时不删除集群。

流程包括部署依赖、等待服务就绪、Maven 集成测试，以及 SQL 性能、GrowthBook、Hugging Face、模型效果 benchmark。任一步骤失败会停止。脚本使用配置的发布镜像，不构建本地源码镜像；验证代码改动时应先构建镜像并配置对应版本。运行需要外网、部署权限及足够的存储和计算资源。

## 查看报告

完整测试的报告和各阶段日志位于 `benchmark/runs/` 中本次运行的目录，结束时会打印路径；失败的运行也生成报告。设置 `TEST_REPORT_ROOT` 可更改父目录。

- `summary.md`：阶段状态、测试和 benchmark 结果。
- `logs/`：每个阶段的命令输出。
- `growthbook/`、`huggingface/`、`model_quality/`：对应 benchmark 的详细产物。

单独运行 benchmark 的选项见各自 README：

- [SQL 性能](https://github.com/sqlrec/sqlrec/blob/master/benchmark/sql_performance/README.md)
- [GrowthBook](https://github.com/sqlrec/sqlrec/blob/master/benchmark/growthbook/README.md)
- [Hugging Face](https://github.com/sqlrec/sqlrec/blob/master/benchmark/huggingface/README.md)
- [模型效果](https://github.com/sqlrec/sqlrec/blob/master/benchmark/model_quality/README.md)

## 文档预览与检查

```bash
npm ci
npm run docs:dev
npm run docs:build
```

文档源文件位于 `docs/docs/` 和 `docs/en/docs/`。调整页面位置时同步更新中英文链接和 `docs/.vitepress/config.mts`，不要编辑生成的 `.vitepress/dist` 或缓存文件。
