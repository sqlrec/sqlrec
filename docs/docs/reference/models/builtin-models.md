# 内置模型

根据输入数据和任务选择模型后端：

| 后端 | 主要用途 | 输入 | 输出 |
| --- | --- | --- | --- |
| [TZRec](./tzrec.md) | 召回、排序、多目标预测 | 类别与数值特征 | 概率、预测值或双塔向量 |
| [GBDT](./gbdt.md) | 树模型排序与回归 | 数值特征；CatBoost 也支持类别特征 | `probs` |
| [Hugging Face](./huggingface.md) | 文本分类、生成、文本和图片向量 | 文本或图片 URL | 标签、文本或向量 |
| [external](#外部模型) | 接入已有 HTTP 推理服务 | 服务声明的字段 | 配置的输出字段 |

创建、训练、导出及调用步骤见[模型训练与在线推理](../../guides/model-lifecycle.md)。

## 通用流程与资源配置

Model、训练、导出和 Service 的 `WITH` 选项使用字符串键和值，例如 `'batch_size' = '8192'`；SQLRec 会按参数类型解析值。

训练型模型按[模型指南](../../guides/model-lifecycle.md#训练型模型的完整流程)执行创建、训练、导出和服务部署。训练型模型的示例以 Model 定义为主，`training_sample` 等训练表需另行准备。TZRec 的模型结构放在 MODEL，训练参数放在 TRAIN，导出及服务资源分别在对应操作中设置。

下表的资源配置适用于 tzrec、GBDT 和 Hugging Face 自托管任务或服务；`replicas` 用于在线服务。

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `pod_cpu_cores` | Integer | 1 | Pod CPU 核数 |
| `pod_memory` | String | "2Gi" | Pod 内存 |
| `pod_cpu_limit` | String | - | Pod CPU 上限 |
| `pod_memory_limit` | String | - | Pod 内存上限 |
| `replicas` | Integer | 1 | 服务副本数 |

镜像默认值：tzrec 使用 `sqlrec/tzrec`，GBDT 使用 `sqlrec/gbdt`，版本为 `${SQLREC_VERSION}-cpu`；Hugging Face 使用 `sqlrec/transformers:${SQLREC_VERSION}`。可用 `image`、`version` 覆盖。

## 外部模型

外部模型用于对接已有的外部模型服务，不支持训练和导出操作。

**模型名称**：`external`

**配置参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `output_columns` | String | 输出列定义，格式：`name1:type1,name2:type2` |

`output_columns` 是 Model 配置；`url` 是 Service 配置，用于指定实际调用的推理地址。

**使用示例**：

```sql
CREATE MODEL external_model (
    user_id BIGINT,
    item_id BIGINT,
    category VARCHAR,
    price DOUBLE
) WITH (
    'model' = 'external',
    'output_columns' = 'score:FLOAT,label:VARCHAR'
);

CREATE SERVICE `external-service`
    ON MODEL external_model
    WITH (
        'url' = 'http://external-service:8080/predict'
    );
```
