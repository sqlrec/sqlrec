# GBDT 模型

LightGBM、XGBoost 用于数值特征，CatBoost 还支持类别特征。执行步骤见[模型指南](../../guides/model-lifecycle.md)，Pod 参数见[通用资源配置](./builtin-models.md#通用流程与资源配置)。

## LightGBM 模型

LightGBM 模型是基于 GBDT（梯度提升决策树）框架实现的模型，支持完整的训练、导出和服务部署流程。训练数据和模型文件使用配置的存储，导出时转换为 ONNX 格式用于在线推理。

LightGBM 训练特征先转换为 float32，与 ONNX 和在线服务使用同样的精度；保留 NaN 缺失值，拒绝 Infinity 或 float32 溢出。旧版使用 double 训练的模型需要重新训练，才能保证这项精度一致性。

**模型名称**：`gbdt.lightgbm`

GBDT 的 TRAIN、EXPORT 和 SERVICE 可覆盖运行参数及资源配置，但不能改变 Model 声明的模型类型或 `label_columns`；显式传入相同值允许执行，传入不同值会报错。改变模型类型或标签须新建模型。这项规则适用于 LightGBM、XGBoost 和 CatBoost。

GBDT 在线推理的缺失数值字段和 `null` 在服务内部转换为 float32 `NaN`，供模型按缺失值处理，与训练时一致；`0` 是有效数值。客户端传 `null` 或省略字段即可，JSON 中不要发送 `NaN`。数值字符串须是完整的有限 float32 十进制数，非法值返回 HTTP 400。服务继承模型声明中的镜像、版本和资源配置，服务参数可覆盖这些配置；模型下载、加载完成且 `/health` 探针成功后才接收流量。此行为也适用于 XGBoost 和 CatBoost。

`POST /predict` 接收非空的对象行数组或列式 JSON；列式值必须为数组，长度须一致，长度为 1 的列可广播。JSON 解析失败、行元素不是对象、列值不是数组、列长度不兼容及空批次均返回 HTTP 400；模型执行抛出的运行时异常返回 HTTP 500。错误响应使用 JSON 格式，包含 `error` 字段。

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 二分类正类概率；回归预测值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 单个标签列名，不能包含逗号或首尾空白 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `num_iterations` | Integer | 300 | boosting 迭代次数 |
| `learning_rate` | Double | 0.1 | 学习率 |
| `num_leaves` | Integer | 63 | 每棵树最大叶子数 |
| `max_depth` | Integer | 6 | 最大树深度 |
| `feature_fraction` | Double | 0.8 | 每棵树使用的特征比例 |
| `bagging_fraction` | Double | 0.8 | 每棵树使用的数据比例 |
| `bagging_freq` | Integer | 5 | bagging 频率 |
| `min_data_in_leaf` | Integer | 20 | 叶子节点最小样本数 |
| `l2_regularization` | Double | 1.0 | L2 正则化系数 |

**使用示例**：

```sql
CREATE MODEL lgb_model (
    user_id FLOAT,
    age FLOAT,
    item_id FLOAT,
    item_price FLOAT,
    label INT
) WITH (
    'model' = 'gbdt.lightgbm',
    'label_columns' = 'label',
    'num_iterations' = '200',
    'learning_rate' = '0.05',
    'num_leaves' = '127'
);
```

## XGBoost 模型

XGBoost 模型是基于 GBDT（梯度提升决策树）框架实现的模型，支持完整的训练、导出和服务部署流程。训练数据和模型文件均存储在分布式存储上，导出时转换为 ONNX 格式用于在线推理。

**模型名称**：`gbdt.xgboost`

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 二分类正类概率；回归预测值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 单个标签列名，不能包含逗号或首尾空白 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `num_iterations` | Integer | 300 | boosting 迭代次数 |
| `learning_rate` | Double | 0.1 | 学习率 |
| `max_depth` | Integer | 6 | 最大树深度 |
| `feature_fraction` | Double | 0.8 | 每棵树使用的特征比例（对应 XGBoost colsample_bytree） |
| `bagging_fraction` | Double | 0.8 | 每棵树使用的数据比例（对应 XGBoost subsample） |
| `min_child_weight` | Integer | 1 | 子节点最小权重和 |
| `l2_regularization` | Double | 1.0 | L2 正则化系数（对应 XGBoost reg_lambda） |

**使用示例**：

```sql
CREATE MODEL xgb_model (
    user_id FLOAT,
    user_age FLOAT,
    item_id FLOAT,
    item_price FLOAT,
    label INT
) WITH (
    'model' = 'gbdt.xgboost',
    'label_columns' = 'label',
    'num_iterations' = '200',
    'learning_rate' = '0.05',
    'max_depth' = '8'
);
```

## CatBoost 模型

CatBoost 模型是基于 GBDT 框架实现的模型，原生支持类别特征处理，支持完整的训练、导出和服务部署流程。训练数据和模型文件使用配置的存储，导出原生 `.cbm` 格式用于在线推理。

**模型名称**：`gbdt.catboost`

训练和在线推理统一将整数类别值转换为十进制字符串，将空类别值转换为 `""`，保留大整数 ID 的精度。在线类别值须为整数、字符串或 `null`；浮点数、布尔值和数组返回 HTTP 400。

**输出字段**：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| `probs` | FLOAT | 二分类正类概率；回归预测值 |

**必需参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `label_columns` | String | 单个标签列名，不能包含逗号或首尾空白 |

**训练配置参数**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `objective` | String | "binary" | 学习目标（binary, regression） |
| `metric` | String | "auc" | 评估指标（auc, logloss, rmse） |
| `cb_iterations` | Integer | 1000 | CatBoost 迭代次数 |
| `cb_depth` | Integer | 6 | CatBoost 树深度 |
| `cb_l2_leaf_reg` | Double | 3.0 | L2 叶子正则化系数 |
| `learning_rate` | Double | 0.1 | 学习率 |

**使用示例**：

```sql
CREATE MODEL cb_model (
    user_id BIGINT,
    user_country VARCHAR,
    age INT,
    item_id BIGINT,
    item_category VARCHAR,
    label INT
) WITH (
    'model' = 'gbdt.catboost',
    'label_columns' = 'label',
    'cb_iterations' = '1000',
    'cb_depth' = '8',
    'learning_rate' = '0.03'
);
```
