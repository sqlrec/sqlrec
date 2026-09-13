# 扩展自定义模型

当内置模型无法满足需求时，可以通过 `ModelController` SPI 接入新的训练、导出和在线服务后端。这是面向 Java 扩展开发者的能力，使用现有模型时不需要理解本页内容。

## 开发前先确定生命周期

实现之前先回答以下问题：

- `TRAIN MODEL` 是否需要 `ON data_source`？
- 训练会生成几个 origin checkpoint？
- 是否需要 `EXPORT MODEL`？一次导出会生成几个 checkpoint？
- Service 允许使用 origin 还是 export checkpoint？
- 模型服务的 URL、输入 JSON 和输出 JSON 分别是什么？
- 训练、导出和服务所需的镜像、存储和 Kubernetes 资源是什么？

内置的 `external`、Hugging Face、tzrec 和 GBDT 实现覆盖了几种不同生命周期，建议选择最接近的实现作为起点。

## ModelController 接口

当前接口位于 `sqlrec-common` 模块：

```java
package com.sqlrec.common.model;

public interface ModelController {
    String getModelName();

    List<FieldSchema> getOutputFields(ModelConf model);

    String checkModel(ModelConf model);

    String genModelTrainK8sYaml(
        ModelConf model,
        ModelTrainConf trainConf
    );

    default boolean requiresTrainingData() {
        return true;
    }

    List<String> getExportCheckpoints(ModelExportConf exportConf);

    String getExportCleanPath(ModelExportConf exportConf);

    String genModelExportK8sYaml(
        ModelConf model,
        ModelExportConf exportConf
    );

    String getServiceUrl(ModelConf model, ServiceConf serviceConf);

    String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf);

    default String validateServiceCheckpointType(String checkpointType) {
        if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpointType)) {
            return "service only supports export checkpoint";
        }
        return null;
    }
}
```

| 方法 | 实现要点 |
|------|------------|
| `getModelName()` | 返回 `CREATE MODEL ... WITH (model = '...')` 使用的类型名 |
| `getOutputFields()` | 返回推理服务的输出字段，必须与服务响应一致 |
| `checkModel()` | 校验建模属性；有效时返回 `null`，否则返回错误信息 |
| `requiresTrainingData()` | 返回 `TRAIN MODEL` 是否必须有 `ON` 数据源；默认为 `true` |
| `genModelTrainK8sYaml()` | 生成训练或模型准备任务的 Kubernetes YAML |
| `getExportCheckpoints()` | 返回一次导出会产生的 checkpoint 名称 |
| `getExportCleanPath()` | 返回导出前需要清理的路径 |
| `genModelExportK8sYaml()` | 生成导出任务的 Kubernetes YAML |
| `getServiceUrl()` | 返回 `call_service` 实际请求的 URL |
| `getServiceK8sYaml()` | 生成在线服务的 Kubernetes YAML |
| `validateServiceCheckpointType()` | 检查 Service 能否使用指定类型的 checkpoint |

`ModelConf`、`ModelTrainConf`、`ModelExportConf` 和 `ServiceConf` 会提供已解析的模型、训练、导出及服务信息。请直接依赖当前版本的 `sqlrec-common`，不要在扩展项目中复制这些数据类。

## 定义配置项

使用 `ConfigOption` 定义属性名、默认值和类型：

```java
public final class MyModelOptions {
    public static final ConfigOption<String> IMAGE = new ConfigOption<>(
        "image",
        "example/my-model",
        "Training and serving image",
        null,
        String.class
    );

    private MyModelOptions() {
    }
}
```

在 `checkModel()` 中对必填值、取值范围和互斥配置做完整检查，让用户在 `CREATE MODEL` 时就得到清晰错误，不要把配置错误延迟到 Kubernetes Job 启动后才暴露。

## 处理特殊生命周期

### 训练不需要 SQL 数据表

如果 `TRAIN MODEL` 表示下载或准备模型，覆盖：

```java
@Override
public boolean requiresTrainingData() {
    return false;
}
```

这类模型可以使用不带 `ON` 的 `TRAIN MODEL`。Hugging Face 实现就采用此模式。

### Service 使用 origin checkpoint

默认实现只允许 export checkpoint。如果自定义服务可以直接加载 origin，必须覆盖 `validateServiceCheckpointType()` 并进行明确校验，不建议无条件接受任意字符串。

### 不支持训练或导出

参考 `ExternalModel` 的做法，在对应方法中拒绝不受支持的操作，并返回对用户有意义的错误。

## 注册 SPI

在扩展 JAR 中创建：

```text
src/main/resources/META-INF/services/com.sqlrec.common.model.ModelController
```

文件内每行写一个实现类的全限定名：

```text
com.example.sqlrec.model.MyModelController
```

将 JAR 及它的运行时依赖放入所有 SQLRec 服务实例的 classpath，然后重启服务。

## 使用自定义模型

```sql
CREATE MODEL my_model (
  feature1 VARCHAR,
  feature2 DOUBLE
) WITH (
  model = 'my_model_type',
  image = 'example/my-model'
);
```

`model` 的值必须与 `getModelName()` 的返回值一致。

## 发布前检查

- 用最小合法配置和各类非法配置测试 `checkModel()`。
- 验证训练和导出产物名称与 `getExportCheckpoints()` 一致。
- 确认生成的 Kubernetes 资源名称合法，且只使用需要的权限。
- 确认模型服务输出字段与 `getOutputFields()` 完全一致。
- 用 `CREATE MODEL`、`TRAIN MODEL`、`EXPORT MODEL`、`CREATE SERVICE` 和 `call_service` 做端到端测试，不只测试 YAML 字符串生成。

参考实现位于 `sqlrec-model/src/main/java/com/sqlrec/model/`。接口会随项目演进，开发扩展时应以当前源码为准。
