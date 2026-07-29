# 扩展自定义模型

可以通过实现 `ModelController` 接口来扩展自定义模型。

## ModelController 接口

`ModelController` 是所有模型控制器的核心接口，定义了模型的生命周期管理方法。

### 接口定义

```java
public interface ModelController {
    String getModelName();
    List<FieldSchema> getOutputFields(ModelConfig model);
    String checkModel(ModelConfig model);
    String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf);
    List<String> getExportCheckpoints(ModelExportConf exportConf);
    String getExportCleanPath(ModelExportConf exportConf);
    String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf);
    String getServiceUrl(ModelConfig model, ServiceConfig serviceConf);
    String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf);

    // 校验检查点类型是否可用于部署服务，返回 null 表示有效
    default String validateServiceCheckpointType(String checkpointType) {
        if (!Consts.CHECKPOINT_TYPE_EXPORT.equals(checkpointType)) {
            return "service only supports export checkpoint";
        }
        return null;
    }
}
```

### 方法说明

| 方法 | 说明 |
|------|------|
| `getModelName()` | 返回模型类型名称，用于标识模型类型 |
| `getOutputFields(ModelConfig)` | 返回模型的输出字段列表 |
| `checkModel(ModelConfig)` | 检查模型配置是否有效，返回 null 表示有效 |
| `genModelTrainK8sYaml(ModelConfig, ModelTrainConf)` | 生成训练任务的 Kubernetes YAML |
| `getExportCheckpoints(ModelExportConf)` | 获取导出后的检查点名称列表 |
| `getExportCleanPath(ModelExportConf)` | 获取导出任务的清理路径 |
| `genModelExportK8sYaml(ModelConfig, ModelExportConf)` | 生成导出任务的 Kubernetes YAML |
| `getServiceUrl(ModelConfig, ServiceConfig)` | 获取服务的访问 URL |
| `getServiceK8sYaml(ModelConfig, ServiceConfig)` | 生成服务部署的 Kubernetes YAML |
| `validateServiceCheckpointType(String)` | 校验检查点类型是否可用于部署服务，返回 null 表示有效（默认实现要求 `export` 类型；外部模型豁免） |

## 实现步骤

### 1. 创建模型配置类（可选）

```java
public class MyModelConfig {
    public static final ConfigOption<String> CUSTOM_PARAM = new ConfigOption<>(
        "custom_param",
        "default_value", 
        "Custom parameter description", 
        null, 
        String.class
    );
}
```

### 2. 实现 ModelController 接口

```java
public class MyModel implements ModelController {
    
    @Override
    public String getModelName() {
        return "my_model";
    }
    
    @Override
    public List<FieldSchema> getOutputFields(ModelConfig model) {
        return Arrays.asList(
            new FieldSchema("prediction", "DOUBLE"),
            new FieldSchema("confidence", "DOUBLE")
        );
    }
    
    @Override
    public String checkModel(ModelConfig model) {
        return null;
    }
    
    @Override
    public String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf) {
        return "...";
    }
    
    @Override
    public List<String> getExportCheckpoints(ModelExportConf exportConf) {
        return Arrays.asList(exportConf.getCheckpointName() + "_export");
    }
    
    @Override
    public String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf) {
        return "...";
    }
    
    @Override
    public String getServiceUrl(ModelConfig model, ServiceConfig serviceConf) {
        return "http://" + serviceConf.getId() + ".svc.cluster.local:80/predict";
    }
    
    @Override
    public String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf) {
        return "...";
    }
}
```

### 3. 注册模型控制器（SPI方式）

在 `src/main/resources/META-INF/services/` 目录下创建文件 `com.sqlrec.common.model.ModelController`，文件内容为实现类的全限定名：

```
com.example.MyModel
```

## 使用自定义模型

```sql
CREATE MODEL my_custom_model (
    feature1 VARCHAR,
    feature2 DOUBLE
) WITH (
    model = 'my_model',
    custom_param = 'value'
);
```
