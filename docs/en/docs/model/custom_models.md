# Extending Custom Models

You can extend custom models by implementing the `ModelController` interface.

## ModelController Interface

`ModelController` is the core interface for all model controllers, defining model lifecycle management methods.

### Interface Definition

```java
public interface ModelController {
    String getModelName();
    List<FieldSchema> getOutputFields(ModelConfig model);
    String checkModel(ModelConfig model);
    String genModelTrainK8sYaml(ModelConfig model, ModelTrainConf trainConf);
    List<String> getExportCheckpoints(ModelExportConf exportConf);
    String genModelExportK8sYaml(ModelConfig model, ModelExportConf exportConf);
    String getServiceUrl(ModelConfig model, ServiceConfig serviceConf);
    String getServiceK8sYaml(ModelConfig model, ServiceConfig serviceConf);
}
```

### Method Description

| Method | Description |
|--------|-------------|
| `getModelName()` | Return model type name, used to identify model type |
| `getOutputFields(ModelConfig)` | Return model output field list |
| `checkModel(ModelConfig)` | Check if model configuration is valid, return null if valid |
| `genModelTrainK8sYaml(ModelConfig, ModelTrainConf)` | Generate training task Kubernetes YAML |
| `getExportCheckpoints(ModelExportConf)` | Get exported checkpoint name list |
| `genModelExportK8sYaml(ModelConfig, ModelExportConf)` | Generate export task Kubernetes YAML |
| `getServiceUrl(ModelConfig, ServiceConfig)` | Get service access URL |
| `getServiceK8sYaml(ModelConfig, ServiceConfig)` | Generate service deployment Kubernetes YAML |

## Implementation Steps

### 1. Create Model Configuration Class (optional)

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

### 2. Implement ModelController Interface

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

### 3. Register Model Controller (SPI method)

Create file `com.sqlrec.common.model.ModelController` in `src/main/resources/META-INF/services/` directory, file content is the fully qualified name of the implementation class:

```
com.example.MyModel
```

## Using Custom Models

```sql
CREATE MODEL my_custom_model (
    feature1 VARCHAR,
    feature2 DOUBLE
) WITH (
    model = 'my_model',
    custom_param = 'value'
);
```
