# Extending SQLRec with a Custom Model

When the built-in backends do not fit, implement the `ModelController` SPI to integrate a new training, export, and online-serving backend. Regular model users do not need this extension API.

## Define the Lifecycle First

Before implementation, decide:

- Does `TRAIN MODEL` require an `ON data_source` clause?
- How many `origin` checkpoints does training produce?
- Is `EXPORT MODEL` supported, and how many checkpoints does it produce?
- Can a Service use an `origin` checkpoint, or does it require `export`?
- What are the Service URL and its input/output JSON contracts?
- Which images, storage, and Kubernetes resources are required?

The built-in `external`, Hugging Face, tzrec, and GBDT implementations cover several different lifecycles. Start from the closest one.

## The `ModelController` Interface

The current interface is in `sqlrec-common`:

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

| Method | Responsibility |
|--------|----------------|
| `getModelName()` | Return the type used by `CREATE MODEL ... WITH (model = '...')` |
| `getOutputFields()` | Return output fields that exactly match the Service response |
| `checkModel()` | Return `null` for a valid model or a useful validation error |
| `requiresTrainingData()` | Say whether `TRAIN MODEL` requires `ON`; defaults to `true` |
| `genModelTrainK8sYaml()` | Generate Kubernetes YAML for training or preparation |
| `getExportCheckpoints()` | Return the checkpoint names produced by one export |
| `getExportCleanPath()` | Return the path to clean before export |
| `genModelExportK8sYaml()` | Generate Kubernetes YAML for export |
| `getServiceUrl()` | Return the URL used by `call_service` |
| `getServiceK8sYaml()` | Generate online-Service Kubernetes YAML |
| `validateServiceCheckpointType()` | Validate the checkpoint type accepted by a Service |

`ModelConf`, `ModelTrainConf`, `ModelExportConf`, and `ServiceConf` provide parsed configuration. Depend on the current `sqlrec-common` module instead of copying these classes into an extension.

## Define Options

Use `ConfigOption` to define each name, default, and type:

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

Validate required values, ranges, and conflicting settings in `checkModel()`. Users should see configuration errors during `CREATE MODEL`, not after a Kubernetes Job starts.

## Handle a Special Lifecycle

### Training Does Not Use a SQL Table

If training actually downloads or prepares a model, override:

```java
@Override
public boolean requiresTrainingData() {
    return false;
}
```

The model can then use `TRAIN MODEL` without `ON`. The Hugging Face backend follows this pattern.

### A Service Uses an Origin Checkpoint

The default accepts only `export`. If the backend can serve `origin`, override `validateServiceCheckpointType()` with an explicit check. Avoid accepting arbitrary values.

### Training or Export Is Unsupported

Follow `ExternalModel`: reject the unsupported operation in the corresponding method and return an actionable message.

## Register the SPI

Create this file in the extension JAR:

```text
src/main/resources/META-INF/services/com.sqlrec.common.model.ModelController
```

Add one fully qualified implementation class per line:

```text
com.example.sqlrec.model.MyModelController
```

Put the JAR and runtime dependencies on every SQLRec service instance's classpath, then restart the service.

## Use the Model

```sql
CREATE MODEL my_model (
  feature1 VARCHAR,
  feature2 DOUBLE
) WITH (
  model = 'my_model_type',
  image = 'example/my-model'
);
```

The `model` value must equal `getModelName()`.

## Release Checklist

- Test `checkModel()` with the minimum valid configuration and each invalid case.
- Verify that produced checkpoint names match `getExportCheckpoints()`.
- Ensure generated Kubernetes resource names are valid and permissions are minimal.
- Make Service outputs match `getOutputFields()` exactly.
- Run an end-to-end test with `CREATE MODEL`, `TRAIN MODEL`, `EXPORT MODEL`, `CREATE SERVICE`, and `call_service`; testing YAML generation alone is insufficient.

Reference implementations are under `sqlrec-model/src/main/java/com/sqlrec/model/`. Extension interfaces may evolve, so use the current source as authoritative.
