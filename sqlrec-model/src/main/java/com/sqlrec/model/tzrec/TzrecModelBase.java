package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.model.common.ModelConfigUtils;

/**
 * Shared {@link ModelController} implementation for TZRec models (DSSM / WideAndDeep).
 *
 * <p>Mirrors {@code com.sqlrec.model.gbdt.GbdtModelBase}: concrete subclasses only provide a
 * model name suffix (e.g. {@code dssm}, {@code wide_and_deep}) and the model-specific
 * pipeline.config generation. Train/export Job YAML and serving Deployment/Service YAML are
 * identical across TZRec models and live here.
 */
public abstract class TzrecModelBase implements ModelController {

    protected abstract String getModelNameSuffix();

    @Override
    public final String getModelName() {
        return "tzrec." + getModelNameSuffix();
    }

    /** Generates the model-specific pipeline.config content for training. */
    protected abstract String generateTrainConfig(ModelConf model, ModelTrainConf trainConf);

    /** Generates the model-specific pipeline.config content for export. */
    protected abstract String generateExportConfig(ModelConf model, ModelExportConf exportConf);

    /** Validate operation overrides before merging them into the model declaration. */
    protected void checkOverrides(ModelConf model, java.util.Map<String, String> overrides) {}

    @Override
    public String genModelTrainK8sYaml(ModelConf model, ModelTrainConf trainConf) {
        requireSameModel(trainConf.getParams());
        checkOverrides(model, trainConf.getParams());
        model = PipelineConfigUtils.effectiveModel(model, trainConf.getParams());
        requireValid(model);
        String pipelineConfig = generateTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(model, trainConf);
        return TzrecK8sYamlUtils.genJobYaml(pipelineConfig, shell, trainConf.getId(), model.getParams());
    }

    @Override
    public String genModelExportK8sYaml(ModelConf model, ModelExportConf exportConf) {
        requireSameModel(exportConf.getParams());
        checkOverrides(model, exportConf.getParams());
        if (Config.NNODES.getValue(exportConf.getParams()) != 1 || Config.NPROC_PER_NODE.getValue(exportConf.getParams()) != 1) {
            throw new IllegalArgumentException("TZRec export requires a single process");
        }
        model = PipelineConfigUtils.effectiveModel(model, exportConf.getParams());
        model = PipelineConfigUtils.effectiveModel(model, java.util.Map.of("nnodes", "1", "nproc_per_node", "1"));
        requireValid(model);
        String exportDir = exportConf.getBaseModelDir() + "_export";
        String pipelineConfig = generateExportConfig(model, exportConf);
        String shell = ShellUtils.genExportModelShell(model, exportConf, exportDir);
        return TzrecK8sYamlUtils.genJobYaml(pipelineConfig, shell, exportConf.getId(), model.getParams());
    }

    @Override
    public String getServiceUrl(ModelConf model, ServiceConf serviceConf) {
        return TzrecK8sYamlUtils.getServiceUrl(serviceConf);
    }

    @Override
    public String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf) {
        checkOverrides(model, serviceConf.getParams());
        return TzrecK8sYamlUtils.getServiceK8sYaml(serviceConf,
                ModelConfigUtils.mergeParams(model.getParams(), serviceConf.getParams()));
    }

    private void requireValid(ModelConf model) {
        String error = checkModel(model);
        if (error != null) throw new IllegalArgumentException(error);
    }

    private void requireSameModel(java.util.Map<String, String> params) {
        if (params != null && params.containsKey("model") && !getModelName().equals(params.get("model"))) {
            throw new IllegalArgumentException("Training/export cannot change the model type; create a new model instead");
        }
    }
}
