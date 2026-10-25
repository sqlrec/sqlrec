package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;
import com.sqlrec.model.common.ModelConfigUtils;
import java.util.Map;

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

    /** Generates training config from the prepared model and merged, validated training options. */
    protected abstract String generateTrainConfig(ModelConf model, ModelTrainConf trainConf);

    /** Generates export config from the prepared model and merged export options. */
    protected abstract String generateExportConfig(ModelConf model, ModelExportConf exportConf);

    @Override
    public String genModelTrainK8sYaml(ModelConf model, ModelTrainConf trainConf) {
        model = prepareModel(model);
        trainConf.setParams(ModelConfigUtils.mergeParams(model.getParams(), trainConf.getParams()));
        TzrecK8sYamlUtils.prepareRuntime(trainConf.getParams());
        TrainingOptions.validate(model, trainConf.getParams());
        TrainingOptions.validateModelDir(trainConf.getParams(), trainConf.getModelDir());
        String pipelineConfig = generateTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(trainConf);
        return TzrecK8sYamlUtils.genJobYaml(pipelineConfig, shell, trainConf.getId(), trainConf.getParams());
    }

    @Override
    public String genModelExportK8sYaml(ModelConf model, ModelExportConf exportConf) {
        model = prepareModel(model);
        Map<String, String> params = ModelConfigUtils.mergeParams(model.getParams(), exportConf.getParams());
        TzrecK8sYamlUtils.prepareRuntime(params);
        params.put("nnodes", "1");
        params.put("nproc_per_node", "1");
        exportConf.setParams(params);
        String exportDir = exportConf.getBaseModelDir() + "_export";
        String pipelineConfig = generateExportConfig(model, exportConf);
        String shell = ShellUtils.genExportModelShell(exportDir);
        return TzrecK8sYamlUtils.genJobYaml(pipelineConfig, shell, exportConf.getId(), params);
    }

    @Override
    public String getServiceUrl(ModelConf model, ServiceConf serviceConf) {
        serviceConf.setParams(ModelConfigUtils.mergeParams(model.getParams(), serviceConf.getParams()));
        return TzrecK8sYamlUtils.getServiceUrl(serviceConf);
    }

    @Override
    public String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf) {
        Map<String, String> params = ModelConfigUtils.mergeParams(model.getParams(), serviceConf.getParams());
        TzrecK8sYamlUtils.prepareRuntime(params);
        if (Config.REPLICAS.getValue(params) < 1) throw new IllegalArgumentException("replicas must be positive");
        serviceConf.setParams(params);
        return TzrecK8sYamlUtils.getServiceK8sYaml(serviceConf, params);
    }

    private ModelConf prepareModel(ModelConf source) {
        ModelConf model = new ModelConf();
        model.setModelName(source.getModelName());
        model.setInputFields(source.getInputFields());
        model.setPath(source.getPath());
        model.setDdl(source.getDdl());
        model.setParams(ModelConfigUtils.mergeParams(source.getParams(), Map.of("model", getModelName())));
        String error = checkModel(model);
        if (error != null) throw new IllegalArgumentException(error);
        return model;
    }

}
