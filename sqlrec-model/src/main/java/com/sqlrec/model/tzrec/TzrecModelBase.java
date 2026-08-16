package com.sqlrec.model.tzrec;

import com.sqlrec.common.model.ModelConf;
import com.sqlrec.common.model.ModelController;
import com.sqlrec.common.model.ModelExportConf;
import com.sqlrec.common.model.ModelTrainConf;
import com.sqlrec.common.model.ServiceConf;

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

    @Override
    public String genModelTrainK8sYaml(ModelConf model, ModelTrainConf trainConf) {
        String pipelineConfig = generateTrainConfig(model, trainConf);
        String shell = ShellUtils.genTrainModelShell(model, trainConf);
        return TzrecK8sYamlUtils.genJobYaml(pipelineConfig, shell, trainConf.getId(), trainConf.getParams());
    }

    @Override
    public String genModelExportK8sYaml(ModelConf model, ModelExportConf exportConf) {
        String exportDir = exportConf.getBaseModelDir() + "_export";
        String pipelineConfig = generateExportConfig(model, exportConf);
        String shell = ShellUtils.genExportModelShell(model, exportConf, exportDir);
        return TzrecK8sYamlUtils.genJobYaml(pipelineConfig, shell, exportConf.getId(), exportConf.getParams());
    }

    @Override
    public String getServiceUrl(ModelConf model, ServiceConf serviceConf) {
        return TzrecK8sYamlUtils.getServiceUrl(serviceConf);
    }

    @Override
    public String getServiceK8sYaml(ModelConf model, ServiceConf serviceConf) {
        return TzrecK8sYamlUtils.getServiceK8sYaml(serviceConf);
    }
}
