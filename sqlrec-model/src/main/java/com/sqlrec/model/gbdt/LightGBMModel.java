package com.sqlrec.model.gbdt;

import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;

/**
 * LightGBM-based GBDT model controller.
 *
 * <p>Model name: {@code gbdt.lightgbm}. Training/export/serving are delegated to the
 * {@code gbdt} python entry points via {@link GbdtModelBase}.
 */
public class LightGBMModel extends GbdtModelBase {

    public LightGBMModel() {
        super(ModelType.LIGHTGBM);
    }

    @Override
    protected String getModelNameSuffix() {
        return "lightgbm";
    }
}
