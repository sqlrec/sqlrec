package com.sqlrec.model.gbdt;

import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;

/**
 * CatBoost-based GBDT model controller.
 *
 * <p>Model name: {@code gbdt.catboost}. Training/export/serving are delegated to the
 * {@code gbdt} python entry points via {@link GbdtModelBase}.
 */
public class CatBoostModel extends GbdtModelBase {

    public CatBoostModel() {
        super(ModelType.CATBOOST);
    }

    @Override
    protected String getModelNameSuffix() {
        return "catboost";
    }
}
