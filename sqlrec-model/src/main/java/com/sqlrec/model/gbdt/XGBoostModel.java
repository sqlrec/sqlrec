package com.sqlrec.model.gbdt;

import com.sqlrec.model.gbdt.PipelineConfigUtils.ModelType;

/**
 * XGBoost-based GBDT model controller.
 *
 * <p>Model name: {@code gbdt.xgboost}. Training/export/serving are delegated to the
 * {@code gbdt} python entry points via {@link GbdtModelBase}.
 */
public class XGBoostModel extends GbdtModelBase {

    public XGBoostModel() {
        super(ModelType.XGBOOST);
    }

    @Override
    protected String getModelNameSuffix() {
        return "xgboost";
    }
}
