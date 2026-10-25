package com.sqlrec.model.tzrec;

/** Explicit DeepFM backend. */
public class DeepFMModel extends WideAndDeepModel {
    @Override
    protected String getModelNameSuffix() {
        return "deepfm";
    }
}
