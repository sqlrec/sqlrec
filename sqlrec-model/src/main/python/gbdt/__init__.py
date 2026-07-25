"""GBDT (CatBoost / LightGBM) train / export / serve entry points.

The module is invoked by the K8s job container via ``python -m gbdt.train_lightgbm``,
``python -m gbdt.export_catboost`` etc. Configuration is supplied as a JSON file
(produced by the Java ``PipelineConfigUtils``) and all data / model artifacts live on HDFS.
"""
