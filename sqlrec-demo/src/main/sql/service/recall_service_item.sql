create service recall_service_item on model recall_model_item
with (
'url'='http://recall-service-item.sqlrec.svc.cluster.local:80/predict'
);
