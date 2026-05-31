create service recall_service_user on model recall_model_user
with (
'url'='http://recall-service-user.sqlrec.svc.cluster.local:80/predict'
);
