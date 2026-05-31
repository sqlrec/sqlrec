create service rank_service on model rank_model
with (
'url'='http://rank-service.sqlrec.svc.cluster.local:80/predict'
);
