-- rank_service: serves the rank_model for online inference.
create service rank_service on model rank_model checkpoint='v1_export'
with (
'NAMESPACE'='sqlrec'
);
