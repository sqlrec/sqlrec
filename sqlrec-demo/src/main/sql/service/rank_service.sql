create service rank_service on model rank_model checkpoint='v1_export'
with (
'NAMESPACE'='sqlrec'
);