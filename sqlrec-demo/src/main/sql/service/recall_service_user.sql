create service recall_service_user on model recall_model checkpoint='v1_export/user'
with (
'NAMESPACE'='sqlrec'
);