-- recall_service_user: serves the user tower of recall_model.
create service recall_service_user on model recall_model checkpoint='v1_export/user'
with (
'NAMESPACE'='sqlrec'
);