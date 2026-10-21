-- recall-service-user: serves the user tower of recall_model.
create service `recall-service-user` on model recall_model checkpoint='v1_export/user'
with (
'NAMESPACE'='sqlrec'
);
