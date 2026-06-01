create service recall_service_item on model recall_model checkpoint='v1_export/item'
with (
'NAMESPACE'='sqlrec'
);