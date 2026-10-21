-- recall-service-item: serves the item tower of recall_model.
create service `recall-service-item` on model recall_model checkpoint='v1_export/item'
with (
'NAMESPACE'='sqlrec'
);
