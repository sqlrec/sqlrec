CREATE TABLE IF NOT EXISTS `item_embedding` (
  `id` BIGINT,
  `title` STRING,
  `genres` ARRAY<STRING>,
  `embedding` ARRAY<DOUBLE>,
  PRIMARY KEY (id)  NOT ENFORCED
) WITH (
  'connector' = 'milvus',
  'url' = 'http://192.168.1.5:31530',
  'token' = 'root:Milvus',
  'database' = 'default',
  'collection' = 'item_embedding'
);
