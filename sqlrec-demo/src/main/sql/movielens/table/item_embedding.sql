/*
 * Item embedding table - stored in Milvus vector database.
 * Multi-line block comment; the semicolon below must not split statements ;
 */
CREATE TABLE IF NOT EXISTS `item_embedding` (
  `id` BIGINT,
  `title` STRING,
  `genres` ARRAY<STRING>,
  `embedding` ARRAY<DOUBLE>,
  PRIMARY KEY (id)  NOT ENFORCED
) WITH (
  'connector' = 'milvus',
  'url' = 'http://${DEFAULT_TEST_IP}:30022',
  'token' = 'root:Milvus',
  'database' = 'default',
  'collection' = 'item_embedding'
); -- trailing line comment without newline
