-- Quick-start exposure history starts empty and is updated in memory by demo_rec.
CREATE TABLE IF NOT EXISTS `demo_exposure_item` (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
