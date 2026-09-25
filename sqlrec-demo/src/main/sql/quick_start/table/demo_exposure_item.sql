-- user_id is the lookup key used by demo_rec; each user can have many exposures.
CREATE TABLE IF NOT EXISTS `demo_exposure_item` (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `bhv_time` BIGINT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'filesystem'
);
