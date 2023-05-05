CREATE schema  IF NOT EXISTS  `kafka-rebalancer`;

USE `kafka-rebalancer`;

USE public;

CREATE TABLE IF NOT EXISTS `test-data` (
  `id` bigint(20) NOT NULL auto_increment COMMENT 'id',
  `field1` varchar(64) NOT NULL,
  `value1` varchar(128) DEFAULT NULL,
  `uniq_field2` tinyint(4) NOT NULL DEFAULT 0,
  `create_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间' ,
  `update_timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  primary key (`id`),
  unique key idx_field2(`uniq_field2`)
) ;