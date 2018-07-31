DROP TABLE IF EXISTS `t1`;
CREATE TABLE `t1` (
  `name` varchar(12) DEFAULT NULL,
  KEY `pname` (`name`(12))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
insert into t1 values('借款策略集_网页');