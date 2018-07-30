DROP TABLE IF EXISTS `prefix`;
CREATE TABLE `prefix` (
  `a` int(11) NOT NULL,
  `b` varchar(55) DEFAULT NULL,
  `c` int(11) DEFAULT NULL,
  PRIMARY KEY (`a`),
  KEY `prefix_index` (`b`(2)),
 KEY `prefix_complex` (`a`, `b`(2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
INSERT INTO `prefix` VALUES(0, "b", 2), (1, "bbb", 3), (2, "bbc", 4), (3, "bbb", 5), (4, "abc", 6), (5, "abc", 7), (6, "abc", 7), (7, "ÿÿ", 8), (8, "ÿÿ0", 9), (9, "ÿÿÿ", 10);
ANALYZE TABLE `prefix`;