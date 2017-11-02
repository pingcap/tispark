DROP TABLE IF EXISTS `test_index`;
CREATE TABLE `test_index` (
  `a` int(11) NOT NULL,
  `b` smallint(11) DEFAULT NULL,
  `c` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `d` float DEFAULT NULL,
  `e` float DEFAULT NULL,
  PRIMARY KEY (`a`),
  KEY `idx_c` (`c`),
  KEY `idx_de` (`d`,`e`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin