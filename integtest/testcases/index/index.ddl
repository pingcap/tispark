DROP TABLE IF EXISTS `test_selectivity`;
CREATE TABLE `test_selectivity` (
  `a` int(11) NOT NULL,
  `b` int(11) DEFAULT NULL,
  `c` int(11) DEFAULT NULL,
  `d` int(11) DEFAULT NULL,
  `e` int(11) DEFAULT NULL,
  PRIMARY KEY (`a`),
  KEY `idx_cd` (`c`,`d`),
  KEY `idx_de` (`d`,`e`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin