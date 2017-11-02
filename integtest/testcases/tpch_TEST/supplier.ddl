DROP TABLE IF EXISTS `supplier`;
CREATE TABLE `supplier` (
  `S_SUPPKEY` int(11) NOT NULL,
  `S_NAME` char(25) NOT NULL,
  `S_ADDRESS` varchar(40) NOT NULL,
  `S_NATIONKEY` int(11) NOT NULL,
  `S_PHONE` char(15) NOT NULL,
  `S_ACCTBAL` float NOT NULL,
  `S_COMMENT` varchar(101) NOT NULL,
  PRIMARY KEY (`S_SUPPKEY`),
  CONSTRAINT `SUPPLIER_FK1` FOREIGN KEY (`S_NATIONKEY`) REFERENCES `nation` (`S_NATIONKEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin