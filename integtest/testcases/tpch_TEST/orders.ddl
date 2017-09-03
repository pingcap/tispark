CREATE TABLE `orders` (
  `O_ORDERKEY` int(11) NOT NULL,
  `O_CUSTKEY` int(11) NOT NULL,
  `O_ORDERSTATUS` char(1) NOT NULL,
  `O_TOTALPRICE` float NOT NULL,
  `O_ORDERDATE` date NOT NULL,
  `O_ORDERPRIORITY` char(15) NOT NULL,
  `O_CLERK` char(15) NOT NULL,
  `O_SHIPPRIORITY` int(11) NOT NULL,
  `O_COMMENT` varchar(79) NOT NULL,
  PRIMARY KEY (`O_ORDERKEY`),
  CONSTRAINT `ORDERS_FK1` FOREIGN KEY (`O_CUSTKEY`) REFERENCES `customer` (`O_CUSTKEY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin