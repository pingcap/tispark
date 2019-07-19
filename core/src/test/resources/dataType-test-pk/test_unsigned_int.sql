drop table if exists `data_type_test_pk`.`test_unsigned_int`;
CREATE TABLE `data_type_test_pk`.`test_unsigned_int` (
  `col_int` int(11) unsigned not null,
  PRIMARY KEY (`col_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_unsigned_int` values (2636010146),(1683114929),(2050026429),(896751662),(3069156605),(3653528383),(2901701821),(3592963957),(4063971215),(714477378),(260438037),(272536940),(1103636124),(63612787),(3993673601),(2726019778),(2403286380),(1815496369),(3030064337),(2031081616);