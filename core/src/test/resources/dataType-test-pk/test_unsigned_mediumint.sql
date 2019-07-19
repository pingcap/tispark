drop table if exists `data_type_test_pk`.`test_unsigned_mediumint`;
CREATE TABLE `data_type_test_pk`.`test_unsigned_mediumint` (
  `col_mediumint` mediumint(9) unsigned not null,
  PRIMARY KEY (`col_mediumint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_unsigned_mediumint` values (5824691),(12493343),(5300085),(1732669),(11408921),(9888396),(7930384),(2309526),(108221),(746033),(14812389),(6859052),(13394559),(14201195),(12185775),(10080959),(7932315),(8359951),(15329654),(10668299);