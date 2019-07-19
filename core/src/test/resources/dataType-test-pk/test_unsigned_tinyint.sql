drop table if exists `data_type_test_pk`.`test_unsigned_tinyint`;
CREATE TABLE `data_type_test_pk`.`test_unsigned_tinyint` (
  `col_tinyint` tinyint(4) unsigned not null,
  PRIMARY KEY (`col_tinyint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_unsigned_tinyint` values (248),(81),(61),(193),(133),(145),(181),(202),(3),(130),(31),(203),(156),(49),(84),(216),(66),(118),(152),(155);