drop table if exists `data_type_test_pk`.`test_tinyint`;
CREATE TABLE `data_type_test_pk`.`test_tinyint` (
  `col_tinyint` tinyint(4) not null,
  PRIMARY KEY (`col_tinyint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_tinyint` values (37),(-62),(115),(-68),(91),(-47),(-11),(12),(-42),(-105),(-76),(-125),(-40),(-12),(30),(-102),(114),(-27),(-124),(-67);