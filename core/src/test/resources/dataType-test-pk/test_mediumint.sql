drop table if exists `data_type_test_pk`.`test_mediumint`;
CREATE TABLE `data_type_test_pk`.`test_mediumint` (
  `col_mediumint` mediumint(9) not null,
  PRIMARY KEY (`col_mediumint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_mediumint` values (-2166579),(-384962),(-7743336),(-6954666),(344916),(4252377),(6664485),(2642942),(4308840),(-7242265),(-1852694),(-5346633),(-5638720),(-7362547),(3294878),(1658649),(1527612),(2329943),(6154713),(-5757719);