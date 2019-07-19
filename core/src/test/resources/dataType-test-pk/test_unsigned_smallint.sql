drop table if exists `data_type_test_pk`.`test_unsigned_smallint`;
CREATE TABLE `data_type_test_pk`.`test_unsigned_smallint` (
  `col_smallint` smallint(6) unsigned not null,
  PRIMARY KEY (`col_smallint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_unsigned_smallint` values (43831),(38084),(25460),(8566),(29146),(62904),(50370),(26291),(44552),(20916),(62687),(38507),(53144),(51575),(4754),(36547),(17294),(53746),(41986),(65322);