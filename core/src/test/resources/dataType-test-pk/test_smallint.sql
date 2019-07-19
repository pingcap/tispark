drop table if exists `data_type_test_pk`.`test_smallint`;
CREATE TABLE `data_type_test_pk`.`test_smallint` (
  `col_smallint` smallint(6) not null,
  PRIMARY KEY (`col_smallint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_smallint` values (-6126),(-1293),(-28196),(21801),(-29520),(-30739),(-17008),(-31505),(16966),(-12976),(-15554),(18590),(-13554),(25035),(-7024),(-2546),(13572),(15450),(-23162),(-20117);