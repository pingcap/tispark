drop table if exists `data_type_test_pk`.`test_unsigned_float`;
CREATE TABLE `data_type_test_pk`.`test_unsigned_float` (
  `col_float` float unsigned not null,
  PRIMARY KEY (`col_float`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_unsigned_float` values (0.8644155),(0.3882994),(0.23998016),(0.7607705),(0.05630529),(0.19282472),(0.3635515),(0.6836497),(0.7228101),(0.43802553),(0.9517695),(0.85299206),(0.7537251),(0.40279347),(0.17543322),(0.18561184),(0.30614185),(0.99632895),(0.5844924),(0.19935483);