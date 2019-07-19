drop table if exists `data_type_test_pk`.`test_float`;
CREATE TABLE `data_type_test_pk`.`test_float` (
  `col_float` float not null,
  PRIMARY KEY (`col_float`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_float` values (0.3344555),(0.7531235),(0.34891874),(0.4891544),(0.9036852),(0.07313055),(0.7214622),(0.46226412),(0.060016453),(0.57527876),(0.79577875),(0.22960114),(0.7992055),(0.7133278),(0.39025623),(0.05389613),(0.6304783),(0.751547),(0.11777145),(0.6318339);