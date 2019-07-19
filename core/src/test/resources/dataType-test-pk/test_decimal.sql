drop table if exists `data_type_test_pk`.`test_decimal`;
CREATE TABLE `data_type_test_pk`.`test_decimal` (
  `col_decimal` decimal(11) not null,
  PRIMARY KEY (`col_decimal`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_decimal` values ('-39642824704'),('-45326541824'),('93903144960'),('-30343561216'),('-19583390336'),('-60415281664'),('-22580393984'),('89977111040'),('99383012352'),('98529547264'),('19409883136'),('19845483520'),('-71010272256'),('56237944832'),('-8993585152'),('33844711424'),('98410658304'),('-34677656576'),('-21330299392'),('74818975488');