drop table if exists `data_type_test_pk`.`test_unsigned_decimal`;
CREATE TABLE `data_type_test_pk`.`test_unsigned_decimal` (
  `col_decimal` decimal(11) unsigned not null,
  PRIMARY KEY (`col_decimal`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_unsigned_decimal` values ('20774931456'),('61894055104'),('6986074624'),('44234052608'),('27567568896'),('24217512960'),('97360142336'),('47734704128'),('94908741824'),('91361630208'),('94641968128'),('18682023936'),('83060802560'),('50799943168'),('67614540288'),('320158592'),('52384979968'),('36855255296'),('134216704'),('92529944576');