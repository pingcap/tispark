drop table if exists `data_type_test_pk`.`test_char`;
CREATE TABLE `data_type_test_pk`.`test_char` (
  `col_char` char(10) not null,
  PRIMARY KEY (`col_char`(2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_char` values ('HL1BKImaaz'),('tpWXsA1cre'),('pa4d3rSoYF'),('SIdSXwbA0w'),('1gSdx34cmc'),('c2ihqPJioW'),('5F3NC29ch2'),('qrFGRtLzWo'),('lufwTsS5Ka'),('gKK0vzzG15'),('IIiD1GYlRa'),('50aR49Jsic'),('U7go5AemVO'),('KnUY2tIz6T'),('Abl9HLYyHP'),('aSyIjxxttR'),('vdsRiZBJbB'),('6Uk6JRdREz'),('XpO7sVPeh7'),('ibsNh1V4Dv');