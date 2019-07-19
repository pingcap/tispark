drop table if exists `data_type_test_pk`.`test_int`;
CREATE TABLE `data_type_test_pk`.`test_int` (
  `col_int` int(11) not null,
  PRIMARY KEY (`col_int`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_int` values (1590266524),(1738307886),(1099690259),(-1114460689),(947176512),(1720160860),(-290241090),(351915256),(362280390),(741503441),(-716866363),(-1166508339),(308064178),(-715801777),(1410429169),(-2084635901),(-616561149),(-297829160),(1403116115),(-1704682898);