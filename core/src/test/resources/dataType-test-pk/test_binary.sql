drop table if exists `data_type_test_pk`.`test_binary`;
CREATE TABLE `data_type_test_pk`.`test_binary` (
  `col_binary` binary(10) not null,
  PRIMARY KEY (`col_binary`(3))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_binary` values (X'6bc231808c117dd673c0'),(X'147f9b18326f27f6ef72'),(X'16932cc50f44ca83634f'),(X'9b73ee549a61f0942c85'),(X'7fc4793570cf28c034a1'),(X'2c9dc879436ce0febdbc'),(X'89f50e97866c85f0615d'),(X'e57c6140841e5178ae14'),(X'73fb0c733ed39f676a6f'),(X'99a3b7202c965bf13c86'),(X'2a23dadf8173a6cad047'),(X'ce4e7c1cf921a79fe542'),(X'e5c61f7407da87aa1e0c'),(X'd3489ad22a1f0f8101c1'),(X'0a9e3d6b69c9286ec885'),(X'fd9af4b5abc1db6098ee'),(X'c7bc0917cf8d920501e1'),(X'2be99f37c3b9ff107800'),(X'f04d16cb079e070ede83'),(X'1454f77658d3326cb463');