drop table if exists `data_type_test_pk`.`test_tinytext`;
CREATE TABLE `data_type_test_pk`.`test_tinytext` (
  `col_tinytext` tinytext not null,
  PRIMARY KEY (`col_tinytext`(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_tinytext` values ('vktlI30uHEEqne8Xa'),('Qqi11a0QXH51MxkuBlDlmEN'),('0jjIz2eM1a8FbxMQ5luliWHhI'),('JHUjvuB1khcjjgkGDcMDivYjuB4R'),('nfEI6hvlBTOfaYRoPqU'),('j6Lra1TON2oPXR2zQHibqC5X6l47h6XliLMbOfXD4ZfD'),('cBoVLphOzy0sf9wE5tY29'),('WWB4hLiERrs'),('Lgkdh5rCEZWQ9hL1H7k6Yrx0yD2FDU6NtqasR6dG'),('AKkI8FbBX2uS6qxP5Gy5R3'),('ErgNcgn2L3LHTQDMpLd'),('8qbjY8a5desDKrDAx2RyJ9Rec'),('Y07KveUF2WT3Ewx38OsnZjsv7Q6zsZZ0p8h5IEZgqMdbnM'),('PPrLX8aTcd2UEIbOigYpfRmm31t'),('ruTP9xs3Kaj9gDJ1do6L7IjEo7wXEYsoNNB'),('fBC25Qz2RJrmd90fcRTK'),('xOw2yfksHaPyUXi'),('wsWMZoZHizNZgj10'),('5I0HpcBFqr28Ptry1EXbXkrtjLcC1YSf'),('J4uVj79bq1KyZwZF4UX');