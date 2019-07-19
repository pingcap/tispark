drop table if exists `data_type_test_pk`.`test_text`;
CREATE TABLE `data_type_test_pk`.`test_text` (
  `col_text` text not null,
  PRIMARY KEY (`col_text`(3))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_text` values ('MvrczYuW3lUmchoiRsCGQPZlnrgio2tZ4qFYt3a9E'),('54AREgcJ0ItKS'),('xYDDqQlzWtu7ww4kHlLbbwN0jOc3QnCG06'),('csyHIucx8CfS2YoHrLg0hLnNQwlRsKhXEXxWhW'),('ObRAPv5py0joCZHRb1TipgRDnu5nr5'),('nHC9XFjYQ1h1J'),('1VLL11KExt1VHTkkI'),('ne06S7rKvw39kA3QLvFjX9akhExnnnOSVeevJuu'),('GAHFLHqelIv00y6Zu1'),('7a4U9zZYJwvZyE2l1ir98F7xqklB'),('OkcdlBNPL8fYVDj9t1hD7eMoP12'),('z63opyTXj3CsqQqTKjIjVc'),('3eVwJeHfmheHQuV5VYMi2dltYV6u2BYjrnHc8Brt'),('WPgVh2kJNj'),('t0lVfqCA6EETls4iWSiN6tpmShNC6C5Df1RnAc49'),('yNTK19FeHPdMgAqB4BneNt9sQ'),('BZUqNAfI38EwcEFr5aLXFXQ3cJDIiwUUMsUi'),('BJD1nyc1bsiYw1OV1sn4rGvJ6Vd7AEdsZkXOzOBZfMav'),('vclPeP2CEVcSzms152qzgFnCRyAn0zY'),('vncKIQW6eOBGlXDk5DqtPc');