drop table if exists `data_type_test_pk`.`test_varbinary`;
CREATE TABLE `data_type_test_pk`.`test_varbinary` (
  `col_varbinary` varbinary(41) not null,
  PRIMARY KEY (`col_varbinary`(4))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into `data_type_test_pk`.`test_varbinary` values (X'b2de2e704b6aa9c1f367f3797f295dd9a1543cff6017cc0b7b4340337aa16facceae115218720e23f4'),(X'edee57439cb9e1c42d06caf2219855e10915013c44ffcdcfe97d6a94f6ac223be0612ecf1de5304521'),(X'7cf43675832ff2f11dd915996119d3b6f7a9cfab4c22a71b9f53c86768b4e67649366e6077780eac66'),(X'd665aadf98c3fb41f73e4b6ce12912ee236fbf322ee71f47877f9bf8b065f56ad39c30a2a1853dc6fe'),(X'66790fb8e7235362335517d8e74a82879048f1d39662d3477ba24fd65069adfcf69497483c7800cd44'),(X'e215c8f4203ebcdd93e82403b4a3496f31092bda1b757ddd20bee9b6b0f9eafa555ff0e87de85db1ef'),(X'7f2a0ef7b9c6edfda52c4dcc818e4afaad5e0c6c324822c547e497db053cc55331c010c053fcaf3b27'),(X'c633f64ad1fec1e4ff412b322415d177e5c4d451c536d5843e4a8e86771be12447e448529a1d966e54'),(X'3c026e6bd079b9575999cae20ffd52aa32f4fc13d0cc1511e82516fd6108654abe4228ccbdc3a482af'),(X'2dc457533540f5eb23eb5b5c7eb2227165cd6a245afa368cd7772b9fadff8e9454b877232599031172'),(X'84260876382124e6c6655a4ffda96bc5dd8bf01f13b2cb27e9bd74bae9a3679008d6e1d2c231a4823a'),(X'414f2ae4eee57ee24ef02a002a7706d8225dc99c78be09d981240cf6f382d8f46ab0121431fe65b6bc'),(X'da8b6d7aaeeed56729cf962e08f572c8b3fb5b8472ac81c71c7151c36a4c92d2aac5f55593a8eab3d4'),(X'7aa37a993f3f32f208cb864ecb6780b736228b4792056e22c9daccf3138398db13b3ecfee072ed220e'),(X'7a08456e5529f0be4d68d20fbc5fe679e92a10ab9ff8f731e36ffe201cf91d9c0c77d14fcd518d6eda'),(X'cdf698ad3785f5ee01b9e91f19c0edaafb694fdf87657490fb683b5dfc7e3155d16eb244482a8026ae'),(X'58aa07ec2c95f0309634a7570ff016abbbbf0e1290a04b764627bc0d2fd5131df74ecd745fd3f5d606'),(X'8c424b70e1f5c35a47399cbbc236a6ada35d879556d4bc20fca4265c4913d79fc90cacb27ebccfd427'),(X'308966a13a27490854a342f79d742fd9f651f9d8721297ff6515e245d4e914e5681decffbb600fedd7'),(X'9229390446006d470745d8acadfd30dfe600a4ef27a96a3fffef525a48e900e25b8af003997f47045b');