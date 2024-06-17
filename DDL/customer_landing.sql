CREATE EXTERNAL TABLE IF NOT EXISTS `baotcn_db`.`customer_landing` (
  `serialnumber` string,
  `sharewithpublicasofdate` bigint,
  `birthday` string,
  `registrationdate` bigint,
  `sharewithresearchasofdate` bigint,
  `customername` string,
  `email` bigint,
  `lastupdatedate` bigint,
  `phone` bigint,
  `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://baotcn/customers/'
TBLPROPERTIES ('classification' = 'json');