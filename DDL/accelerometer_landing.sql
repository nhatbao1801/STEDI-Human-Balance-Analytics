CREATE EXTERNAL TABLE IF NOT EXISTS `baotcn_db`.`accelerometer_landing` (
  `timeStamp` bigint,
  `user` string,
  `x` float,
  `y` float,
  `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://baotcn/accelerometer/'
TBLPROPERTIES ('classification' = 'json');