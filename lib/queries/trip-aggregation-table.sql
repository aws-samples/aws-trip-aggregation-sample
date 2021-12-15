CREATE EXTERNAL TABLE `trip_aggregation`(
  `eventid` string, 
  `tripid` string, 
  `deviceid` string, 
  `eventtime` int, 
  `eventtype` string)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string, 
  `hour` string,
  `minute` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://dataingestionstack-ingestiondestinationbucketfb85-13ugdpl0tfyon/raw/'
TBLPROPERTIES (
  'has_encrypted_data'='false', 
  'transient_lastDdlTime'='1637618754')