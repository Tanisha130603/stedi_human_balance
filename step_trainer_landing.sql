CREATE EXTERNAL TABLE `step_trainer_landing`(
)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://tanisha13-lake-house/step-trainer-landing/'
TBLPROPERTIES (
  'classification'='json')
