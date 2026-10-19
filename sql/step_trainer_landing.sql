-- Expected landing rows: 28,680. sensorReadingTime is Unix epoch milliseconds.
-- Case-insensitive OpenX matching handles the source's camelCase JSON keys.
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('case.insensitive' = 'true')
STORED AS TEXTFILE
LOCATION 's3://wgu-d609-tklein2/landing/step_trainer/';
