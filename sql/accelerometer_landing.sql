-- Expected landing rows: 81,273. timestamp is Unix epoch milliseconds.
-- OpenX reads one JSON object per line with case-insensitive key matching.
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
    `timestamp` BIGINT,
    `user` STRING,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('case.insensitive' = 'true')
STORED AS TEXTFILE
LOCATION 's3://wgu-d609-tklein2/landing/accelerometer/';
