-- Expected landing rows: 956. Numeric dates are Unix epoch milliseconds.
-- birthDay remains a string; absent consent fields deserialize as NULL.
-- Case-insensitive OpenX matching handles the source's camelCase JSON keys.
CREATE EXTERNAL TABLE IF NOT EXISTS stedi.customer_landing (
    birthDay STRING,
    customerName STRING,
    email STRING,
    lastUpdateDate BIGINT,
    phone STRING,
    registrationDate BIGINT,
    serialNumber STRING,
    shareWithFriendsAsOfDate BIGINT,
    shareWithPublicAsOfDate BIGINT,
    shareWithResearchAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('case.insensitive' = 'true')
STORED AS TEXTFILE
LOCATION 's3://wgu-d609-tklein2/landing/customer/';
