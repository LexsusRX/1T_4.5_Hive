set hive.enforce.bucketing = true;

CREATE TEMPORARY TABLE IF NOT EXISTS customers_temp (
    index_table INT,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    company STRING,
    city STRING,
    country STRING,
    phone_1 STRING,
    phone_2 STRING,
    email STRING,
    subscription_date DATE,
    website STRING,
    group_table INT,
    subscription_year INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/hueadmin/data/customers.csv' OVERWRITE INTO TABLE customers_temp;

CREATE TABLE IF NOT EXISTS customers (
    index_table INT,
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    company STRING,
    city STRING,
    country STRING,
    phone_1 STRING,
    phone_2 STRING,
    email STRING,
    subscription_date DATE,
    website STRING,
    group_table INT
)
PARTITIONED BY (subscription_year INT)
CLUSTERED BY (first_name, last_name, email) INTO 10 BUCKETS
STORED AS PARQUET;

INSERT INTO TABLE customers PARTITION(subscription_year = 2020)
SELECT index_table, customer_id, first_name, last_name, company, city, country,
	phone_1, phone_2, email, subscription_date, website, group_table
FROM customers_temp WHERE subscription_year = 2020;

INSERT INTO TABLE customers PARTITION(subscription_year = 2021)
SELECT index_table, customer_id, first_name, last_name, company, city, country,
	phone_1, phone_2, email, subscription_date, website, group_table
FROM customers_temp WHERE subscription_year = 2021;

INSERT INTO TABLE customers PARTITION(subscription_year = 2022)
SELECT index_table, customer_id, first_name, last_name, company, city, country,
	phone_1, phone_2, email, subscription_date, website, group_table
FROM customers_temp WHERE subscription_year = 2022;



CREATE TEMPORARY TABLE IF NOT EXISTS organizations_temp (
	index_table INT,
	organization_id STRING,
	name STRING,
	website STRING,
	country STRING,
	description STRING,
	founded INT,
	industry STRING,
	number_of_employees INT,
	group_table INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/hueadmin/data/organizations.csv' OVERWRITE INTO TABLE organizations_temp;

CREATE TABLE IF NOT EXISTS organizations (
 	index_table INT,
	organization_id STRING,
	name STRING,
	website STRING,
	country STRING,
	description STRING,
	founded INT,
	industry STRING,
	number_of_employees INT,
	group_table INT
)
CLUSTERED BY (name) INTO 10 BUCKETS
STORED AS PARQUET;

INSERT INTO TABLE organizations
SELECT * 
FROM organizations_temp;


CREATE TEMPORARY TABLE IF NOT EXISTS people_temp (
	index_table INT,
	user_id STRING,
	first_name STRING,
	last_name STRING,
	sex STRING,
	email STRING,
	phone STRING,
	date_of_birth DATE,
	job_title STRING,
	group_table INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/hueadmin/data/people.csv' OVERWRITE INTO TABLE people_temp;

CREATE TABLE IF NOT EXISTS people (
	index_table INT,
	user_id STRING,
	first_name STRING,
	last_name STRING,
	sex STRING,
	email STRING,
	phone STRING,
	date_of_birth DATE,
	job_title STRING,
	group_table INT
)
CLUSTERED BY(first_name, last_name, email) INTO 10 BUCKETS
STORED AS PARQUET;

INSERT INTO TABLE people
SELECT * 
FROM people_temp;



CREATE TABLE IF NOT EXISTS result_mart AS
WITH custumer_union AS (
	SELECT * FROM customers WHERE subscription_year = 2020
	UNION
	SELECT * FROM customers WHERE subscription_year = 2021
	UNION
	SELECT * FROM customers WHERE subscription_year = 2022
),
customers_range_age AS (
	SELECT cu.company, cu.subscription_year, COUNT(*) AS amount,
		CASE
			WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 18 THEN '0 - 18'
			WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 25 THEN '19 - 25'
			WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 35 THEN '26 - 35'
			WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 45 THEN '36 - 45'
			WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 55 THEN '46 - 55'
			WHEN YEAR(CURRENT_DATE) - YEAR(p.date_of_birth) <= 65 THEN '56 - 65'
			ELSE 'more than 66'
		END AS age_group_table
	FROM custumer_union cu
	JOIN people p ON cu.first_name = p.first_name AND cu.last_name = p.last_name AND cu.email = p.email
	JOIN organizations o ON cu.company = o.name
	GROUP BY cu.company, cu.subscription_year, p.date_of_birth
)

SELECT company, subscription_year, age_group_table, MAX(amount)
FROM customers_range_age
GROUP BY company, subscription_year, age_group_table;

DROP TABLE organizations_temp;
DROP TABLE people_temp;
DROP TABLE customers_temp;
