-- Migrate Data to Data Warehouse tables

CREATE SCHEMA dwh;

CREATE OR REPLACE TABLE DimClimate (date DATE PRIMARY KEY,
min_temp INT,
max_temp INT,
normal_min FLOAT,
normal_max FLOAT,
precipitation FLOAT,
precipitation_normal FLOAT);



INSERT INTO DimClimate
SELECT w.date, w.min_temp, w.max_temp, w.normal_min, w.normal_max, p.precipitation, p.precipitation_normal
FROM WEATHERIMPACTONREVIEWS.ODS.WEATHER as w
JOIN WEATHERIMPACTONREVIEWS.ODS.PRECIPITATION as p
on w.date = p.date;



CREATE OR REPLACE TABLE DimReview (business_id varchar(100),
cool int,
date TIMESTAMP,
funny int,
review_id VARCHAR(100) PRIMARY KEY,
stars int,
text string,
useful int,
user_id varchar(100));

INSERT INTO DimReview
SELECT business_id,
cool,
date,
funny,
review_id,
stars,
text,
useful,
user_id
FROM WEATHERIMPACTONREVIEWS.ODS.Review;





CREATE OR REPLACE TABLE DimBusiness (
address varchar(400), 
attributes object, 
business_id varchar(100) PRIMARY KEY, 
categories varchar(500), 
hours object, 
is_open varchar(50),
name varchar(200),
review_count int,
stars float,
state varchar(50)
);

INSERT INTO DimBusiness (address, attributes, business_id, categories, hours, is_open, name, review_count, stars, state)
SELECT address,
attributes,
business_id,
categories,
hours,
is_open,
name,
review_count,
stars,
state
FROM WEATHERIMPACTONREVIEWS.ODS.Business;

CREATE OR REPLACE TABLE DimLocation (
business_id varchar(100) PRIMARY KEY,
city varchar(100), 
latitude FLOAT,
longitude FLOAT,
postal_code varchar(50)
);

INSERT INTO DimLocation
SELECT business_id,
city,
latitude,
longitude,
postal_code
FROM WEATHERIMPACTONREVIEWS.ODS.Business;

-- Create and insert into fact_table

CREATE OR REPLACE TABLE fact_table (
    date DATE,
    business_id varchar(100),
    review_id varchar(100),
    business_name string,
    stars FLOAT
);

INSERT INTO fact_table(date, business_id, review_id, business_name, stars)
SELECT to_date(r.date), b.business_id, r.review_id, b.name, r.stars
FROM DimReview AS r
join DimBusiness AS b
ON r.business_id = b.business_id;