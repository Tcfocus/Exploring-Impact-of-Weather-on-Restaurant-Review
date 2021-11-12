-- Copy the JSON from stage into staging tables

copy into yelp_business from @my_json_stage/yelp_academic_dataset_business.json.gz file_format = myjsonformat on_error='skip_file';
copy into yelp_checkin from @my_json_stage/yelp_academic_dataset_checkin.json.gz file_format = myjsonformat on_error='skip_file';
copy into yelp_covid_features from @my_json_stage/yelp_academic_dataset_covid_features.json.gz file_format = myjsonformat on_error='skip_file';
copy into yelp_tip from @my_json_stage/yelp_academic_dataset_tip.json.gz file_format = myjsonformat on_error='skip_file';
copy into yelp_user from @my_json_stage/yelp_academic_dataset_user.json.gz file_format = myjsonformat on_error='skip_file';
copy into yelp_review from @my_json_stage/yelp_academic_dataset_review.json.gz file_format = myjsonformat on_error='skip_file';

-- SQL Queries to move from single JSON structure in staging to multiple columns in ODS

CREATE SCHEMA ODS;

-- CREATE THE TABLES
-- Business Table

CREATE OR REPLACE TABLE Business (
address varchar(400), 
attributes object, 
business_id varchar(100) PRIMARY KEY, 
categories varchar(500), 
city varchar(100), 
hours object, 
is_open varchar(50),
latitude FLOAT,
longitude FLOAT,
name varchar(200),
postal_code varchar(50),
review_count int,
stars float,
state varchar(50)
);

INSERT INTO Business
SELECT userjson: address,
userjson: attributes,
userjson: business_id,
userjson: categories,
userjson: city,
userjson: hours,
userjson: is_open,
userjson: latitude,
userjson: longitude,
userjson: name,
userjson: postal_code,
userjson: review_count,
userjson: stars,
userjson: state
FROM WEATHERIMPACTONREVIEWS.STAGING.yelp_business;

-- User Table

CREATE OR REPLACE TABLE User (
average_stars FLOAT,
compliment_cool int,
compliment_cute int,
compliment_funny int,
compliment_hot int,
compliment_list int,
compliment_more int,
compliment_note int,
compliment_photos int,
compliment_plain int,
compliment_profile int, 
compliment_writer int,
cool int,
elite string,
fans int,
friends string,
funny int,
name string,
review_count int,
useful int,
user_id varchar(100) PRIMARY KEY,
yelping_since string);

INSERT INTO User
SELECT userjson: average_stars,
userjson: compliment_cool,
userjson: compliment_cute,
userjson: compliment_funny,
userjson: compliment_hot,
userjson: compliment_list,
userjson: compliment_more,
userjson: compliment_note,
userjson: compliment_photos,
userjson: compliment_plain,
userjson: compliment_profile,
userjson: compliment_writer,
userjson: cool,
userjson: elite,
userjson: fans,
userjson: friends,
userjson: funny,
userjson: name,
userjson: review_count,
userjson: useful,
userjson: user_id,
userjson: yelping_since
FROM WEATHERIMPACTONREVIEWS.STAGING.yelp_user;



-- Tip Table ***

CREATE OR REPLACE SEQUENCE tipsequence;

CREATE OR REPLACE TABLE Tip (Tip_id NUMBER DEFAULT tipsequence.nextval PRIMARY KEY,
business_id varchar(100) REFERENCES Business(business_id),
compliment_count int,
date TIMESTAMP,
text STRING,
user_ID varchar(100) REFERENCES User(user_id))

INSERT INTO Tip (business_id, compliment_count, date, text, user_id)
SELECT userjson: business_id,
userjson: compliment_count,
to_timestamp(userjson: date),
userjson: text,
userjson: user_id
FROM WEATHERIMPACTONREVIEWS.STAGING.yelp_tip;


--Check_in Table

CREATE OR REPLACE SEQUENCE checkinsequence;

CREATE OR REPLACE TABLE Check_in (checkin_id NUMBER DEFAULT checkinsequence.nextval PRIMARY KEY,
business_id varchar(100) REFERENCES Business(business_id),
date STRING);

INSERT INTO Check_in (business_id, date)
SELECT userjson: business_id,
userjson: date
FROM WEATHERIMPACTONREVIEWS.STAGING.YELP_CHECKIN;

-- Review Table

CREATE OR REPLACE TABLE Review (business_id varchar(100) REFERENCES Business(business_id),
cool int,
date TIMESTAMP,
funny int,
review_id VARCHAR(100) PRIMARY KEY,
stars int,
text string,
useful int,
user_id varchar(100) REFERENCES User(user_id));

INSERT INTO Review
SELECT userjson: business_id,
userjson: cool,
to_timestamp(userjson: date),
userjson: funny,
userjson: review_id,
userjson: stars,
userjson: text,
userjson: useful,
userjson: user_id
FROM WEATHERIMPACTONREVIEWS.STAGING.YELP_REVIEW;

CREATE OR REPLACE TABLE Covid_features (call_to_action_enabled string,
covid_banner string,
grubhub_enabled string,
request_a_quote_enabled string,
temporary_closed_until string,
virtual_services_offered string,
business_id VARCHAR(100) PRIMARY KEY REFERENCES Business(business_id),
delivery_or_takeout string,
highlights string);

INSERT INTO Covid_features
SELECT userjson: "Call To Action enabled",
userjson: "Covid Banner",
userjson: "Grubhub enabled",
userjson: "Request a Quote Enabled",
userjson: "Temporary Closed Until",
userjson: "Virtual Services Offered",
userjson: business_id,
userjson: "delivery or takeout",
userjson: highlights
FROM WEATHERIMPACTONREVIEWS.STAGING.yelp_Covid_features;