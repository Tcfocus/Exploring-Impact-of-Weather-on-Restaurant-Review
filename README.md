# Exploring-Impact-of-Weather-on-Restaurant-Review

The purpose of this project is to utilize massive data sets of climate data and Yelp reviews, in order to analyze the impact of weather on restaurant reviews.
Snowflake - a cloud-native data warehouse - is used to stage the data, where it is then migrated and transformed into a ODS environment.
THe data is then migrates to a Data Warehouse schema, where SQL queries are performed to understand the relationship between weather and reviews.

##Data Sources:
  - Yelp dataset of all existing reviews in JSON format(link: https://www.yelp.com/dataset/download). consisting of seperate datasets:
    1. Individual Business information
    2. Checkin
    3. Covid features
    4. Reviews
    5. Tips
    6. Users
- Historical climate data for San Diego in CSV format (link: https://crt-climate-explorer.nemac.org/):
    1. Precipitation
    2. Temperature

##Process:
  1. Load data into Snowflake stage from locally downloaded files.
  2. Migrate staged files into staging schema.
  3. Transform data from staging into ODS schema.
    a. Follow ER Diagram data structure.
  4. Migrate data from ODS schema into final Data Warehouse schema,
  5. Perform queries to understand any hidden relationships between weather and a customers review on a restaurant.
   
