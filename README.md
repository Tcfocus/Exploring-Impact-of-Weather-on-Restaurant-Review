# Exploring-Impact-of-Weather-on-Restaurant-Review

The purpose of this project is to analyze the impact of weather on restaurant reviews viau large data sets for climate data and Yelp reviews.
Snowflake - a cloud-native data warehouse - is used to stage the data, where it is then migrated and transformed into a ODS environment.
The data is then migrated to a Data Warehouse schema, where SQL queries are performed to understand the relationship between weather and reviews.

## Data Sources:
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

## Process:
  1. Load data into Snowflake stage from locally downloaded files.
  2. Migrate staged files into staging schema.
  3. Transform data from staging into ODS schema.
    a. Follow ER Diagram data structure.
  4. Migrate data from ODS schema into final Data Warehouse schema,
  5. Perform queries to understand any hidden relationships between weather and a customers review on a restaurant.

![Screenshot](https://github.com/Tcfocus/Exploring-Impact-of-Weather-on-Restaurant-Review/blob/master/Images/ER%20Diagram%20for%20ODS.png)


   
