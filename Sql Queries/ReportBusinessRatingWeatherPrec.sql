-- SQL queries code that reports the business name, temperature, precipitation, and ratings - shows how climate data is impacting restaurant reviews

Select f.business_name, f.stars, w.min_temp, w.max_temp, p.precipitation
FROM fact_table AS f
JOIN DimPrecipitation as p
ON f.date = p.date
JOIN DimWeather as w
ON f.date = w.date;

